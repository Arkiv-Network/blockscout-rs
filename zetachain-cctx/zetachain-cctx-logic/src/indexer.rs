use std::sync::Arc;
use std::time::Duration;

use anyhow::Ok;

use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::models::{CrossChainTx, InboundParams, OutboundParams, PagedCCTXResponse, RevertOptions};
use zetachain_cctx_entity::prelude::CrossChainTx as CrossChainTxModel;
use zetachain_cctx_entity::sea_orm_active_enums::{CctxStatusStatus, TxFinalizationStatus, WatermarkType};
use zetachain_cctx_entity::{
    cctx_status, cross_chain_tx, inbound_params, outbound_params, revert_options, watermark,
};

use sea_orm::{ColumnTrait, ConnectionTrait, DbBackend, QueryOrder, RelationTrait, Statement};

use crate::{client::Client, settings::IndexerSettings};
use chrono::{DateTime, Utc};
use sea_orm::ActiveModelTrait;
use futures::StreamExt;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use tracing::{instrument, Instrument};

use sea_orm::TransactionTrait;

use futures::stream::{select_with_strategy, PollNext};

/// Sanitizes strings by removing null bytes and replacing invalid UTF-8 sequences
/// PostgreSQL UTF-8 encoding doesn't allow null bytes (0x00)
fn sanitize_string(input: String) -> String {
    // Remove null bytes and replace invalid UTF-8 sequences
    input
        .chars()
        .filter(|&c| c != '\0')  // Remove null bytes
        .collect::<String>()
        .replace('\u{FFFD}', "?")  // Replace replacement character with question mark
}

pub struct Indexer {
    pub settings: IndexerSettings,
    pub db: Arc<DatabaseConnection>,
    pub client: Arc<Client>,
}

enum IndexerJob {
    StatusUpdate(cross_chain_tx::Model, Uuid),             //cctx index and id to be updated
    GapFill(watermark::Model, Uuid), // Watermark (pointer) to the next page of cctxs to be fetched
    HistoricalDataFetch(watermark::Model, Uuid), // Watermark (pointer) to the next page of cctxs to be fetched
}

#[instrument(skip(db, client), fields(job_id = %job_id))]
async fn update_cctx_status(
    job_id: Uuid,
    db: &DatabaseConnection,
    client: &Client,
    existing_cctx: cross_chain_tx::Model,
) -> anyhow::Result<()> {
    //get the most recent cctx data from the client and update corresponding child record of cctx (outbound params)
    let fetched_cctx = client.get_cctx(&existing_cctx.index).await.unwrap();

    //begin transaction
    let tx = db.begin().await?;
    for outbound_params in fetched_cctx.outbound_params {
        if let Some(record) = outbound_params::Entity::find()
            .filter(outbound_params::Column::CrossChainTxId.eq(Some(existing_cctx.id)))
            .filter(outbound_params::Column::Receiver.eq(outbound_params.receiver))
            .one(db)
            .await
            .unwrap()
        {
            outbound_params::Entity::update(outbound_params::ActiveModel {
                id: ActiveValue::Set(record.id),
                tx_finalization_status: ActiveValue::Set(
                    TxFinalizationStatus::try_from(outbound_params.tx_finalization_status)
                        .map_err(|e| anyhow::anyhow!(e))?,
                ),
                gas_used: ActiveValue::Set(outbound_params.gas_used),
                effective_gas_price: ActiveValue::Set(outbound_params.effective_gas_price),
                effective_gas_limit: ActiveValue::Set(outbound_params.effective_gas_limit),
                tss_nonce: ActiveValue::Set(outbound_params.tss_nonce),
                ..Default::default()
            })
            .filter(outbound_params::Column::Id.eq(record.id))
            .exec(db)
            .await?;
        }
    }
    //unlock cctx
    cross_chain_tx::Entity::update(cross_chain_tx::ActiveModel {
        id: ActiveValue::Set(existing_cctx.id),
        lock: ActiveValue::Set(false),
        ..Default::default()
    })
    .filter(cross_chain_tx::Column::Id.eq(existing_cctx.id))
    .exec(&tx)
    .await?;

    //commit transaction
    tx.commit().await?;

    Ok(())
}
#[instrument(skip(db, client), fields(job_id = %job_id))]
async fn gap_fill(
    job_id: Uuid,
    db: &DatabaseConnection,
    client: &Client,
    watermark: watermark::Model,
) -> anyhow::Result<()> {
    let response = client.list_cctx(Some(&watermark.pointer), true, 100,job_id).await.unwrap();
    let cctxs = response.cross_chain_tx;
    let last_cctx = cctxs.last().unwrap();
    let last_cctx = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq(&last_cctx.index))
        .one(db)
        .await
        .unwrap();

    //begin transaction
    let tx = db.begin().await?;

    //insert transactions
    if last_cctx.is_none() {
        //update watermark pointer to the next key and unlock
        watermark::Entity::update(watermark::ActiveModel {
            id: ActiveValue::Set(watermark.id),
            pointer: ActiveValue::Set(response.pagination.next_key),
            lock: ActiveValue::Set(false),
            updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
            ..Default::default()
        })
        .filter(watermark::Column::Id.eq(watermark.id))
        .exec(&tx)
        .await?;
    }
    
    // Batch insert all transactions
    batch_insert_transactions(job_id, &tx, &cctxs).await?;
    
    tx.commit().await?;
    Ok(())
}

#[instrument(skip(db), fields(watermark_id = %watermark.id))]
pub async fn unlock_watermark(db: &DatabaseConnection, watermark: watermark::Model) -> anyhow::Result<()> {
    let res = watermark::Entity::update(watermark::ActiveModel {
        id: ActiveValue::Unchanged(watermark.id),
        lock: ActiveValue::Set(false),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        ..Default::default()
    })
    .filter(watermark::Column::Id.eq(watermark.id))
    .exec(db)
    .instrument(tracing::info_span!("unlocking watermark", watermark_id = %watermark.id))
    .await?;

    tracing::info!("unlocked watermark: {:?}", res);
    Ok(())
}

pub async fn lock_watermark(db: &DatabaseConnection, watermark: watermark::Model) -> anyhow::Result<()> {
    watermark::Entity::update(watermark::ActiveModel {
        id: ActiveValue::Unchanged(watermark.id),
        lock: ActiveValue::Set(true),
        ..Default::default()
    }).filter(watermark::Column::Id.eq(watermark.id))
    .exec(db)
    .await?;
    Ok(())
}
async fn unlock_cctx(db: &DatabaseConnection, id: i32) -> anyhow::Result<()> {
    cross_chain_tx::Entity::update(cross_chain_tx::ActiveModel {
        id: ActiveValue::Unchanged(id),
        lock: ActiveValue::Set(false),
        ..Default::default()
    })
    .filter(cross_chain_tx::Column::Id.eq(id))
    .exec(db)
    .await?;
    Ok(())
}
#[instrument(skip(db, client), fields(job_id = %job_id))]
async fn historical_sync(
    db: &DatabaseConnection,
    client: &Client,
    watermark: watermark::Model,
    job_id: Uuid,
    batch_size: u32,
) -> anyhow::Result<()> {
    let response = client
        .list_cctx(Some(&watermark.pointer), true, batch_size,job_id)
        .instrument( tracing::info_span!("fetching historical data from node", job_id = %job_id))
        .await?;
    let cross_chain_txs = response.cross_chain_tx;
    let pagination = response.pagination;
    //atomically insert cctxs and update watermark
    let tx = db.begin().await?;
    if let Err(e) = batch_insert_transactions(job_id, &tx, &cross_chain_txs).await {
        tracing::error!(error = %e, "Failed to batch insert transactions, pointer = {}", watermark.pointer);
        tracing::error!("cross_chain_txs: {:?}", cross_chain_txs);
        return Err(e);
    }
    let watermark_id = watermark.id; 
    let mut watermark_active:watermark::ActiveModel = watermark.into();
    watermark_active.id = ActiveValue::Unchanged(watermark_id);
    watermark_active.pointer = ActiveValue::Set(pagination.next_key);
    watermark_active.lock = ActiveValue::Set(false);
    watermark_active.updated_at = ActiveValue::Set(Utc::now().naive_utc());
    watermark_active.update(&tx).await?;
    tx.commit().await?;
    Ok(())
}



#[instrument(skip(db, client), fields(job_id = %job_id))]
async fn realtime_fetch(job_id: Uuid,db: &DatabaseConnection, client: &Client) -> anyhow::Result<()> {
    let response = client
        .list_cctx(None, false, 10, job_id)
        .instrument(tracing::info_span!("requesting realtime cctxs", job_id = %job_id))
        .await
        .unwrap();
    let txs = response.cross_chain_tx;
    if txs.is_empty() {
        tracing::info!("No new cctxs found");
        return Ok(());
    }
    let next_key = response.pagination.next_key;

    //check whether the latest fetched cctx is present in the database
    //we fetch transaction in LIFO order
    let latest_fetched = txs.first().unwrap();
    let latest_loaded = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq(&latest_fetched.index))
        .one(db)
        .instrument(tracing::info_span!("query latest cctx from db", job_id = %job_id))
        .await
        .unwrap();

    //if latest fetched cctx is in the db that means that the upper boundary is already covered and there is no new cctxs to save
    if latest_loaded.is_some() {
        tracing::debug!("latest cctx already exists");
        return Ok(());
    }
    //now we need to check the lower boudary ( the earliest of the fetched cctxs)
    let earliest_fetched = txs.last().unwrap();
    let earliest_loaded = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq(&earliest_fetched.index))
            .one(db)
            .instrument(tracing::info_span!("query earliest cctx from db", job_id = %job_id))
            .await
            .unwrap();
    
    if earliest_loaded.is_none() {
            // the lower boundary is not covered, so there could be more transaction that happened earlier, that we haven't observed
            //we have to save current pointer to continue fetching until we hit a known transaction
            tracing::info!("no last cctx found, creating new realtime watermark");
            watermark::Entity::insert(watermark::ActiveModel {
                id: ActiveValue::NotSet,
                watermark_type: ActiveValue::Set(WatermarkType::Realtime),
                pointer: ActiveValue::Set(next_key),
                lock: ActiveValue::Set(false),
                created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
            })
            .exec(db)
            .instrument(tracing::info_span!("creating new realtime watermark", job_id = %job_id))
            .await
            .unwrap();
    } 

    if let Err(e) = batch_insert_transactions(job_id, db, &txs).await {
        tracing::error!(error = %e, "Failed to batch insert transactions");
        return Err(e);
    }

    Ok(())
}

#[instrument(skip(db, tx), fields(tx_index = %tx.index, job_id = %job_id))]
async fn insert_transaction<C: ConnectionTrait>(db: &C, tx: CrossChainTx, job_id: Uuid) -> anyhow::Result<()> {
    // Insert main cross_chain_tx record
    let index = tx.index.clone();
    let cctx_model = cross_chain_tx::ActiveModel {
        id: ActiveValue::NotSet,
        creator: ActiveValue::Set(tx.creator),
        index: ActiveValue::Set(tx.index),
        lock: ActiveValue::Set(false),
        zeta_fees: ActiveValue::Set(tx.zeta_fees),
        relayed_message: ActiveValue::Set(Some(tx.relayed_message)),
        protocol_contract_version: ActiveValue::Set(tx.protocol_contract_version),
        last_status_update_timestamp: ActiveValue::Set(Utc::now().naive_utc()),
    };

    let tx_result = cross_chain_tx::Entity::insert(cctx_model)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(cross_chain_tx::Column::Index)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .instrument(tracing::info_span!("inserting cctx", index = %index, job_id = %job_id))
        .await?;

    // Get the inserted tx id
    let tx_id = tx_result.last_insert_id;

    // Insert cctx_status
    let status_model = cctx_status::ActiveModel {
        id: ActiveValue::NotSet,
        cross_chain_tx_id: ActiveValue::Set(tx_id),
        status: ActiveValue::Set(CctxStatusStatus::try_from(tx.cctx_status.status.clone()).map_err(|e| anyhow::anyhow!(e))?),
        status_message: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.status_message))),
        error_message: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message))),
        last_update_timestamp: ActiveValue::Set(
            // parse NaiveDateTime from epoch seconds
            chrono::DateTime::from_timestamp(
                tx.cctx_status.last_update_timestamp.parse::<i64>().unwrap(),
                0,
            )
            .unwrap()
            .naive_utc(),
        ),
        is_abort_refunded: ActiveValue::Set(tx.cctx_status.is_abort_refunded),
        created_timestamp: ActiveValue::Set(tx.cctx_status.created_timestamp),
        error_message_revert: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message_revert))),
        error_message_abort: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message_abort))),
    };
    cctx_status::Entity::insert(status_model)
        .exec(db)
        .instrument(tracing::info_span!("inserting cctx_status", index = %index, job_id = %job_id))
        .await?;

    // Insert inbound_params
    let inbound_model = inbound_params::ActiveModel {
        id: ActiveValue::NotSet,
        cross_chain_tx_id: ActiveValue::Set(tx_id),
        sender: ActiveValue::Set(tx.inbound_params.sender),
        sender_chain_id: ActiveValue::Set(tx.inbound_params.sender_chain_id),
        tx_origin: ActiveValue::Set(tx.inbound_params.tx_origin),
        coin_type: ActiveValue::Set(tx.inbound_params.coin_type),
        asset: ActiveValue::Set(Some(tx.inbound_params.asset)),
        amount: ActiveValue::Set(tx.inbound_params.amount),
        observed_hash: ActiveValue::Set(tx.inbound_params.observed_hash),
        observed_external_height: ActiveValue::Set(tx.inbound_params.observed_external_height),
        ballot_index: ActiveValue::Set(tx.inbound_params.ballot_index),
        finalized_zeta_height: ActiveValue::Set(tx.inbound_params.finalized_zeta_height),
        tx_finalization_status: ActiveValue::Set(
            TxFinalizationStatus::try_from(tx.inbound_params.tx_finalization_status)
                .map_err(|e| anyhow::anyhow!(e))?,
        ),
        is_cross_chain_call: ActiveValue::Set(tx.inbound_params.is_cross_chain_call),
        status: ActiveValue::Set(tx.inbound_params.status),
        confirmation_mode: ActiveValue::Set(tx.inbound_params.confirmation_mode),
    };
    inbound_params::Entity::insert(inbound_model)
        .exec(db)
        .instrument(tracing::info_span!("inserting inbound_params", index = %index, job_id = %job_id))
        .await?;

    // Insert outbound_params
    for outbound in tx.outbound_params {
        let outbound_model = outbound_params::ActiveModel {
            id: ActiveValue::NotSet,
            cross_chain_tx_id: ActiveValue::Set(tx_id),
            receiver: ActiveValue::Set(outbound.receiver),
            receiver_chain_id: ActiveValue::Set(outbound.receiver_chain_id),
            coin_type: ActiveValue::Set(outbound.coin_type),
            amount: ActiveValue::Set(outbound.amount),
            tss_nonce: ActiveValue::Set(outbound.tss_nonce),
            gas_limit: ActiveValue::Set(outbound.gas_limit),
            gas_price: ActiveValue::Set(Some(outbound.gas_price)),
            gas_priority_fee: ActiveValue::Set(Some(outbound.gas_priority_fee)),
            hash: ActiveValue::Set(Some(outbound.hash)),
            ballot_index: ActiveValue::Set(Some(outbound.ballot_index)),
            observed_external_height: ActiveValue::Set(outbound.observed_external_height),
            gas_used: ActiveValue::Set(outbound.gas_used),
            effective_gas_price: ActiveValue::Set(outbound.effective_gas_price),
            effective_gas_limit: ActiveValue::Set(outbound.effective_gas_limit),
            tss_pubkey: ActiveValue::Set(outbound.tss_pubkey),
            tx_finalization_status: ActiveValue::Set(
                TxFinalizationStatus::try_from(outbound.tx_finalization_status)
                    .map_err(|e| anyhow::anyhow!(e))?,
            ),
            call_options_gas_limit: ActiveValue::Set(outbound.call_options.clone().map(|c| c.gas_limit)),
            call_options_is_arbitrary_call: ActiveValue::Set(
                outbound.call_options.map(|c| c.is_arbitrary_call),
            ),
            confirmation_mode: ActiveValue::Set(outbound.confirmation_mode),
        };
        outbound_params::Entity::insert(outbound_model)
            .exec(db)
            .instrument(tracing::info_span!("inserting outbound_params", index = %index, job_id = %job_id))
            .await?;
    }

    // Insert revert_options
    let revert_model = revert_options::ActiveModel {
        id: ActiveValue::NotSet,
        cross_chain_tx_id: ActiveValue::Set(tx_id),
        revert_address: ActiveValue::Set(Some(tx.revert_options.revert_address)),
        call_on_revert: ActiveValue::Set(tx.revert_options.call_on_revert),
        abort_address: ActiveValue::Set(Some(tx.revert_options.abort_address)),
        revert_message: ActiveValue::Set(tx.revert_options.revert_message),
        revert_gas_limit: ActiveValue::Set(tx.revert_options.revert_gas_limit),
    };
    revert_options::Entity::insert(revert_model)
        .exec(db)
        .instrument(tracing::info_span!("inserting revert_options", index = %index, job_id = %job_id))
        .await?;

    Ok(())
}

/// Batch insert multiple CrossChainTx records with their child entities
#[instrument(skip(db, transactions), fields(job_id = %job_id))]
async fn batch_insert_transactions<C: ConnectionTrait>(
    job_id: Uuid,
    db: &C,
    transactions: &Vec<CrossChainTx>,
) -> anyhow::Result<()> {
    if transactions.is_empty() {
        return Ok(());
    }

    // Prepare batch data for each table
    let mut cctx_models = Vec::new();
    let mut status_models = Vec::new();
    let mut inbound_models = Vec::new();
    let mut outbound_models = Vec::new();
    let mut revert_models = Vec::new();

    // First, prepare all the parent records
    for tx in transactions {
        let cctx_model = cross_chain_tx::ActiveModel {
            id: ActiveValue::NotSet,
            creator: ActiveValue::Set(tx.creator.clone()),
            index: ActiveValue::Set(tx.index.clone()),
            lock: ActiveValue::Set(false),
            zeta_fees: ActiveValue::Set(tx.zeta_fees.clone()),
            relayed_message: ActiveValue::Set(Some(tx.relayed_message.clone())),
            protocol_contract_version: ActiveValue::Set(tx.protocol_contract_version.clone()),
            last_status_update_timestamp: ActiveValue::Set(Utc::now().naive_utc()),
        };
        cctx_models.push(cctx_model);
    }

    // Batch insert parent records
    let _cctx_results = cross_chain_tx::Entity::insert_many(cctx_models)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(cross_chain_tx::Column::Index)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .instrument(tracing::info_span!("inserting cctxs", job_id = %job_id))
        .await?;

    // Get the inserted IDs - we need to query them since we can't get them from insert_many
    let inserted_indices: Vec<String> = transactions.iter().map(|tx| tx.index.clone()).collect();
    let inserted_cctxs = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.is_in(inserted_indices))
        .all(db)
        .await?;

    // Create a map from index to id for quick lookup
    let index_to_id: std::collections::HashMap<String, i32> = inserted_cctxs
        .into_iter()
        .map(|cctx| (cctx.index, cctx.id))
        .collect();

    // Now prepare child records for each transaction
    for tx in transactions {
        if let Some(&tx_id) = index_to_id.get(&tx.index) {
            // Prepare cctx_status
            let status_model = cctx_status::ActiveModel {
                id: ActiveValue::NotSet,
                cross_chain_tx_id: ActiveValue::Set(tx_id),
                status: ActiveValue::Set(CctxStatusStatus::try_from(tx.cctx_status.status.clone()).map_err(|e| anyhow::anyhow!(e))?),
                status_message: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.status_message.clone()))),
                error_message: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message.clone()))),
                last_update_timestamp: ActiveValue::Set(
                    chrono::DateTime::from_timestamp(
                        tx.cctx_status.last_update_timestamp.parse::<i64>().unwrap(),
                        0,
                    )
                    .unwrap()
                    .naive_utc(),
                ),
                is_abort_refunded: ActiveValue::Set(tx.cctx_status.is_abort_refunded),
                created_timestamp: ActiveValue::Set(tx.cctx_status.created_timestamp.clone()),
                error_message_revert: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message_revert.clone()))),
                error_message_abort: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message_abort.clone()))),
            };
            status_models.push(status_model);

            // Prepare inbound_params
            let inbound_model = inbound_params::ActiveModel {
                id: ActiveValue::NotSet,
                cross_chain_tx_id: ActiveValue::Set(tx_id),
                sender: ActiveValue::Set(tx.inbound_params.sender.clone()),
                sender_chain_id: ActiveValue::Set(tx.inbound_params.sender_chain_id.clone()),
                tx_origin: ActiveValue::Set(tx.inbound_params.tx_origin.clone()),
                coin_type: ActiveValue::Set(tx.inbound_params.coin_type.clone()),
                asset: ActiveValue::Set(Some(tx.inbound_params.asset.clone())),
                amount: ActiveValue::Set(tx.inbound_params.amount.clone()),
                observed_hash: ActiveValue::Set(tx.inbound_params.observed_hash.clone()),
                observed_external_height: ActiveValue::Set(tx.inbound_params.observed_external_height.clone()),
                ballot_index: ActiveValue::Set(tx.inbound_params.ballot_index.clone()),
                finalized_zeta_height: ActiveValue::Set(tx.inbound_params.finalized_zeta_height.clone()),
                tx_finalization_status: ActiveValue::Set(
                    TxFinalizationStatus::try_from(tx.inbound_params.tx_finalization_status.clone())
                        .map_err(|e| anyhow::anyhow!(e))?,
                ),
                is_cross_chain_call: ActiveValue::Set(tx.inbound_params.is_cross_chain_call),
                status: ActiveValue::Set(tx.inbound_params.status.clone()),
                confirmation_mode: ActiveValue::Set(tx.inbound_params.confirmation_mode.clone()),
            };
            inbound_models.push(inbound_model);

            // Prepare outbound_params
            for outbound in &tx.outbound_params {
                let outbound_model = outbound_params::ActiveModel {
                    id: ActiveValue::NotSet,
                    cross_chain_tx_id: ActiveValue::Set(tx_id),
                    receiver: ActiveValue::Set(outbound.receiver.clone()),
                    receiver_chain_id: ActiveValue::Set(outbound.receiver_chain_id.clone()),
                    coin_type: ActiveValue::Set(outbound.coin_type.clone()),
                    amount: ActiveValue::Set(outbound.amount.clone()),
                    tss_nonce: ActiveValue::Set(outbound.tss_nonce.clone()),
                    gas_limit: ActiveValue::Set(outbound.gas_limit.clone()),
                    gas_price: ActiveValue::Set(Some(outbound.gas_price.clone())),
                    gas_priority_fee: ActiveValue::Set(Some(outbound.gas_priority_fee.clone())),
                    hash: ActiveValue::Set(Some(outbound.hash.clone())),
                    ballot_index: ActiveValue::Set(Some(outbound.ballot_index.clone())),
                    observed_external_height: ActiveValue::Set(outbound.observed_external_height.clone()),
                    gas_used: ActiveValue::Set(outbound.gas_used.clone()),
                    effective_gas_price: ActiveValue::Set(outbound.effective_gas_price.clone()),
                    effective_gas_limit: ActiveValue::Set(outbound.effective_gas_limit.clone()),
                    tss_pubkey: ActiveValue::Set(outbound.tss_pubkey.clone()),
                    tx_finalization_status: ActiveValue::Set(
                        TxFinalizationStatus::try_from(outbound.tx_finalization_status.clone())
                            .map_err(|e| anyhow::anyhow!(e))?,
                    ),
                    call_options_gas_limit: ActiveValue::Set(outbound.call_options.clone().map(|c| c.gas_limit.clone())),
                    call_options_is_arbitrary_call: ActiveValue::Set(
                        outbound.call_options.clone().map(|c| c.is_arbitrary_call),
                    ),
                    confirmation_mode: ActiveValue::Set(outbound.confirmation_mode.clone()),
                };
                outbound_models.push(outbound_model);
            }

            // Prepare revert_options
            let revert_model = revert_options::ActiveModel {
                id: ActiveValue::NotSet,
                cross_chain_tx_id: ActiveValue::Set(tx_id),
                revert_address: ActiveValue::Set(Some(tx.revert_options.revert_address.clone())),
                call_on_revert: ActiveValue::Set(tx.revert_options.call_on_revert),
                abort_address: ActiveValue::Set(Some(tx.revert_options.abort_address.clone())),
                revert_message: ActiveValue::Set(tx.revert_options.revert_message.clone()),
                revert_gas_limit: ActiveValue::Set(tx.revert_options.revert_gas_limit.clone()),
            };
            revert_models.push(revert_model);
        }
    }

    // Batch insert all child records
    if !status_models.is_empty() {
        cctx_status::Entity::insert_many(status_models).on_conflict(
            sea_orm::sea_query::OnConflict::column(cctx_status::Column::CrossChainTxId)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .instrument(tracing::info_span!("inserting cctx_status", job_id = %job_id))
        .await?;
    }
    
    Ok(())
}

fn prio_left(_: &mut ()) -> PollNext {
    PollNext::Left
}
impl Indexer {
    pub fn new(
        settings: IndexerSettings,
        db: Arc<DatabaseConnection>,
        client: Arc<Client>,
    ) -> Self {
        Self {
            settings,
            db,
            client,
        }
    }

    fn realtime_fetch_handler(&self) -> JoinHandle<()> {
        let db = self.db.clone();
        let polling_interval = self.settings.polling_interval;
        let client = self.client.clone();
        tokio::spawn(async move {
            loop {
                let job_id = Uuid::new_v4();
                realtime_fetch(job_id, &db, &client)
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(polling_interval)).await;
            }
        })
    }

    
    pub async fn run(&self) {
        
        tracing::info!("initializing indexer");
        let db = self.db.clone();
        // select cctxs where outbound_params.tx_finalization_status is not final
        
        tracing::info!("checking if historical watermark exists");

        //insert historical watermarks if there are no watermarks for historical type
        let historical_watermark = watermark::Entity::find()
        .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
        .one(db.as_ref())
        .await
        .unwrap();

        tracing::info!("removing lock from cctxs");
        cross_chain_tx::Entity::update_many()
        .filter(cross_chain_tx::Column::Lock.eq(true))
        .set(cross_chain_tx::ActiveModel {
            lock: ActiveValue::Set(false),
            ..Default::default()
        })
        .exec(db.as_ref())
        .await
        .unwrap();
        
        if historical_watermark.is_none() {  
            tracing::info!("inserting historical watermark");
            watermark::Entity::insert(watermark::ActiveModel {
                watermark_type: ActiveValue::Set(WatermarkType::Historical),
                lock: ActiveValue::Set(false),
                pointer: ActiveValue::Set("MH==".to_string()), //0 in base64
                created_at: ActiveValue::Set(Utc::now().naive_utc()),
                updated_at: ActiveValue::Set(Utc::now().naive_utc()),
                id: ActiveValue::NotSet,
            })
            .exec(db.as_ref())
            .await
            .unwrap();
        } else {
            tracing::info!("historical watermark already exist, pointer: {}", historical_watermark.unwrap().pointer);
        }
        watermark::Entity::update_many()
        .filter(watermark::Column::Lock.eq(true))
        .set(watermark::ActiveModel {
            lock: ActiveValue::Set(false),
            ..Default::default()
        })
        .exec(db.as_ref())
        .await
        .unwrap();

        tracing::info!("setup completed, initializing streams");
        let status_update_batch_size = self.settings.status_update_batch_size;
        let status_update_stream = Box::pin(async_stream::stream! {
            loop {

                let statement = format!(r#"
                WITH cctxs AS (
                    SELECT cctx.id, cctx.index, cctx.last_status_update_timestamp
                    FROM cross_chain_tx cctx
                    JOIN cctx_status cs ON cctx.id = cs.cross_chain_tx_id
                    WHERE cs.status IN ('PendingInbound', 'PendingOutbound', 'PendingRevert')
                    AND cctx.lock = false
                    ORDER BY cctx.last_status_update_timestamp ASC
                    LIMIT {status_update_batch_size}
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE cross_chain_tx cctx
                SET lock = true, last_status_update_timestamp = NOW()
                WHERE id IN (SELECT id FROM cctxs)
                RETURNING id, index, last_status_update_timestamp, lock, creator, index, relayed_message, protocol_contract_version, zeta_fees
                "#
            );

            println!("{statement}");

                let statement = Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    statement,
                    vec![],
                );

                match cross_chain_tx::Entity::find()
                .from_raw_sql(statement)
                .all(db.as_ref())
                .await {
                    std::result::Result::Ok(cctxs) => {
                        if cctxs.is_empty() {
                            tracing::info!("no cctxs to update");
                        } else {
                        for cctx in cctxs {
                            yield IndexerJob::StatusUpdate(cctx, Uuid::new_v4());
                            }
                        }
                        
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to fetch cctxs for status update");
                    }
                }
                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
                
            }
        });

        let db = self.db.clone();
        // checks whether the realtme fetcher hasn't actually fetched all the data in a single request, so we might be lagging behind
        let gap_fill_stream = Box::pin(async_stream::stream! {
            loop {

                let watermarks = watermark::Entity::find()
                    .filter(watermark::Column::WatermarkType.eq(WatermarkType::Realtime))
                    .filter(watermark::Column::Lock.eq(false))
                    .filter(watermark::Column::UpdatedAt.lt(Utc::now() - Duration::from_secs(10))) //TODO: make it configurable
                    .all(db.as_ref())
                    .await
                    .unwrap();

                for watermark in watermarks {
                    //update watermark lock to true
                    watermark::Entity::update(watermark::ActiveModel {
                        id: ActiveValue::Set(watermark.id),
                        lock: ActiveValue::Set(true),
                        ..Default::default()
                    })
                    .filter(watermark::Column::Id.eq(watermark.id))
                    .exec(db.as_ref())
                    .await
                    .unwrap();

                    yield IndexerJob::GapFill(watermark, Uuid::new_v4());
                }

                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
            }
        });

        let db = self.db.clone();
        let historical_stream = Box::pin(async_stream::stream! {
            loop {
                let job_id = Uuid::new_v4();
                let watermarks = watermark::Entity::find()
                    .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
                    .filter(watermark::Column::Lock.eq(false))
                    .one(db.as_ref())
                    .instrument(tracing::info_span!("looking for historical watermark",job_id = %job_id))
                    .await
                    .unwrap();

                    //TODO: add updated_by field to watermark model
                if let Some(watermark) = watermarks {
                    let mut model: watermark::ActiveModel = watermark.clone().into();
                    model.id = ActiveValue::Unchanged(watermark.id); // Ensure primary key is set
                    model.lock = ActiveValue::Set(true);
                    model.updated_at = ActiveValue::Set(Utc::now().naive_utc());
                    model.update(db.as_ref()).await.unwrap();
                    yield IndexerJob::HistoricalDataFetch(watermark, job_id);
                } else {
                    tracing::info!("job_id: {} historical watermark is absent or locked", job_id);
                }
                

                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
            }
        });

        // Priority strategy:
        // 1. Realtime fetch (highest priority - must run at configured frequency)
        // 2. Gap fill (medium priority - if there's a gap, we're lagging behind)
        // 3. Status update for new cctxs (medium priority)
        // 4. Historical sync (lowest priority - can lag without affecting realtime)
        let combined_stream =
            select_with_strategy(status_update_stream, historical_stream, prio_left);
        let combined_stream = select_with_strategy(gap_fill_stream, combined_stream, prio_left);
        
        combined_stream
            .for_each_concurrent(Some(self.settings.concurrency as usize), |job| {
                let client = self.client.clone();
                let db = self.db.clone();
                let batch_size = self.settings.historical_batch_size;
                tokio::spawn(async move {
                    match job {
                        IndexerJob::StatusUpdate(existing_cctx, job_id) => {
                            let cctx_id = existing_cctx.id;
                            if let Err(e) =    update_cctx_status(job_id, &db, &client, existing_cctx)
                            .await {
                                tracing::error!(error = %e, job_id = %job_id, "Failed to update cctx status");
                                unlock_cctx(&db, cctx_id).await.unwrap();
                            }
                        }
                        IndexerJob::GapFill(watermark, job_id) => {
                            if let Err(e) = gap_fill(job_id, &db, &client, watermark.clone())
                            .await {
                                tracing::error!(error = %e, job_id = %job_id, "Failed to fetch gap fill data");
                            }
                            unlock_watermark(&db, watermark).await.unwrap();
                        }
                        IndexerJob::HistoricalDataFetch(watermark, job_id) => { 
                            if let Err(e) =historical_sync(&db, &client, watermark.clone(), job_id, batch_size)
                            .await {
                                tracing::error!(error = %e, job_id = %job_id, "Failed to fetch historical data");
                                unlock_watermark(&db, watermark).await.unwrap();
                            }
                            // unlock_watermark(&db, watermark).await.unwrap();
                        }
                    }
                });
                futures::future::ready(())
            })
            .await;

        let realtime_handler = self.realtime_fetch_handler();
        realtime_handler.await.unwrap();
    }
}
