use std::sync::Arc;
use std::time::Duration;

use anyhow::Ok;
use uuid::Uuid;
use crate::models::{CrossChainTx, OutboundParams, InboundParams, RevertOptions};
use zetachain_cctx_entity::prelude::CrossChainTx as CrossChainTxModel;
use zetachain_cctx_entity::sea_orm_active_enums::{TxFinalizationStatus, WatermarkType};
use zetachain_cctx_entity::{
    cctx_status, cross_chain_tx, inbound_params, outbound_params, revert_options, watermark,
};

use sea_orm::{ColumnTrait, ConnectionTrait, RelationTrait};

use crate::{client::Client, settings::IndexerSettings};
use chrono::{DateTime, Utc};

use futures::StreamExt;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use tracing::{instrument, Instrument};

use sea_orm::TransactionTrait;

use futures::stream::{select_with_strategy, PollNext};
pub struct Indexer {
    pub settings: IndexerSettings,
    pub db: Arc<DatabaseConnection>,
    pub client: Arc<Client>,
}

enum IndexerJob {
    RealtimeFetch(Uuid),                         // New job type for realtime data fetching
    StatusUpdate(cross_chain_tx::Model, Uuid),             //cctx index and id to be updated
    GapFill(watermark::Model, Uuid), // Watermark (pointer) to the next page of cctxs to be fetched
    HistoricalDataFetch(watermark::Model, Uuid), // Watermark (pointer) to the next page of cctxs to be fetched
}

async fn update_cctx_status(
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
async fn gap_fill(
    db: &DatabaseConnection,
    client: &Client,
    watermark: watermark::Model,
) -> anyhow::Result<()> {
    let pointer = watermark.pointer;
    let response = client.list_cctx(Some(pointer), true, 100).await.unwrap();
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
    batch_insert_transactions(&tx, cctxs).await?;
    
    tx.commit().await?;
    Ok(())
}

async fn historical_sync(
    db: &DatabaseConnection,
    client: &Client,
    watermark: watermark::Model,
) -> anyhow::Result<()> {
    let pointer = watermark.pointer;

    let response = client
        .list_cctx(Some(pointer), true, 100)
        .instrument(tracing::info_span!("historical_sync"))
        .await
        .unwrap();
    let cctxs = response.cross_chain_tx;

    //atomically insert cctxs and update watermark
    let tx = db.begin().await?;

    // Filter out transactions that already exist
    let mut new_transactions = Vec::new();
    for cctx in cctxs {
        let exists_result = cross_chain_tx::Entity::find()
            .filter(cross_chain_tx::Column::Index.eq(&cctx.index))
            .one(&tx)
            .await;
        
        match exists_result {
            std::result::Result::Ok(None) => {
                new_transactions.push(cctx);
            }
            std::result::Result::Ok(Some(_)) => {
                println!("transaction already exists: {}", cctx.index);
            }
            std::result::Result::Err(_) => {
                println!("error checking if transaction exists: {}", cctx.index);
            }
        }
    }

    // Batch insert all new transactions
    if !new_transactions.is_empty() {
        batch_insert_transactions(&tx, new_transactions).await?;
    }

    println!("setting response.pagination.next_key: {}", response.pagination.next_key);
    watermark::Entity::update(watermark::ActiveModel {
        id: ActiveValue::Set(watermark.id),
        pointer: ActiveValue::Set(response.pagination.next_key),
        lock: ActiveValue::Set(false),
        updated_at: ActiveValue::Set(Utc::now().naive_utc()),
        ..Default::default()
    })
    .filter(watermark::Column::Id.eq(watermark.id))
    .exec(&tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn realtime_fetch(db: &DatabaseConnection, client: &Client) -> anyhow::Result<()> {
    let response = client
        .list_cctx(None, false, 100)
        .instrument(tracing::info_span!("requesting realtime cctxs"))
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
        .instrument(tracing::info_span!("query first cctx"))
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
            .instrument(tracing::info_span!("query last cctx"))
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
            .instrument(tracing::info_span!("creating new realtime watermark"))
            .await
            .unwrap();
    } 

    // Filter out transactions that already exist and batch insert new ones
    let mut new_transactions = Vec::new();
    for tx in txs {
        let exists_result = cross_chain_tx::Entity::find()
            .filter(cross_chain_tx::Column::Index.eq(&tx.index))
            .one(db)
            .await;
        
        match exists_result {
            std::result::Result::Ok(None) => {
                new_transactions.push(tx);
            }
            std::result::Result::Ok(Some(_)) => {
                tracing::debug!("transaction already exists: {}", tx.index);
            }
            Err(_) => {
                tracing::error!("error checking if transaction exists: {}", tx.index);
            }
        }
    }

    // Batch insert all new transactions
    if !new_transactions.is_empty() {
        if let Err(e) = batch_insert_transactions(db, new_transactions).await {
            tracing::error!(error = %e, "Failed to batch insert transactions");
        }
    }

    Ok(())
}

#[instrument(skip(db, tx), fields(tx_index = %tx.index))]
async fn insert_transaction<C: ConnectionTrait>(db: &C, tx: CrossChainTx) -> anyhow::Result<()> {
    // Insert main cross_chain_tx record
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
        .instrument(tracing::info_span!("insert_tx"))
        .await?;

    // Get the inserted tx id
    let tx_id = tx_result.last_insert_id;

    // Insert cctx_status
    let status_model = cctx_status::ActiveModel {
        id: ActiveValue::NotSet,
        cross_chain_tx_id: ActiveValue::Set(tx_id),
        status: ActiveValue::Set(tx.cctx_status.status),
        status_message: ActiveValue::Set(Some(tx.cctx_status.status_message)),
        error_message: ActiveValue::Set(Some(tx.cctx_status.error_message)),
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
        error_message_revert: ActiveValue::Set(Some(tx.cctx_status.error_message_revert)),
        error_message_abort: ActiveValue::Set(Some(tx.cctx_status.error_message_abort)),
    };
    cctx_status::Entity::insert(status_model)
        .exec(db)
        .instrument(tracing::info_span!("insert_cctx_status"))
        .await
        .map_err(|e| {
            println!("insert_cctx_status error: {}", e);
            e
        })?;

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
        .instrument(tracing::info_span!("insert_inbound_params"))
        .await
        .map_err(|e| {
            println!("insert_inbound_params error: {}", e);
            e
        })?;

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
            call_options_gas_limit: ActiveValue::Set(outbound.call_options.gas_limit),
            call_options_is_arbitrary_call: ActiveValue::Set(
                outbound.call_options.is_arbitrary_call,
            ),
            confirmation_mode: ActiveValue::Set(outbound.confirmation_mode),
        };
        outbound_params::Entity::insert(outbound_model)
            .exec(db)
            .instrument(tracing::info_span!("insert_outbound_params"))
            .await
            .map_err(|e| {
                println!("insert_outbound_params error: {}", e);
                e
            })?;
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
        .instrument(tracing::info_span!("insert_revert_options"))
        .await
        .map_err(|e| {
            println!("insert_revert_options error: {}", e);
            e
        })?;

    Ok(())
}

/// Batch insert multiple CrossChainTx records with their child entities
async fn batch_insert_transactions<C: ConnectionTrait>(
    db: &C,
    transactions: Vec<CrossChainTx>,
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
    for tx in &transactions {
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
    let cctx_results = cross_chain_tx::Entity::insert_many(cctx_models)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(cross_chain_tx::Column::Index)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
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
    for tx in &transactions {
        if let Some(&tx_id) = index_to_id.get(&tx.index) {
            // Prepare cctx_status
            let status_model = cctx_status::ActiveModel {
                id: ActiveValue::NotSet,
                cross_chain_tx_id: ActiveValue::Set(tx_id),
                status: ActiveValue::Set(tx.cctx_status.status.clone()),
                status_message: ActiveValue::Set(Some(tx.cctx_status.status_message.clone())),
                error_message: ActiveValue::Set(Some(tx.cctx_status.error_message.clone())),
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
                error_message_revert: ActiveValue::Set(Some(tx.cctx_status.error_message_revert.clone())),
                error_message_abort: ActiveValue::Set(Some(tx.cctx_status.error_message_abort.clone())),
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
                    call_options_gas_limit: ActiveValue::Set(outbound.call_options.gas_limit.clone()),
                    call_options_is_arbitrary_call: ActiveValue::Set(
                        outbound.call_options.is_arbitrary_call,
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
        cctx_status::Entity::insert_many(status_models).exec(db).await?;
    }
    
    if !inbound_models.is_empty() {
        inbound_params::Entity::insert_many(inbound_models).exec(db).await?;
    }
    
    if !outbound_models.is_empty() {
        outbound_params::Entity::insert_many(outbound_models).exec(db).await?;
    }
    
    if !revert_models.is_empty() {
        revert_options::Entity::insert_many(revert_models).exec(db).await?;
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

    pub async fn run(&self) {
        // Realtime fetch stream - highest priority, runs at configured polling interval
        let realtime_stream = Box::pin(async_stream::stream! {
            loop {
                yield IndexerJob::RealtimeFetch(Uuid::new_v4());
                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
            }
        });

        let db = self.db.clone();
        // select cctxs where outbound_params.tx_finalization_status is not final
        let status_update_stream = Box::pin(async_stream::stream! {
            loop {
                let cctxs = cross_chain_tx::Entity::find()
                    .join(sea_orm::JoinType::InnerJoin, cross_chain_tx::Relation::OutboundParams.def())
                    .filter(outbound_params::Column::TxFinalizationStatus.ne(TxFinalizationStatus::Executed))
                    .filter(cross_chain_tx::Column::LastStatusUpdateTimestamp.lt(Utc::now() - Duration::from_secs(60)))
                    .filter(cross_chain_tx::Column::Lock.eq(false))
                    .all(db.as_ref())
                    .await
                    .unwrap();

                for cctx in cctxs {
                    tracing::info!(cctx_index = %cctx.index, "Status update");
                    cross_chain_tx::Entity::update(cross_chain_tx::ActiveModel {
                        id: ActiveValue::Set(cctx.id),
                        last_status_update_timestamp: ActiveValue::Set(Utc::now().naive_utc()),
                        lock: ActiveValue::Set(true),
                        ..Default::default()
                    })
                    .filter(cross_chain_tx::Column::Id.eq(cctx.id))
                    .exec(db.as_ref())
                    .await.unwrap();
                    yield IndexerJob::StatusUpdate(cctx, Uuid::new_v4());
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
                let watermarks = watermark::Entity::find()
                    .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
                    .one(db.as_ref())
                    .await
                    .unwrap();

                if let Some(watermark) = watermarks {
                    yield IndexerJob::HistoricalDataFetch(watermark, Uuid::new_v4());
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
        let combined_stream = select_with_strategy(realtime_stream, combined_stream, prio_left);

        combined_stream
            .for_each_concurrent(Some(self.settings.concurrency as usize), |job| {
                let client = self.client.clone();
                let db = self.db.clone();
                tokio::spawn(async move {
                    match job {
                        IndexerJob::RealtimeFetch(span_id) => {
                            if let Err(e) = realtime_fetch(&db, &client)
                            .instrument(tracing::info_span!("processing realtime fetch job", span_id = %span_id))
                            .await {
                                tracing::error!(error = %e, "Failed to fetch realtime data");
                            }
                        }
                        IndexerJob::StatusUpdate(existing_cctx, span_id) => {
                            
                            update_cctx_status(&db, &client, existing_cctx)
                            .instrument(tracing::info_span!("processing status update job", span_id = %span_id))
                                .await
                                .unwrap();
                        }
                        IndexerJob::GapFill(watermark, span_id) => {
                            gap_fill(&db, &client, watermark)
                            .instrument(tracing::info_span!("processing gap fill job", span_id = %span_id))
                            .await
                            .unwrap();
                        }
                        IndexerJob::HistoricalDataFetch(watermark, span_id) => { 
                            historical_sync(&db, &client, watermark)
                            .instrument(tracing::info_span!("processing historical data fetch job", span_id = %span_id))
                            .await
                            .map_err(|e| println!("historical_sync error: {}", e)).unwrap();
                        }
                    }
                });
                futures::future::ready(())
            })
            .await;
    }
}
