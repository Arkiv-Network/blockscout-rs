use std::sync::Arc;
use std::time::Duration;

use anyhow::Ok;
use tokio::task::JoinHandle;
use zetachain_cctx_entity::sea_orm_active_enums::{TxFinalizationStatus, WatermarkType};
use zetachain_cctx_entity::{
    cctx_status, cross_chain_tx, inbound_params, outbound_params, prelude::*, revert_options,
    watermark,
};

use sea_orm::{ColumnTrait, ConnectionTrait, RelationTrait};

use crate::{client::Client, models::CrossChainTx, settings::IndexerSettings};
use chrono::Utc;

use futures::StreamExt;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use tracing::{instrument, Instrument};

use sea_orm::TransactionTrait;

use futures::stream::{select, select_with_strategy, BoxStream, PollNext};
pub struct Indexer {
    pub settings: IndexerSettings,
    pub db: Arc<DatabaseConnection>,
    pub client: Arc<Client>,
}

enum IndexerJob {
    StatusUpdate(String, i32),             //cctx index and id to be updated
    GapFill(watermark::Model), // Watermark (pointer) to the next page of cctxs to be fetched
    HistoricalDataFetch(watermark::Model), // Watermark (pointer) to the next page of cctxs to be fetched
}

async fn update_cctx_status(
    db: &DatabaseConnection,
    client: &Client,
    index: String,
    id: i32,
) -> anyhow::Result<()> {
    //get cctx from client then update outbound params in the database
    let cctx = client.get_cctx(&index).await.unwrap();
    let outbound_params = cctx.outbound_params;

    for outbound in outbound_params {
        //upsert outbound params based on hash field
        let outbound_model = outbound_params::ActiveModel {
            id: ActiveValue::NotSet,
            cross_chain_tx_id: ActiveValue::Set(id),
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
            .on_conflict(
                sea_orm::sea_query::OnConflict::column(outbound_params::Column::Hash)
                    .update_columns([
                        outbound_params::Column::TxFinalizationStatus,
                        outbound_params::Column::GasUsed,
                        outbound_params::Column::EffectiveGasPrice,
                        outbound_params::Column::EffectiveGasLimit,
                        outbound_params::Column::TssNonce,
                    ])
                    .to_owned(),
            )
            .exec(db)
            .await?;
    }

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
    //insert transactions
    for cctx in cctxs {
        insert_transaction(&tx, cctx).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn historical_sync(db: &DatabaseConnection, client: &Client, watermark: watermark::Model) -> anyhow::Result<()> {
    let pointer = watermark.pointer;

    let response = client.list_cctx(Some(pointer), true, 100).await.unwrap();
    let cctxs = response.cross_chain_tx;
    
    //atomically insert cctxs and update watermark
    let tx = db.begin().await?;

    for cctx in cctxs {
        insert_transaction(&tx, cctx).await?;
    }
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
    tx.commit().await?;
    Ok(())
}
#[instrument(skip(db, tx), fields(tx_index = %tx.index))]
async fn insert_transaction<C: ConnectionTrait>(db: &C, tx: CrossChainTx) -> anyhow::Result<()> {
    // Insert main cross_chain_tx record
    let tx_model = cross_chain_tx::ActiveModel {
        id: ActiveValue::NotSet,
        creator: ActiveValue::Set(tx.creator),
        index: ActiveValue::Set(tx.index),
        zeta_fees: ActiveValue::Set(tx.zeta_fees),
        relayed_message: ActiveValue::Set(Some(tx.relayed_message)),
        protocol_contract_version: ActiveValue::Set(tx.protocol_contract_version),
        last_status_update_timestamp: ActiveValue::NotSet,
    };

    let tx_result = cross_chain_tx::Entity::insert(tx_model)
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
        last_update_timestamp: ActiveValue::Set(tx.cctx_status.last_update_timestamp),
        is_abort_refunded: ActiveValue::Set(tx.cctx_status.is_abort_refunded),
        created_timestamp: ActiveValue::Set(tx.cctx_status.created_timestamp),
        error_message_revert: ActiveValue::Set(Some(tx.cctx_status.error_message_revert)),
        error_message_abort: ActiveValue::Set(Some(tx.cctx_status.error_message_abort)),
    };
    cctx_status::Entity::insert(status_model)
        .exec(db)
        .instrument(tracing::info_span!("insert_cctx_status"))
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
        .instrument(tracing::info_span!("insert_inbound_params"))
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
            call_options_gas_limit: ActiveValue::Set(outbound.call_options.gas_limit),
            call_options_is_arbitrary_call: ActiveValue::Set(
                outbound.call_options.is_arbitrary_call,
            ),
            confirmation_mode: ActiveValue::Set(outbound.confirmation_mode),
        };
        outbound_params::Entity::insert(outbound_model)
            .exec(db)
            .instrument(tracing::info_span!("insert_outbound_params"))
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
        .instrument(tracing::info_span!("insert_revert_options"))
        .await?;

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
        let db = self.db.clone();
        let status_update_stream = Box::pin(async_stream::stream! {
            loop {
                //select cctx where outbound_params.tx_finalization_status is not final
                //if there is cctx, yield the cctx index
                //sleep for 1 second
                //repeat

                let cctxs = cross_chain_tx::Entity::find()
                    .join(sea_orm::JoinType::InnerJoin, cross_chain_tx::Relation::OutboundParams.def())
                    .filter(outbound_params::Column::TxFinalizationStatus.ne(TxFinalizationStatus::Executed))
                    .filter(cross_chain_tx::Column::LastStatusUpdateTimestamp.lt(Utc::now() - Duration::from_secs(60)))
                    .all(db.as_ref())
                    .await
                    .unwrap();

                for cctx in cctxs {
                    yield IndexerJob::StatusUpdate(cctx.index.to_string(), cctx.id);
                }

                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
            }
        });

        let db = self.db.clone();
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

                    yield IndexerJob::GapFill(watermark);
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
                    yield IndexerJob::HistoricalDataFetch(watermark);
                }

                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
            }
        });

        //current strategy:
        //1. gap fill (because if there's a gap, it means that we're lagging behind)
        //2. status update for new cctxs
        //3. historical sync
        let combined_stream =
            select_with_strategy(status_update_stream, historical_stream, prio_left);
        let combined_stream = select_with_strategy(gap_fill_stream, combined_stream, prio_left);

        combined_stream
            .for_each_concurrent(Some(self.settings.concurrency as usize), |job| {
                let client = self.client.clone();
                let db = self.db.clone();
                tokio::spawn(async move {
                    match job {
                        IndexerJob::StatusUpdate(cctx_index, cctx_id) => {
                            tracing::info!(cctx_index = %cctx_index, "Status update");
                            update_cctx_status(&db, &client, cctx_index, cctx_id)
                                .await
                                .unwrap();
                        }
                        IndexerJob::GapFill(watermark) => {
                            tracing::info!(watermark = %watermark.pointer, "Gap fill");
                            gap_fill(&db, &client, watermark).await.unwrap();
                        }
                        IndexerJob::HistoricalDataFetch(watermark) => {
                            tracing::info!(watermark = %watermark.pointer, "Historical sync");
                        }
                    }
                });
                futures::future::ready(())
            })
            .await;
    }

    #[instrument(skip(self))]
    pub fn create_realtime_fetcher(&self) -> JoinHandle<()> {
        let db = self.db.clone();
        let client = self.client.clone();
        let polling_interval = self.settings.polling_interval;
        let realtime_fetcher = tokio::spawn(async move {
            loop {
                let response = client.list_cctx(None, false, 100).await.unwrap();
                let txs = response.cross_chain_tx;

                let next_key = response.pagination.next_key;

                //check whether the first of cctx in response is present in the database
                let first_cctx = txs.first().unwrap();
                let first_cctx = cross_chain_tx::Entity::find()
                    .filter(cross_chain_tx::Column::Index.eq(&first_cctx.index))
                    .one(db.as_ref())
                    .await
                    .unwrap();

                //if first_cctx is in the database, just wait for the next polling
                if first_cctx.is_some() {
                    //check whether the last of cctx in response is present in the database
                    let last_cctx = txs.last().unwrap();
                    let last_cctx = cross_chain_tx::Entity::find()
                        .filter(cross_chain_tx::Column::Index.eq(&last_cctx.index))
                        .one(db.as_ref())
                        .await
                        .unwrap();

                    if last_cctx.is_none() {
                        //there might be a gap, so we need to save pagination.next_key as a starting point for the gap-filling process
                        watermark::Entity::insert(watermark::ActiveModel {
                            id: ActiveValue::NotSet,
                            watermark_type: ActiveValue::Set(WatermarkType::Realtime),
                            pointer: ActiveValue::Set(next_key),
                            lock: ActiveValue::Set(false),
                            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                            updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
                        })
                        .exec(db.as_ref())
                        .instrument(tracing::info_span!("creating new realtime watermark"))
                        .await
                        .unwrap();
                    }

                    for tx in txs {
                        let db = db.clone();
                        if let Err(e) = insert_transaction(db.as_ref(), tx).await {
                            tracing::error!(error = %e, "Failed to insert transaction");
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(polling_interval)).await;
            }
        });
        realtime_fetcher
    }
}
