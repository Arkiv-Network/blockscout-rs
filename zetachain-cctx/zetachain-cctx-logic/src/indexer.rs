use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use zetachain_cctx_entity::inbound_params::TxFinalizationStatus;
use zetachain_cctx_entity::{
    cross_chain_tx, cctx_status, inbound_params, outbound_params, revert_options,
    prelude::*, watermark
};

use chrono::Utc;
use sea_orm::prelude::DateTime;
use crate::{client::Client, settings::IndexerSettings, models::CrossChainTx};
use sea_orm::ColumnTrait;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use tracing::{instrument, Instrument};

pub struct Indexer {
    pub settings: IndexerSettings,
    pub db: Arc<DatabaseConnection>,
    pub client: Arc<Client>,
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

    #[instrument(skip(db, tx), fields(tx_index = %tx.index))]
    async fn insert_transaction(db: Arc<DatabaseConnection>, tx: CrossChainTx) -> anyhow::Result<()> {
        // Insert main cross_chain_tx record
        let tx_model = cross_chain_tx::ActiveModel {
            id: ActiveValue::NotSet,
            creator: ActiveValue::Set(tx.creator),
            index: ActiveValue::Set(tx.index),
            zeta_fees: ActiveValue::Set(tx.zeta_fees),
            relayed_message: ActiveValue::Set(Some(tx.relayed_message)),
            protocol_contract_version: ActiveValue::Set(
                tx.protocol_contract_version,
            )
        };
        
        let tx_result = cross_chain_tx::Entity::insert(tx_model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::column(cross_chain_tx::Column::Index)
                    .do_nothing()
                    .to_owned()
            )
            .exec(db.as_ref())
            .instrument(tracing::info_span!("insert_tx"))
            .await?;

        // Get the inserted tx id
        let (tx_id,_index) = tx_result.last_insert_id;

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
            .exec(db.as_ref())
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
            tx_finalization_status: ActiveValue::Set(TxFinalizationStatus::from(tx.inbound_params.tx_finalization_status)),
            is_cross_chain_call: ActiveValue::Set(tx.inbound_params.is_cross_chain_call),
            status: ActiveValue::Set(tx.inbound_params.status),
            confirmation_mode: ActiveValue::Set(tx.inbound_params.confirmation_mode),
        };
        inbound_params::Entity::insert(inbound_model)
            .exec(db.as_ref())
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
                tx_finalization_status: ActiveValue::Set(outbound.tx_finalization_status),
                call_options_gas_limit: ActiveValue::Set(outbound.call_options.gas_limit),
                call_options_is_arbitrary_call: ActiveValue::Set(outbound.call_options.is_arbitrary_call),
                confirmation_mode: ActiveValue::Set(outbound.confirmation_mode),
            };
            outbound_params::Entity::insert(outbound_model)
                .exec(db.as_ref())
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
            .exec(db.as_ref())
            .instrument(tracing::info_span!("insert_revert_options"))
            .await?;

        Ok(())
    }

    pub fn status_updater(&self) ->
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
                            pointer: ActiveValue::Set(Some(next_key)),
                            watermark_type: ActiveValue::Set(String::from("realtime")),
                            created_at: ActiveValue::Set(DateTime::from(Utc::now().naive_utc())),
                            updated_at: ActiveValue::Set(DateTime::from(Utc::now().naive_utc())),
                        })
                        .exec(db.as_ref())
                        .instrument(tracing::info_span!("creating new realtime watermark"))
                        .await
                        .unwrap();
                    }

                    for tx in txs {
                        let db = db.clone();
                        if let Err(e) = Indexer::insert_transaction(db, tx).await {
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
