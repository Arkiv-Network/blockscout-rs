use std::sync::Arc;

use anyhow::Ok;
use chrono::Utc;
use sea_orm::{ActiveValue, DatabaseConnection, DbBackend, Statement, TransactionTrait};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use tracing::{instrument, Instrument};
use uuid::Uuid;
use zetachain_cctx_entity::sea_orm_active_enums::{CctxStatusStatus, CoinType, ConfirmationMode, InboundStatus, Kind, ProtocolContractVersion};
use zetachain_cctx_entity::{cctx_status, inbound_params, revert_options, watermark};
use zetachain_cctx_entity::{
    cross_chain_tx as CrossChainTxEntity, outbound_params as OutboundParamsEntity,
    sea_orm_active_enums::TxFinalizationStatus,
};

use crate::models::CrossChainTx;

pub struct ZetachainCctxDatabase {
    db: Arc<DatabaseConnection>,
}

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

// Add this struct to hold the complete CCTX data
#[derive(Debug)]
pub struct CompleteCctx {
    pub cctx: CrossChainTxEntity::Model,
    pub status: cctx_status::Model,
    pub inbound: inbound_params::Model,
    pub outbounds: Vec<OutboundParamsEntity::Model>,
    pub revert: revert_options::Model,
}

impl ZetachainCctxDatabase {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        Self { db }
    }

    pub async fn setup_db(&self) -> anyhow::Result<()> {
        tracing::debug!("checking if historical watermark exists");

        //insert historical watermarks if there are no watermarks for historical type
        let historical_watermark = watermark::Entity::find()
        .filter(watermark::Column::Kind.eq(Kind::Historical))
        .one(self.db.as_ref())
        .await?;

        tracing::debug!("removing lock from cctxs");
        CrossChainTxEntity::Entity::update_many()
        .filter(CrossChainTxEntity::Column::Lock.eq(true))
        .set(CrossChainTxEntity::ActiveModel {
            lock: ActiveValue::Set(false),
            ..Default::default()
        })
        .exec(self.db.as_ref())
        .await?;
        
        if historical_watermark.is_none() {  
            tracing::debug!("inserting historical watermark");
            watermark::Entity::insert(watermark::ActiveModel {
                kind: ActiveValue::Set(Kind::Historical),
                lock: ActiveValue::Set(false),
                pointer: ActiveValue::Set("MH==".to_string()), //0 in base64
                created_at: ActiveValue::Set(Utc::now().naive_utc()),
                updated_at: ActiveValue::Set(Utc::now().naive_utc()),
                id: ActiveValue::NotSet,
            })
            .exec(self.db.as_ref())
            .await?;
        } else {
            tracing::debug!("historical watermark already exist, pointer: {}", historical_watermark.unwrap().pointer);
        }
        watermark::Entity::update_many()
        .filter(watermark::Column::Lock.eq(true))
        .set(watermark::ActiveModel {
            lock: ActiveValue::Set(false),
            ..Default::default()
        })
        .exec(self.db.as_ref())
        .await?;

        Ok(())
    }

    pub async fn move_watermark(&self, watermark: watermark::Model,new_pointer: String) -> anyhow::Result<()> {
        
        watermark::Entity::update(watermark::ActiveModel {
            id: ActiveValue::Set(watermark.id),
            lock: ActiveValue::Set(false),
            pointer: ActiveValue::Set(new_pointer),
            updated_at:ActiveValue::Set(Utc::now().naive_utc()),
            ..Default::default()
        })
        .filter(watermark::Column::Id.eq(watermark.id))
        .exec(self.db.as_ref())
        .await.map_err(|e| anyhow::anyhow!(e))?;
Ok(())
    }
    pub async fn get_cctx(
        &self,
        index: String,
    ) -> anyhow::Result<Option<CrossChainTxEntity::Model>> {
        let cctx = CrossChainTxEntity::Entity::find()
            .filter(CrossChainTxEntity::Column::Index.eq(index))
            .one(self.db.as_ref())
            .await?;

        Ok(cctx)
    }

    pub async fn create_realtime_watermark(&self, pointer: String) -> anyhow::Result<()> {
        watermark::Entity::insert(watermark::ActiveModel {
            id: ActiveValue::NotSet,
            kind: ActiveValue::Set(Kind::Realtime),
            pointer: ActiveValue::Set(pointer),
            lock: ActiveValue::Set(false),
            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
            updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        })
        .exec(self.db.as_ref())
        .instrument(tracing::debug_span!("creating new realtime watermark"))
        .await.map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }


    /// Batch insert multiple CrossChainTx records with their child entities
#[instrument(,level="debug",skip(self, transactions), fields(job_id = %job_id))]
pub async fn batch_insert_transactions(
    &self,
    job_id: Uuid,
    transactions: &Vec<CrossChainTx>,
) -> anyhow::Result<()> {
    if transactions.is_empty() {
        return Ok(());
    }

    tracing::info!("inserting {} cctxs", transactions.iter().map(|tx| tx.index.clone()).collect::<Vec<String>>().join(", "));
    // Prepare batch data for each table
    let mut cctx_models = Vec::new();
    let mut status_models = Vec::new();
    let mut inbound_models = Vec::new();
    let mut outbound_models = Vec::new();
    let mut revert_models = Vec::new();

    // First, prepare all the parent records
    for tx in transactions {
        let cctx_model = CrossChainTxEntity::ActiveModel {
            id: ActiveValue::NotSet,
            creator: ActiveValue::Set(tx.creator.clone()),
            index: ActiveValue::Set(tx.index.clone()),
            lock: ActiveValue::Set(false),
            zeta_fees: ActiveValue::Set(tx.zeta_fees.clone()),
            relayed_message: ActiveValue::Set(Some(tx.relayed_message.clone())),
            protocol_contract_version: ActiveValue::Set(ProtocolContractVersion::try_from(tx.protocol_contract_version.clone()).map_err(|e| anyhow::anyhow!(e))?),
            last_status_update_timestamp: ActiveValue::Set(Utc::now().naive_utc()),
        };
        cctx_models.push(cctx_model);
    }

    // Batch insert parent records
    let inserted_cctxs = CrossChainTxEntity::Entity::insert_many(cctx_models)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(CrossChainTxEntity::Column::Index)
                .do_nothing()
                .to_owned(),
        )
        .exec_with_returning_many(self.db.as_ref())
        .instrument(tracing::debug_span!("inserting cctxs", job_id = %job_id))
        .await?;


    tracing::debug!("inserted cctxs: {:?}", inserted_cctxs.len());
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
                created_timestamp: ActiveValue::Set(tx.cctx_status.created_timestamp.parse::<i64>()?),
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
                coin_type: ActiveValue::Set(CoinType::try_from(tx.inbound_params.coin_type.clone()).map_err(|e| anyhow::anyhow!(e))?),
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
                status: ActiveValue::Set(InboundStatus::try_from(tx.inbound_params.status.clone()).map_err(|e| anyhow::anyhow!(e))?),
                confirmation_mode: ActiveValue::Set(ConfirmationMode::try_from(tx.inbound_params.confirmation_mode.clone()).map_err(|e| anyhow::anyhow!(e))?),
            };
            inbound_models.push(inbound_model);

            // Prepare outbound_params
            for outbound in &tx.outbound_params {
                let outbound_model = OutboundParamsEntity::ActiveModel {
                    id: ActiveValue::NotSet,
                    cross_chain_tx_id: ActiveValue::Set(tx_id),
                    receiver: ActiveValue::Set(outbound.receiver.clone()),
                    receiver_chain_id: ActiveValue::Set(outbound.receiver_chain_id.clone()),
                    coin_type: ActiveValue::Set(CoinType::try_from(outbound.coin_type.clone()).map_err(|e| anyhow::anyhow!(e))?),
                    amount: ActiveValue::Set(outbound.amount.clone()),
                    tss_nonce: ActiveValue::Set(outbound.tss_nonce.clone()),
                    gas_limit: ActiveValue::Set(outbound.gas_limit.clone()),
                    gas_price: ActiveValue::Set(Some(outbound.gas_price.clone())),
                    gas_priority_fee: ActiveValue::Set(Some(outbound.gas_priority_fee.clone())),
                    hash: ActiveValue::Set(outbound.hash.clone()),
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
                    confirmation_mode: ActiveValue::Set(ConfirmationMode::try_from(outbound.confirmation_mode.clone()).map_err(|e| anyhow::anyhow!(e))?),
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
        .exec(self.db.as_ref())
        .instrument(tracing::debug_span!("inserting cctx_status"))
        .await?;
    }
    
    Ok(())
}


#[instrument(,level="debug",skip(self), fields(watermark_id = %watermark.id))]
pub async fn unlock_watermark(&self, watermark: watermark::Model) -> anyhow::Result<()> {
    let res = watermark::Entity::update(watermark::ActiveModel {
        id: ActiveValue::Unchanged(watermark.id),
        lock: ActiveValue::Set(false),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        ..Default::default()
    })
    .filter(watermark::Column::Id.eq(watermark.id))
    .exec(self.db.as_ref())
    .instrument(tracing::debug_span!("unlocking watermark", watermark_id = %watermark.id))
    .await?;

    tracing::debug!("unlocked watermark: {:?}", res);
    Ok(())
}

pub async fn lock_watermark(&self, watermark: watermark::Model) -> anyhow::Result<()> {
    watermark::Entity::update(watermark::ActiveModel {
        id: ActiveValue::Unchanged(watermark.id),
        lock: ActiveValue::Set(true),
        ..Default::default()
    }).filter(watermark::Column::Id.eq(watermark.id))
    .exec(self.db.as_ref())
    .await?;
    Ok(())
}

pub async fn delete_watermark(&self, watermark: watermark::Model) -> anyhow::Result<()> {

    watermark::Entity::delete(watermark::ActiveModel {
        id: ActiveValue::Unchanged(watermark.id),
        ..Default::default()
    })
    .filter(watermark::Column::Id.eq(watermark.id))
    .exec(self.db.as_ref())
    .await?;
    Ok(())
}

pub async fn unlock_cctx(&self, id: i32) -> anyhow::Result<()> {
    CrossChainTxEntity::Entity::update(CrossChainTxEntity::ActiveModel {
        id: ActiveValue::Unchanged(id),
        lock: ActiveValue::Set(false),
        ..Default::default()
    })
    .filter(CrossChainTxEntity::Column::Id.eq(id))
    .exec(self.db.as_ref())
    .await?;
    Ok(())
}
#[instrument(,level="debug",skip(self), fields(job_id = %job_id))]
    pub async fn query_cctxs_for_status_update(
        &self,
        batch_size: u32,
        job_id: Uuid
    ) -> anyhow::Result<Vec<CrossChainTxEntity::Model>> {
        let statement = format!(r#"
        WITH cctxs AS (
            SELECT cctx.id, cctx.index, cctx.last_status_update_timestamp
            FROM cross_chain_tx cctx
            JOIN cctx_status cs ON cctx.id = cs.cross_chain_tx_id
            WHERE cs.status IN ('PendingInbound', 'PendingOutbound', 'PendingRevert')
            AND cctx.lock = false
            ORDER BY cctx.last_status_update_timestamp ASC
            LIMIT {batch_size}
            FOR UPDATE SKIP LOCKED
        )
        UPDATE cross_chain_tx cctx
        SET lock = true, last_status_update_timestamp = NOW()
        WHERE id IN (SELECT id FROM cctxs)
        RETURNING id, index, last_status_update_timestamp, lock, creator, index, relayed_message, protocol_contract_version, zeta_fees
        "#
    );

        let statement = Statement::from_sql_and_values(
            DbBackend::Postgres,
            statement,
            vec![],
        );

        
        CrossChainTxEntity::Entity::find()
        .from_raw_sql(statement)
        .all(self.db.as_ref())
        .await.map_err(|e| anyhow::anyhow!(e))
        
    }

    #[instrument(,level="debug",skip(self), fields(job_id = %job_id))]
    pub async fn update_cctx_status(
        &self,
        job_id: Uuid, 
        cctx: CrossChainTxEntity::Model,
        fetched_cctx: CrossChainTx,
    ) -> anyhow::Result<()> {
        let outbound_params = fetched_cctx.outbound_params;

        let tx = self.db.begin().await?;
        for outbound_params in outbound_params {
            if let Some(record) = OutboundParamsEntity::Entity::find()
                .filter(OutboundParamsEntity::Column::CrossChainTxId.eq(Some(cctx.id)))
                .filter(OutboundParamsEntity::Column::Receiver.eq(outbound_params.receiver))
                .one(self.db.as_ref())
                .await
                .unwrap()
            {
                OutboundParamsEntity::Entity::update(OutboundParamsEntity::ActiveModel {
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
                .filter(OutboundParamsEntity::Column::Id.eq(record.id))
                .exec(self.db.as_ref())
                .await?;
            }
        }
        //unlock cctx
        CrossChainTxEntity::Entity::update(CrossChainTxEntity::ActiveModel {
            id: ActiveValue::Set(cctx.id),
            lock: ActiveValue::Set(false),
            ..Default::default()
        })
        .filter(CrossChainTxEntity::Column::Id.eq(cctx.id))
        .exec(&tx)
        .await?;

        //commit transaction
        tx.commit().await?;

        Ok(())
    }
    #[instrument(,level="debug",skip(self, tx), fields(tx_index = %tx.index, job_id = %job_id))]
async fn insert_transaction(
    &self,
    tx: CrossChainTx,
    job_id: Uuid,
) -> anyhow::Result<()> {
    // Insert main cross_chain_tx record
    let index = tx.index.clone();
    let cctx_model = CrossChainTxEntity::ActiveModel {
        id: ActiveValue::NotSet,
        creator: ActiveValue::Set(tx.creator),
        index: ActiveValue::Set(tx.index),
        lock: ActiveValue::Set(false),
        zeta_fees: ActiveValue::Set(tx.zeta_fees),
        relayed_message: ActiveValue::Set(Some(tx.relayed_message)),
        protocol_contract_version: ActiveValue::Set(ProtocolContractVersion::try_from(tx.protocol_contract_version).map_err(|e| anyhow::anyhow!(e))?),
        last_status_update_timestamp: ActiveValue::Set(Utc::now().naive_utc()),
    };

    let tx_result = CrossChainTxEntity::Entity::insert(cctx_model)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(CrossChainTxEntity::Column::Index)
                .do_nothing()
                .to_owned(),
        )
        .exec(self.db.as_ref())
        .instrument(tracing::debug_span!("inserting cctx", index = %index, job_id = %job_id))
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
        created_timestamp: ActiveValue::Set(tx.cctx_status.created_timestamp.parse::<i64>()?),
        error_message_revert: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message_revert))),
        error_message_abort: ActiveValue::Set(Some(sanitize_string(tx.cctx_status.error_message_abort))),
    };
    cctx_status::Entity::insert(status_model)
        .exec(self.db.as_ref())
        .instrument(tracing::debug_span!("inserting cctx_status", index = %index, job_id = %job_id))
        .await?;

    // Insert inbound_params
    let inbound_model = inbound_params::ActiveModel {
        id: ActiveValue::NotSet,
        cross_chain_tx_id: ActiveValue::Set(tx_id),
        sender: ActiveValue::Set(tx.inbound_params.sender),
        sender_chain_id: ActiveValue::Set(tx.inbound_params.sender_chain_id),
        tx_origin: ActiveValue::Set(tx.inbound_params.tx_origin),
        coin_type: ActiveValue::Set(CoinType::try_from(tx.inbound_params.coin_type).map_err(|e| anyhow::anyhow!(e))?),
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
        status: ActiveValue::Set(InboundStatus::try_from(tx.inbound_params.status).map_err(|e| anyhow::anyhow!(e))?),
        confirmation_mode: ActiveValue::Set(ConfirmationMode::try_from(tx.inbound_params.confirmation_mode).map_err(|e| anyhow::anyhow!(e))?),
    };
    inbound_params::Entity::insert(inbound_model)
        .exec(self.db.as_ref())
        .instrument(tracing::debug_span!("inserting inbound_params", index = %index, job_id = %job_id))
        .await?;

    // Insert outbound_params
    for outbound in tx.outbound_params {
        let outbound_model = OutboundParamsEntity::ActiveModel {
            id: ActiveValue::NotSet,
            cross_chain_tx_id: ActiveValue::Set(tx_id),
            receiver: ActiveValue::Set(outbound.receiver),
            receiver_chain_id: ActiveValue::Set(outbound.receiver_chain_id),
            coin_type: ActiveValue::Set(CoinType::try_from(outbound.coin_type).map_err(|e| anyhow::anyhow!(e))?),
            amount: ActiveValue::Set(outbound.amount),
            tss_nonce: ActiveValue::Set(outbound.tss_nonce),
            gas_limit: ActiveValue::Set(outbound.gas_limit),
            gas_price: ActiveValue::Set(Some(outbound.gas_price)),
            gas_priority_fee: ActiveValue::Set(Some(outbound.gas_priority_fee)),
            hash: ActiveValue::Set(outbound.hash),
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
            confirmation_mode: ActiveValue::Set(ConfirmationMode::try_from(outbound.confirmation_mode).map_err(|e| anyhow::anyhow!(e))?),
        };
        OutboundParamsEntity::Entity::insert(outbound_model)
            .exec(self.db.as_ref())
            .instrument(tracing::debug_span!("inserting outbound_params", index = %index, job_id = %job_id))
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
        .exec(self.db.as_ref())
        .instrument(tracing::debug_span!("inserting revert_options", index = %index, job_id = %job_id))
        .await?;

    Ok(())
}

    pub async fn get_complete_cctx(
        &self,
        index: String,
    ) -> anyhow::Result<Option<CompleteCctx>> {
        // First get the main CCTX
        let cctx = CrossChainTxEntity::Entity::find()
            .filter(CrossChainTxEntity::Column::Index.eq(&index))
            .one(self.db.as_ref())
            .await?;

        let cctx = match cctx {
            Some(cctx) => cctx,
            None => return Ok(None),
        };

        // Get all related entities
        let status = cctx_status::Entity::find()
            .filter(cctx_status::Column::CrossChainTxId.eq(cctx.id))
            .one(self.db.as_ref())
            .await?;

        let inbound = inbound_params::Entity::find()
            .filter(inbound_params::Column::CrossChainTxId.eq(cctx.id))
            .one(self.db.as_ref())
            .await?;

        let outbounds = OutboundParamsEntity::Entity::find()
            .filter(OutboundParamsEntity::Column::CrossChainTxId.eq(cctx.id))
            .all(self.db.as_ref())
            .await?;

        let revert = revert_options::Entity::find()
            .filter(revert_options::Column::CrossChainTxId.eq(cctx.id))
            .one(self.db.as_ref())
            .await?;

        // Ensure all required entities exist
        let status = status.ok_or_else(|| anyhow::anyhow!("CCTX status not found for index: {}", index))?;
        let inbound = inbound.ok_or_else(|| anyhow::anyhow!("Inbound params not found for index: {}", index))?;
        let revert = revert.ok_or_else(|| anyhow::anyhow!("Revert options not found for index: {}", index))?;

        Ok(Some(CompleteCctx {
            cctx,
            status,
            inbound,
            outbounds,
            revert,
        }))
    }

}
