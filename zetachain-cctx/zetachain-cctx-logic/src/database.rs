use std::sync::Arc;

use chrono::Utc;
use sea_orm::{ActiveValue, DatabaseConnection};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use zetachain_cctx_entity::{
    cross_chain_tx as CrossChainTxEntity, outbound_params as OutboundParamsEntity,
    sea_orm_active_enums::TxFinalizationStatus,
};

use crate::models::CrossChainTx;

struct Database {
    db: Arc<DatabaseConnection>,
}

impl Database {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        Self { db }
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

    async fn update_cctx_status(
        &self,
        cctx: CrossChainTxEntity::Model,
        fetched_cctx: CrossChainTx,
    ) -> anyhow::Result<()> {
        let outbound_params = fetched_cctx.outbound_params;

        for outbound in outbound_params {
            // Find the specific outbound_params record for this cross_chain_tx_id and receiver
            let existing_outbound = OutboundParamsEntity::Entity::find()
                .filter(OutboundParamsEntity::Column::CrossChainTxId.eq(cctx.id))
                .filter(OutboundParamsEntity::Column::Receiver.eq(&outbound.receiver))
                .one(self.db.as_ref())
                .await?;

            if let Some(existing_outbound) = existing_outbound {
                let outbound_id = existing_outbound.id;
                let mut active_model: OutboundParamsEntity::ActiveModel = existing_outbound.into();
                active_model.tx_finalization_status = ActiveValue::Set(
                    TxFinalizationStatus::try_from(outbound.tx_finalization_status.clone())
                        .map_err(|e| anyhow::anyhow!(e))?,
                );

                OutboundParamsEntity::Entity::update(active_model)
                    .filter(OutboundParamsEntity::Column::Id.eq(outbound_id))
                    .exec(self.db.as_ref())
                    .await?;
            }
        }

        //update CrossChainTxEntity last_updated_at
        let mut active_model: CrossChainTxEntity::ActiveModel = cctx.clone().into();
        active_model.last_status_update_timestamp = ActiveValue::Set(Utc::now().naive_utc());
        CrossChainTxEntity::Entity::update(active_model)
            .filter(CrossChainTxEntity::Column::Id.eq(cctx.id))
            .exec(self.db.as_ref())
            .await?;

        Ok(())
    }

}
