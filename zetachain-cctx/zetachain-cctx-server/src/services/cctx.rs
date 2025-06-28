use std::sync::Arc;

use tonic::{Request, Response, Status};

use zetachain_cctx_logic::database::ZetachainCctxDatabase;
use zetachain_cctx_proto::blockscout::zetachain_cctx::v1::{
    cctx_info_service_server::CctxInfoService, CallOptions, CrossChainTx, GetCctxInfoRequest, GetCctxInfoResponse, InboundParams, OutboundParams, RevertOptions, Status as CCTXStatus
};




pub struct CctxService {
    database: Arc<ZetachainCctxDatabase>,
}

impl CctxService {
    pub fn new(database: Arc<ZetachainCctxDatabase>) -> Self {
        Self { database }
    }
}

#[async_trait::async_trait]
impl CctxInfoService for CctxService {
    async fn get_cctx_info(&self, request: Request<GetCctxInfoRequest>) -> Result<Response<GetCctxInfoResponse>, Status> {

        let request = request.into_inner();

        let complete_cctx = self
        .database
        .get_complete_cctx(request.cctx_id)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        
        let cctx= complete_cctx 
        .map(|entity|
            CrossChainTx {
                creator: entity.cctx.creator,
                index: entity.cctx.index,
                zeta_fees: entity.cctx.zeta_fees,
                relayed_message: entity.cctx.relayed_message.unwrap_or_default(),
                cctx_status: Some(CCTXStatus {
                    status: entity.status.status.into(),
                    status_message: entity.status.status_message.unwrap_or_default(),
                    error_message: entity.status.error_message.unwrap_or_default(),
                    last_update_timestamp: entity.status.last_update_timestamp.and_utc().timestamp(),
                    is_abort_refunded: entity.status.is_abort_refunded,
                    created_timestamp: entity.status.created_timestamp,
                    error_message_abort: entity.status.error_message_abort.unwrap_or_default(),
                    error_message_revert: entity.status.error_message_revert.unwrap_or_default(),
                }),
                inbound_params: Some(InboundParams {
                    sender: entity.inbound.sender,
                    sender_chain_id: entity.inbound.sender_chain_id.parse().unwrap_or(0),
                    tx_origin: entity.inbound.tx_origin,
                    coin_type: entity.inbound.coin_type.into(),
                    asset: entity.inbound.asset.unwrap_or_default(),
                    amount: entity.inbound.amount,
                    observed_hash: entity.inbound.observed_hash,
                    observed_external_height: entity.inbound.observed_external_height.parse().unwrap_or(0),
                    ballot_index: entity.inbound.ballot_index,
                    finalized_zeta_height: entity.inbound.finalized_zeta_height.parse().unwrap_or(0),
                    tx_finalization_status: entity.inbound.tx_finalization_status.into(),
                    is_cross_chain_call: entity.inbound.is_cross_chain_call,
                    status: entity.inbound.status.into(),   
                    confirmation_mode: entity.inbound.confirmation_mode.into(),
                }),
                outbound_params: entity.outbounds.into_iter().map(|o| OutboundParams {
                    receiver: o.receiver,
                    receiver_chain_id: o.receiver_chain_id.parse().unwrap_or(0),
                    coin_type: o.coin_type.into(),
                    amount: o.amount,
                    tss_nonce: o.tss_nonce.parse().unwrap_or(0),
                    gas_limit: o.gas_limit.parse().unwrap_or(0),
                    gas_price: o.gas_price.unwrap_or_default(),
                    gas_priority_fee: o.gas_priority_fee.unwrap_or_default(),
                    hash: o.hash,
                    ballot_index: o.ballot_index.unwrap_or_default(),
                    observed_external_height: o.observed_external_height.parse().unwrap_or(0),
                    gas_used: o.gas_used.parse().unwrap_or(0),
                    effective_gas_price: o.effective_gas_price,
                    effective_gas_limit: o.effective_gas_limit.parse().unwrap_or(0),
                    tss_pubkey: o.tss_pubkey,
                    tx_finalization_status: o.tx_finalization_status.into(),
                    call_options: Some(CallOptions {
                        gas_limit: o.call_options_gas_limit.unwrap_or("0".to_string()).parse().unwrap(),
                        is_arbitrary_call: o.call_options_is_arbitrary_call.unwrap_or_default(),
                    }),
                    confirmation_mode: o.confirmation_mode.into(),
                }).collect(),
                protocol_contract_version: entity.cctx.protocol_contract_version.into(),
                revert_options: Some(RevertOptions{
                    revert_address: entity.revert.revert_address.unwrap_or_default(),
                    call_on_revert: entity.revert.call_on_revert,
                    abort_address: entity.revert.abort_address.unwrap_or_default(),
                    revert_message: entity.revert.revert_message.unwrap_or_default().as_bytes().to_vec().into(),
                    revert_gas_limit: entity.revert.revert_gas_limit,
                }),
            }
        );

        
        Ok(Response::new(GetCctxInfoResponse {
            cctx,
        }))
    }
}






