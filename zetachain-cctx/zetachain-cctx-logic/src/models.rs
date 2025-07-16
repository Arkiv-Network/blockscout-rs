use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use zetachain_cctx_entity::{cctx_status, cross_chain_tx, inbound_params, outbound_params, revert_options, sea_orm_active_enums::CoinType as DbCoinType};

#[derive(Debug, Serialize, Deserialize)]
pub struct PagedCCTXResponse {
    #[serde(rename = "CrossChainTx")]
    pub cross_chain_tx: Vec<CrossChainTx>,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CCTXResponse {
    #[serde(rename = "CrossChainTx")]
    pub cross_chain_tx: CrossChainTx,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CoinType {
    Zeta,
    Gas,
    ERC20,
    Cmd,
    NoAssetCall,
}

impl TryFrom<CoinType> for DbCoinType {
    type Error = String;
    fn try_from(value: CoinType) -> Result<Self, Self::Error> {
        match value {
            CoinType::Zeta => Ok(DbCoinType::Zeta),
            CoinType::Gas => Ok(DbCoinType::Gas),
            CoinType::ERC20 => Ok(DbCoinType::Erc20),
            CoinType::Cmd => Ok(DbCoinType::Cmd),
            CoinType::NoAssetCall => Ok(DbCoinType::NoAssetCall),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InboundHashToCctxResponse {
    #[serde(rename = "CrossChainTxs")]
    pub cross_chain_txs: Vec<CrossChainTx>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CctxShort {
    pub id: i32,
    pub index: String,
    pub root_id: Option<i32>,
    pub depth: i32,
    pub retries_number: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pagination {
    pub next_key: Option<String>,
    pub total: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CctxStatus {
    pub status: String,
    pub status_message: String,
    pub error_message: String,
    pub last_update_timestamp: String,
    pub is_abort_refunded: bool,
    pub created_timestamp: String,
    pub error_message_revert: String,
    pub error_message_abort: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InboundParams {
    pub sender: String,
    pub sender_chain_id: String,
    pub tx_origin: String,
    pub coin_type: CoinType,
    pub asset: String,
    pub amount: String,
    pub observed_hash: String,
    pub observed_external_height: String,
    pub ballot_index: String,
    pub finalized_zeta_height: String,
    pub tx_finalization_status: String,
    pub is_cross_chain_call: bool,
    pub status: String,
    pub confirmation_mode: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CallOptions {
    pub gas_limit: String,
    pub is_arbitrary_call: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutboundParams {
    pub receiver: String,
    pub receiver_chain_id: String,
    pub coin_type: CoinType,
    pub amount: String,
    pub tss_nonce: String,
    pub gas_limit: String,
    pub gas_price: String,
    pub gas_priority_fee: String,
    pub hash: String,
    pub ballot_index: String,
    pub observed_external_height: String,
    pub gas_used: String,
    pub effective_gas_price: String,
    pub effective_gas_limit: String,
    pub tss_pubkey: String,
    pub tx_finalization_status: String,
    pub call_options: Option<CallOptions>,
    pub confirmation_mode: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RevertOptions {
    pub revert_address: String,
    pub call_on_revert: bool,
    pub abort_address: String,
    pub revert_message: Option<String>,
    pub revert_gas_limit: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CrossChainTx {
    pub creator: String,
    pub index: String,
    pub zeta_fees: String,
    pub relayed_message: String,
    pub cctx_status: CctxStatus,
    pub inbound_params: InboundParams,
    pub outbound_params: Vec<OutboundParams>,
    pub revert_options: RevertOptions,
    pub protocol_contract_version: String,
}

#[derive(Debug)]
pub struct CompleteCctx {
    pub cctx: cross_chain_tx::Model,
    pub status: cctx_status::Model,
    pub inbound: inbound_params::Model,
    pub outbounds: Vec<outbound_params::Model>,
    pub revert: revert_options::Model,
    pub related: Vec<RelatedCctx>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelatedCctx {
    pub index: String,
    pub depth: i32,
    pub source_chain_id: String,
    pub status: String,
    pub inbound_amount: String,
    pub inbound_coin_type: String,
    pub inbound_asset: Option<String>,
    pub outbound_params: Vec<RelatedOutboundParams>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CctxListItem {
    pub index: String,
    pub status: String,
    pub amount: String,
    pub last_update_timestamp: NaiveDateTime,
    pub source_chain_id: String,
    pub target_chain_id: String,
    pub sender_address: String,
    pub receiver_address: String,
    pub asset: String,
    pub coin_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncProgress {
    pub historical_watermark_timestamp: NaiveDateTime,
    pub pending_status_updates_count: i64,
    pub pending_cctxs_count: i64,
    pub realtime_gaps_count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelatedOutboundParams {
    pub amount: String,
    pub chain_id: String,
    pub coin_type: String,
}

#[derive(Debug)]
pub struct CctxWithStatus {
    pub id: i32,
    pub index: String,
    pub root_id: Option<i32>,
    pub depth: i32,
    pub retries_number: i32,
}

// Token models for external service response
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Token {
    pub zrc20_contract_address: String,
    pub asset: String,
    pub foreign_chain_id: String,
    pub decimals: i32,
    pub name: String,
    pub symbol: String,
    pub coin_type: String,
    pub gas_limit: String,
    pub paused: bool,
    pub liquidity_cap: String,
}

impl TryFrom<Token> for zetachain_cctx_entity::token::ActiveModel {
    type Error = anyhow::Error;
    
    fn try_from(token: Token) -> Result<Self, Self::Error> {
        use zetachain_cctx_entity::token;
        use sea_orm::ActiveValue;
        
        let coin_type = match token.coin_type.as_str() {
            "Zeta" => DbCoinType::Zeta,
            "Gas" => DbCoinType::Gas,
            "ERC20" | "Erc20" => DbCoinType::Erc20,
            "Cmd" => DbCoinType::Cmd,
            "NoAssetCall" => DbCoinType::NoAssetCall,
            _ => return Err(anyhow::anyhow!("Invalid coin type: {}", token.coin_type)),
        };
        
        Ok(token::ActiveModel {
            id: ActiveValue::NotSet,
            zrc20_contract_address: ActiveValue::Set(token.zrc20_contract_address),
            asset: ActiveValue::Set(token.asset),
            foreign_chain_id: ActiveValue::Set(token.foreign_chain_id),
            decimals: ActiveValue::Set(token.decimals),
            name: ActiveValue::Set(token.name),
            symbol: ActiveValue::Set(token.symbol),
            coin_type: ActiveValue::Set(coin_type),
            gas_limit: ActiveValue::Set(token.gas_limit),
            paused: ActiveValue::Set(token.paused),
            liquidity_cap: ActiveValue::Set(token.liquidity_cap),
            created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
            updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PagedTokenResponse {
    #[serde(rename = "foreignCoins")]
    pub foreign_coins: Vec<Token>,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    pub foreign_chain_id: String,
    pub decimals: i32,
    pub name: String,
    pub symbol: String,
}