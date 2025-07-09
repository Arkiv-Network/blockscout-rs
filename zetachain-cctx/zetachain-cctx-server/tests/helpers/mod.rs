use blockscout_service_launcher::{test_database::TestDbGuard, test_server};
use reqwest::Url;
use sea_orm::DatabaseConnection;
use std::sync::Arc;
use zetachain_cctx_logic::client::Client;
use zetachain_cctx_logic::models::{
    CallOptions, CctxStatus, CrossChainTx, InboundParams, OutboundParams, RevertOptions,
};
use zetachain_cctx_server::Settings;

#[allow(dead_code)]
pub async fn init_db(db_prefix: &str, test_name: &str) -> TestDbGuard {
    // Initialize tracing for all tests that use this helper
    let db_name = format!("{db_prefix}_{test_name}");
    TestDbGuard::new::<migration::Migrator>(db_name.as_str()).await
}

#[allow(dead_code)]
pub async fn init_zetachain_cctx_server<F>(
    db_url: String,
    settings_setup: F,
    db: Arc<DatabaseConnection>,
    client: Arc<Client>,
) -> Url
where
    F: Fn(Settings) -> Settings,
{
    // Initialize tracing for server tests
    // init_tracing();

    let (settings, base) = {
        let mut settings = Settings::default(db_url);
        let (server_settings, base) = test_server::get_test_server_settings();
        settings.server = server_settings;
        settings.metrics.enabled = false;
        settings.tracing.enabled = true;
        settings.jaeger.enabled = false;

        (settings_setup(settings), base)
    };

    test_server::init_server(|| zetachain_cctx_server::run(settings, db, client), &base).await;
    base
}

#[allow(dead_code)]
pub fn dummy_cross_chain_tx(index: &str, status: &str) -> CrossChainTx {
    CrossChainTx {
        creator: "creator".to_string(),
        index: index.to_string(),
        zeta_fees: "0".to_string(),
        relayed_message: "msg".to_string(),
        cctx_status: CctxStatus {
            status: status.to_string(),
            status_message: "".to_string(),
            error_message: "".to_string(),
            last_update_timestamp: "0".to_string(),
            is_abort_refunded: false,
            created_timestamp: "0".to_string(),
            error_message_revert: "".to_string(),
            error_message_abort: "".to_string(),
        },
        inbound_params: InboundParams {
            sender: "sender".to_string(),
            sender_chain_id: "1".to_string(),
            tx_origin: "origin".to_string(),
            coin_type: "Zeta".to_string(),
            asset: "".to_string(),
            amount: "0".to_string(),
            observed_hash: index.to_string(),
            observed_external_height: "0".to_string(),
            ballot_index: index.to_string(),
            finalized_zeta_height: "0".to_string(),
            tx_finalization_status: "NotFinalized".to_string(),
            is_cross_chain_call: false,
            status: "SUCCESS".to_string(),
            confirmation_mode: "SAFE".to_string(),
        },
        outbound_params: vec![OutboundParams {
            receiver: "receiver".to_string(),
            receiver_chain_id: "2".to_string(),
            coin_type: "Zeta".to_string(),
            amount: "1000000000000000000".to_string(),
            tss_nonce: "0".to_string(),
            gas_limit: "0".to_string(),
            gas_price: "0".to_string(),
            gas_priority_fee: "0".to_string(),
            hash: index.to_string(),
            ballot_index: "".to_string(),
            observed_external_height: "0".to_string(),
            gas_used: "0".to_string(),
            effective_gas_price: "0".to_string(),
            effective_gas_limit: "0".to_string(),
            tss_pubkey: "".to_string(),
            tx_finalization_status: "NotFinalized".to_string(),
            call_options: Some(CallOptions {
                gas_limit: "0".to_string(),
                is_arbitrary_call: false,
            }),
            confirmation_mode: "SAFE".to_string(),
        }],
        protocol_contract_version: "V1".to_string(),
        revert_options: RevertOptions {
            revert_address: "".to_string(),
            call_on_revert: false,
            abort_address: "".to_string(),
            revert_message: None,
            revert_gas_limit: "0".to_string(),
        },
    }
}

const CCTX_TEMPLATE: &str = r#"
        {
            "creator": "zeta1mte0r3jzkf2rkd7ex4p3xsd3fxqg7q29q0wxl5",
            "index": "index_placeholder",
            "zeta_fees": "0",
            "relayed_message": "",
            "cctx_status": {
                "status": "status_placeholder",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344267",
                "isAbortRefunded": false,
                "created_timestamp": "1750344267",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                "sender_chain_id": "18333",
                "tx_origin": "index_placeholder",
                "coin_type": "Gas",
                "asset": "",
                "amount": "998368",
                "observed_hash": "fb9ed1a4f8e3971543f9a598a7b47f70173f5a5f31e43eac2e2fe7911c92254b",
                "observed_external_height": "257081",
                "ballot_index": "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759",
                "finalized_zeta_height": "10966666",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "index_placeholder",
                    "receiver_chainId": "7001",
                    "coin_type": "Gas",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0", 
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "index_placeholder",
                    "ballot_index": "",
                    "observed_external_height": "10966666",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "0",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "",
                "call_on_revert": false,
                "abort_address": "",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        }
"#;

#[allow(dead_code)]
pub fn dummy_related_cctxs_response(indices: &[&str]) -> serde_json::Value {
    let cctxs = indices
        .iter()
        .map(|index| {
            CCTX_TEMPLATE.replace("index_placeholder", index).replace("status_placeholder", "PendingOutbound")
        })
        .collect::<Vec<String>>()
        .join(",");
    let template = r#"
    {
    "CrossChainTxs": 
           [
           cctxs_placeholder
           ]
    }
    "#;
    let result = template
        .replace("cctxs_placeholder", &cctxs)
        .to_string();

    serde_json::from_str::<serde_json::Value>(&result).unwrap()
}
#[allow(dead_code)]
pub fn dummy_cctx_response(index: &str, status: &str) -> serde_json::Value {
    let template = r#"
    {
    "CrossChainTx": 
           cctx_placeholder
    }
    "#;
    let result = template
        .replace("cctx_placeholder", CCTX_TEMPLATE)
        .replace("status_placeholder", status)
        .replace("index_placeholder", index)
        .to_string();
    serde_json::from_str::<serde_json::Value>(&result).unwrap()
}

#[allow(dead_code)]
pub fn dummy_cctx_with_pagination_response(indices: &[&str], next_key: &str) -> serde_json::Value {
    let cctxs = indices
        .iter()
        .map(|index| {
            CCTX_TEMPLATE.replace("index_placeholder", index).replace("status_placeholder", "PendingOutbound")
        })
        .collect::<Vec<String>>()
        .join(",");
    let template = r#"
    {
        "CrossChainTx":
        [
            cctxs_placeholder
        ],
    "pagination": {
        "next_key": "next_key_placeholder",
        "total": "0"
    }
}
"#;
    let result = template
        .replace("cctxs_placeholder", &cctxs)
        .replace("next_key_placeholder", next_key)
        .to_string();

    serde_json::from_str::<serde_json::Value>(&result).unwrap()
}

#[allow(dead_code)]
pub fn empty_response() -> serde_json::Value {
    serde_json::json!({
    "CrossChainTx": [],
    "pagination": {
        "next_key": "end",
        "total": "0"
    }
    })
}