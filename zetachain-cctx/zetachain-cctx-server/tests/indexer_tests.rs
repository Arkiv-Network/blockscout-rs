use std::sync::Arc;
use std::time::Duration;
mod helpers;

use blockscout_service_launcher::test_server;
use chrono::{NaiveDateTime, Utc};
use pretty_assertions::assert_eq;
use sea_orm::{ActiveValue, ColumnTrait, EntityTrait, QueryFilter};
use sea_orm::{ConnectionTrait, PaginatorTrait};
use serde_json::json;
use uuid::Uuid;
use wiremock::matchers::path_regex;
use wiremock::{
    matchers::{method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};
use zetachain_cctx_entity::cctx_status::{
    self, Column as CctxStatusColumn, Entity as CctxStatusEntity,
};
use zetachain_cctx_entity::{inbound_params, outbound_params, revert_options};
use zetachain_cctx_entity::sea_orm_active_enums::CctxStatusStatus::OutboundMined;
use zetachain_cctx_entity::sea_orm_active_enums::{
    CoinType, ProcessingStatus, ProtocolContractVersion,
};
use zetachain_cctx_entity::{cross_chain_tx, sea_orm_active_enums::Kind, watermark};

use crate::helpers::{
    dummy_cctx_response, dummy_cctx_with_pagination_response, dummy_cross_chain_tx,
    dummy_related_cctxs_response, empty_response,
};
use zetachain_cctx_logic::{
    client::{Client, RpcSettings},
    database::ZetachainCctxDatabase,
    indexer::Indexer,
    settings::IndexerSettings,
};
use zetachain_cctx_proto::blockscout::zetachain_cctx::v1::CrossChainTx;

async fn init_tests_logs() {
    blockscout_service_launcher::tracing::init_logs(
        "tests",
        &blockscout_service_launcher::tracing::TracingSettings {
            enabled: true,
            ..Default::default()
        },
        &blockscout_service_launcher::tracing::JaegerSettings::default(),
    )
    .unwrap();
}

#[tokio::test]
async fn test_historical_sync_updates_pointer() {
    //if env var TRACING=true then call init_tests_logs()
    if std::env::var("TEST_TRACING").unwrap_or_default() == "true" {
        init_tests_logs().await;
    }
    let db = crate::helpers::init_db("test", "historical_sync_updates_pointer").await;

    // Setup mock server
    let mock_server = MockServer::start().await;

    // Mock responses for different pagination scenarios
    // Mock first page response (default case)
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "MH=="))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(
                &["page_1_index_1", "page_1_index_2"],
                "SECOND_PAGE",
            ),
        ))
        .mount(&mock_server)
        .await;

    // Mock second page response when pagination.key == "SECOND_PAGE"
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "SECOND_PAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(
                &["page_2_index_1", "page_2_index_2"],
                "THIRD_PAGE",
            ),
        ))
        .mount(&mock_server)
        .await;

    // Mock third page response when pagination.key == "THIRD_PAGE"
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "THIRD_PAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(&["page_3_index_1", "page_3_index_2"], "end"),
        ))
        .mount(&mock_server)
        .await;

    // Mock third page response when pagination.key == "THIRD_PAGE"
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "end"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response()))
        .mount(&mock_server)
        .await;

    // Mock realtime fetch response (unordered=false)
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response()))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/crosschain/cctx/\d+"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_cctx_response("dummy_index", "OutboundMined")),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/inboundHashToCctxData/\d+"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(
            {"CrossChainTxs": []}
        )))
        .mount(&mock_server)
        .await;

    // Create client pointing to mock server
    let client = Client::new(RpcSettings {
        url: mock_server.uri().to_string(),
        request_per_second: 100,
        ..Default::default()
    });

    // Initialize database with historical watermark
    let db_conn = db.client();


    let watermark_model = watermark::ActiveModel {
        id: ActiveValue::NotSet,
        kind: ActiveValue::Set(Kind::Historical),
        pointer: ActiveValue::Set("MH==".to_string()), // Start from beginning
        processing_status: ActiveValue::Set(ProcessingStatus::Unlocked),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_by: ActiveValue::Set("test".to_string()),
        ..Default::default()
    };

    watermark::Entity::insert(watermark_model)
        .exec(db_conn.as_ref())
        .await
        .unwrap();

    // Create indexer
    let indexer = Indexer::new(
        IndexerSettings {
            polling_interval: 100, // Fast polling for tests
            concurrency: 10,
            ..Default::default()
        },
        db_conn.clone(),
        Arc::new(client),
        Arc::new(ZetachainCctxDatabase::new(db_conn.clone())),
    );

    // Run indexer for a short time to process historical data
    let indexer_handle = tokio::spawn(async move {
        // Run for a limited time to avoid infinite loop
        let timeout_duration = Duration::from_secs(1);
        tokio::time::timeout(timeout_duration, async {
            let _ = indexer.run().await;
        })
        .await
        .unwrap_or_else(|_| {
            // Timeout is expected as we want to stop after processing
        });
    });

    // Wait for indexer to process
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Cancel the indexer
    indexer_handle.abort();

    // Verify watermark was updated to the final pagination key
    let final_watermark = watermark::Entity::find()
        .filter(watermark::Column::Kind.eq(Kind::Historical))
        .one(db_conn.as_ref())
        .await
        .unwrap()
        .unwrap();

    println!(
        "final_watermark.pointer: {:?} ,updated_by: {:?}",
        final_watermark.pointer, final_watermark.updated_by
    );
    assert_eq!(
        final_watermark.pointer, "end",
        "watermark has not been properly updated"
    );

    // Verify that all CCTX records were inserted
    let cctx_count = cross_chain_tx::Entity::find()
        .count(db_conn.as_ref())
        .await
        .unwrap();



    // We expect 6 total CCTX records (2 from each page)
    assert_eq!(cctx_count, 6);

    // // Verify specific CCTX records exist
    for index in ["page_1_index_1", "page_1_index_2", "page_2_index_1", "page_2_index_2", "page_3_index_1", "page_3_index_2"] {
        let cctx = cross_chain_tx::Entity::find()
            .filter(cross_chain_tx::Column::Index.eq(index))
            .one(db_conn.as_ref())
            .await
            .unwrap();
        assert!(cctx.is_some());
        let cctx = cctx.unwrap();
        let cctx_id = cctx.id;
        let inbound = inbound_params::Entity::find()
            .filter(inbound_params::Column::CrossChainTxId.eq(cctx_id))
            .one(db_conn.as_ref())
            .await
            .unwrap();
        assert!(inbound.is_some());
        let outbound = outbound_params::Entity::find()
            .filter(outbound_params::Column::CrossChainTxId.eq(cctx_id))
            .one(db_conn.as_ref())
            .await
            .unwrap();
        assert!(outbound.is_some());
        let status = cctx_status::Entity::find()
            .filter(cctx_status::Column::CrossChainTxId.eq(cctx_id))
            .one(db_conn.as_ref())
            .await
            .unwrap();
        assert!(status.is_some());
        let revert = revert_options::Entity::find()
            .filter(revert_options::Column::CrossChainTxId.eq(cctx_id))
            .one(db_conn.as_ref())
            .await
            .unwrap();
        assert!(revert.is_some());
    }
}

#[tokio::test]
async fn test_status_update() {
    let db = crate::helpers::init_db("test", "indexer_status_update").await;

    if std::env::var("TEST_TRACING").unwrap_or_default() == "true" {
        init_tests_logs().await;
    }

    // Setup mock server
    let mock_server = MockServer::start().await;

    let pending_tx_index = "root_index";

    watermark::Entity::insert(watermark::ActiveModel {
        id: ActiveValue::NotSet,
        kind: ActiveValue::Set(Kind::Historical),
        pointer: ActiveValue::Set("MH==".to_string()),
        processing_status: ActiveValue::Set(ProcessingStatus::Unlocked),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_by: ActiveValue::Set("test".to_string()),
        ..Default::default()
    })
    .exec(db.client().as_ref())
    .await
    .unwrap();

    //import a single cctx
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "MH=="))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(&[pending_tx_index], "end"),
        ))
        .mount(&mock_server)
        .await;
    //supress further historical sync
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "end"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response()))
        .mount(&mock_server)
        .await;

    //supress realtime sync
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response()))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/inboundHashToCctxData/\d+"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(
            {"CrossChainTxs": []}
        )))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            format!("/crosschain/cctx/{}", pending_tx_index).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_cctx_response(pending_tx_index, "PendingOutbound")),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            format!("/crosschain/cctx/{}", pending_tx_index).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_cctx_response(pending_tx_index, "OutboundMined")),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    let client = Client::new(RpcSettings {
        url: mock_server.uri().to_string(),
        request_per_second: 100,
        ..Default::default()
    });

    let indexer = Indexer::new(
        IndexerSettings {
            polling_interval: 100, // Fast polling for tests
            concurrency: 10,
            ..Default::default()
        },
        db.client().clone(),
        Arc::new(client),
        Arc::new(ZetachainCctxDatabase::new(db.client().clone())),
    );

    let indexer_handle = tokio::spawn(async move {
        let _ = indexer.run().await;
    });

    tokio::time::sleep(Duration::from_millis(1000)).await;

    indexer_handle.abort();

    let cctx_id = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq(pending_tx_index))
        .one(db.client().as_ref())
        .await
        .unwrap()
        .unwrap()
        .id;
    let cctx_status = cctx_status::Entity::find()
        .filter(CctxStatusColumn::CrossChainTxId.eq(cctx_id))
        .one(db.client().as_ref())
        .await
        .unwrap();

    assert!(cctx_status.is_some());
    assert_eq!(cctx_status.unwrap().status, OutboundMined);
}

#[tokio::test]
async fn test_status_update_links_related() {
    if std::env::var("TEST_TRACING").unwrap_or_default() == "true" {
        init_tests_logs().await;
    }

    let db = crate::helpers::init_db("test", "indexer_status_update_links_related").await;
    let mock_server = MockServer::start().await;

    let root_index = "root_index";

    //This will make indexer import the root cctx
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "MH=="))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_cctx_with_pagination_response(&["root_index"], "end")),
        )
        .mount(&mock_server)
        .await;
    //This will just prevent historical sync from importing any more cctxs
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("pagination.key", "end"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response()))
        .mount(&mock_server)
        .await;
    //This will emulate a situation where the transaction come not in the original order
    //This will make realtime fetcher to import the child #3 before others
    //Because realtime fetcher has the highest priority
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(&["child_3_index"], "end"),
        ))
        .mount(&mock_server)
        .await;

    //This will supress errors from refreshing status
    // Mock::given(method("GET"))
    //     .and(path("/crosschain/cctx/root_index"))
    //     .respond_with(
    //         ResponseTemplate::new(200)
    //             .set_body_json(dummy_cctx_response("root_index", "PendingOutbound")),
    //     )
    //     .up_to_n_times(1)
    //     .mount(&mock_server)
    //     .await;

    Mock::given(method("GET"))
        .and(path_regex(r"/crosschain/cctx/.+"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_cctx_response("root_index", "OutboundMined")),
        )
        .mount(&mock_server)
        .await;

    let child_1_index = "child_1_index";

    //Check import a child cctx from inboundHashToCctxData
    Mock::given(method("GET"))
        .and(path(
            format!("crosschain/inboundHashToCctxData/{}", root_index).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_related_cctxs_response(&[child_1_index])),
        )
        .mount(&mock_server)
        .await;

    //Make the mock return a single child of depth 2
    let child_2_index = "child_2_index";
    Mock::given(method("GET"))
        .and(path(
            format!("crosschain/inboundHashToCctxData/{}", child_1_index).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_related_cctxs_response(&[child_2_index])),
        )
        .mount(&mock_server)
        .await;
    //This will let find out that we already have child_3_index in the database
    //And it will be updated by status update
    Mock::given(method("GET"))
        .and(path(
            format!("crosschain/inboundHashToCctxData/{}", child_2_index).as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_related_cctxs_response(&["child_3_index"])),
        )
        .mount(&mock_server)
        .await;

    //This will let indexer know that there are no further children for child_3_index
    Mock::given(method("GET"))
        .and(path("/crosschain/inboundHashToCctxData/child_3_index"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(
            {"CrossChainTxs": []}
        )))
        .mount(&mock_server)
        .await;

    //insert starting point for historical sync
    watermark::Entity::insert(watermark::ActiveModel {
        id: ActiveValue::NotSet,
        kind: ActiveValue::Set(Kind::Historical),
        pointer: ActiveValue::Set("MH==".to_string()),
        processing_status: ActiveValue::Set(ProcessingStatus::Unlocked),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_by: ActiveValue::Set("test".to_string()),
        ..Default::default()
    })
    .exec(db.client().as_ref())
    .await
    .unwrap();

    let client = Client::new(RpcSettings {
        url: mock_server.uri().to_string(),
        request_per_second: 100,
        ..Default::default()
    });

    let indexer = Indexer::new(
        IndexerSettings {
            polling_interval: 100, // Fast polling for tests
            concurrency: 10,
            retry_threshold: 3,
            ..Default::default()
        },
        db.client().clone(),
        Arc::new(client),
        Arc::new(ZetachainCctxDatabase::new(db.client().clone())),
    );

    let indexer_handle = tokio::spawn(async move {
        let _ = indexer.run().await;
    });

    tokio::time::sleep(Duration::from_millis(1000)).await;

    indexer_handle.abort();

    let root = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq(root_index))
        .one(db.client().as_ref())
        .await
        .unwrap();

    //root must be synced at this point by historical sync
    assert!(root.is_some());
    let root = root.unwrap();
    assert_eq!(root.depth, 0);
    assert_eq!(
        root.processing_status,
        ProcessingStatus::Done,
        "root cctx has not processed"
    );

    //child_1 must be synced at this point by status update
    let child_1 = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq(child_1_index))
        .one(db.client().as_ref())
        .await
        .unwrap();

    assert!(child_1.is_some());
    let child_1 = child_1.unwrap();
    assert_eq!(child_1.root_id, Some(root.id));
    assert_eq!(child_1.parent_id, Some(root.id));
    assert_eq!(child_1.depth, 1);

    //child_2 must be synced at this point by status update
    let child_2 = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq(child_2_index))
        .one(db.client().as_ref())
        .await
        .unwrap();

    assert!(child_2.is_some(), "discovered cctx was not imported");
    let child_2 = child_2.unwrap();
    assert_eq!(
        child_2.root_id,
        Some(root.id),
        "child root id has not been set"
    );
    assert_eq!(
        child_2.parent_id,
        Some(child_1.id),
        "child parent id has not been set"
    );
    assert_eq!(child_2.depth, 2, "child depth has not been set");

    let updated_child_3 = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq("child_3_index"))
        .one(db.client().as_ref())
        .await
        .unwrap();

    assert!(
        updated_child_3.is_some(),
        "grandchild cctx was not imported by realtime fetcher"
    );
    let updated_child_3 = updated_child_3.unwrap();
    assert_eq!(
        updated_child_3.root_id,
        Some(root.id),
        "grandchild root id has not been set"
    );
    assert_eq!(
        updated_child_3.parent_id,
        Some(child_2.id),
        "grandchild parent id has not been set"
    );
    assert_eq!(
        updated_child_3.depth, 3,
        "grandchild depth has not been set"
    );
}

#[tokio::test]
async fn test_lock_watermark() {
    let db = crate::helpers::init_db("test", "indexer_lock_watermark").await;
    let database = ZetachainCctxDatabase::new(db.client().clone());

    watermark::Entity::insert(watermark::ActiveModel {
        id: ActiveValue::NotSet,
        kind: ActiveValue::Set(Kind::Historical),
        pointer: ActiveValue::Set("MH==".to_string()),
        processing_status: ActiveValue::Set(ProcessingStatus::Unlocked),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_by: ActiveValue::Set("test".to_string()),
        ..Default::default()
    })
    .exec(db.client().as_ref())
    .await
    .unwrap();

    let watermark = watermark::Entity::find()
        .filter(watermark::Column::Kind.eq(Kind::Historical))
        .one(db.client().as_ref())
        .await
        .unwrap();
    database
        .lock_watermark(watermark.clone().unwrap(), Uuid::new_v4())
        .await
        .unwrap();
    database
        .unlock_watermark(watermark.clone().unwrap(), Uuid::new_v4())
        .await
        .unwrap();

    let watermark = watermark::Entity::find()
        .filter(watermark::Column::Kind.eq(Kind::Historical))
        .one(db.client().as_ref())
        .await
        .unwrap();
    assert_eq!(
        watermark.unwrap().processing_status,
        ProcessingStatus::Unlocked
    );
}

#[tokio::test]
async fn test_get_cctx_info() {
    let db = crate::helpers::init_db("test", "indexer_get_cctx_info").await;
    let last_update_timestamp =
        chrono::DateTime::<Utc>::from_timestamp(1750344684 as i64, 0).unwrap();
    let insert_statement = format!(
        r#"
    INSERT INTO cross_chain_tx (id, creator, index, zeta_fees, processing_status, relayed_message, last_status_update_timestamp, protocol_contract_version,updated_by) 
    VALUES (1, 'zeta18pksjzclks34qkqyaahf2rakss80mnusju77cm', '0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31', '0', 'unlocked'::processing_status, '', '2025-01-19 12:31:24', 'V2','test');
    INSERT INTO cctx_status (id, cross_chain_tx_id, status, status_message, error_message, last_update_timestamp, is_abort_refunded, created_timestamp, error_message_revert, error_message_abort) 
    VALUES (1, 1, 'OutboundMined', '', '', '{}', false, 1750344684, '', '');
    INSERT INTO inbound_params (id, cross_chain_tx_id, sender, sender_chain_id, tx_origin, coin_type, asset, amount, observed_hash, observed_external_height, ballot_index, finalized_zeta_height, tx_finalization_status, is_cross_chain_call, status, confirmation_mode) 
    VALUES (1, 1, 'tb1q99jmq3q5s9hzm65vg0lyk3eqht63ssw8uyzy67', '18333', 'tb1q99jmq3q5s9hzm65vg0lyk3eqht63ssw8uyzy67', 'Gas', '', '8504', 'ed64294c274f4c8204f9c8e8495495b839c59d3944bfcf51ec8ad6d3d5721e38', '257082', '0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31', '10966764', 'Executed', false, 'SUCCESS', 'SAFE');
    INSERT INTO outbound_params (id, cross_chain_tx_id, receiver, receiver_chain_id, coin_type, amount, tss_nonce, gas_limit, gas_price, gas_priority_fee, hash, ballot_index, observed_external_height, gas_used, effective_gas_price, effective_gas_limit, tss_pubkey, tx_finalization_status, call_options_gas_limit, call_options_is_arbitrary_call, confirmation_mode) 
    VALUES (1, 1, '0x33c2f2B93798629f1311cA9ade3D4BF732011718', '7001', 'Gas', '0', '0', '0', '', '', '0xcd6c1391ca9950bb527b36b21ac75bccd29e7a2adf105662cb5eadd4bda5b4d5', '', '10966764', '0', '0', '0', 'zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p', 'Executed', '0', false, 'SAFE');
    INSERT INTO revert_options (id, cross_chain_tx_id, revert_address, call_on_revert, abort_address, revert_message, revert_gas_limit) 
    VALUES (1, 1, '', false, '', NULL, '0');
    "#,
        last_update_timestamp.naive_utc().to_string()
    );
    
    println!("last update timestamp: {}", last_update_timestamp.naive_utc().to_string());

    // let insert_statement = Statement::from_string(db.client().as_ref().get_database_backend(), insert_statement);

    db.client()
        .execute_unprepared(&insert_statement)
        .await
        .unwrap();

    let database = ZetachainCctxDatabase::new(db.client().clone());

    let cctx = database
        .get_complete_cctx(
            "0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31".to_string(),
        )
        .await
        .unwrap();

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response()))
        .mount(&mock_server)
        .await;

    // Mock::given(method("GET"))
    //     .and(path_regex(r"/crosschain/cctx/.+"))
    //     .respond_with(
    //         ResponseTemplate::new(200).set_body_json(dummy_cctx_response(
    //             "0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31",
    //             "OutboundMined",
    //         )),
    //     )
    //     .mount(&mock_server)
    //     .await;

    // Mock::given(method("GET"))
    //     .and(path_regex(r"/inboundHashToCctxData/\d+"))
    //     .respond_with(ResponseTemplate::new(200).set_body_json(json!(
    //         {"CrossChainTxs": []}
    //     )))
    //     .mount(&mock_server)
    //     .await;

    assert!(cctx.is_some());
    let cctx = cctx.unwrap();
    assert_eq!(
        cctx.cctx.index,
        "0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31"
    );
    assert_eq!(
        cctx.cctx.creator,
        "zeta18pksjzclks34qkqyaahf2rakss80mnusju77cm"
    );
    assert_eq!(cctx.cctx.zeta_fees, "0");
    assert_eq!(cctx.cctx.relayed_message, Some("".to_string()));
    assert_eq!(
        cctx.cctx.protocol_contract_version,
        ProtocolContractVersion::V2
    );
    assert_eq!(
        cctx.cctx.last_status_update_timestamp,
        NaiveDateTime::parse_from_str("2025-01-19 12:31:24", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    assert_eq!(cctx.outbounds.len(), 1);
    assert_eq!(
        cctx.outbounds[0].receiver,
        "0x33c2f2B93798629f1311cA9ade3D4BF732011718"
    );
    assert_eq!(cctx.outbounds[0].receiver_chain_id, "7001");
    assert_eq!(cctx.outbounds[0].coin_type, CoinType::Gas);
    assert_eq!(cctx.outbounds[0].amount, "0");
    assert_eq!(cctx.outbounds[0].tss_nonce, "0");
    assert_eq!(cctx.outbounds[0].gas_limit, "0");

    let client = Client::new(RpcSettings {
        request_per_second: 100,
        url: mock_server.uri().to_string(),
        ..Default::default()
    });
    let base = helpers::init_zetachain_cctx_server(
        db.db_url(),
        |mut x| {
            x.indexer.concurrency = 1;
            x.indexer.polling_interval = 1000;
            x
        },
        db.client(),
        Arc::new(client),
    )
    .await;
    let response: serde_json::Value = test_server::send_get_request(&base, "/api/v1/CctxInfoService:get?cctx_id=0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31")
                .await;
    let expected_response = json!({
        "creator": "zeta18pksjzclks34qkqyaahf2rakss80mnusju77cm",
        "index": "0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31",
        "zeta_fees": "0",
        "relayed_message": "",
        "cctx_status": {
            "status": "OUTBOUND_MINED",
            "status_message": "",
            "error_message": "",
            "last_update_timestamp": "1750344684",
            "is_abort_refunded": false,
            "created_timestamp": "1750344684",
            "error_message_revert": "",
            "error_message_abort": ""
        },
        "inbound_params": {
            "sender": "tb1q99jmq3q5s9hzm65vg0lyk3eqht63ssw8uyzy67",
            "sender_chain_id": "18333",
            "tx_origin": "tb1q99jmq3q5s9hzm65vg0lyk3eqht63ssw8uyzy67",
            "coin_type": "GAS",
            "asset": "",
            "amount": "8504",
            "observed_hash": "ed64294c274f4c8204f9c8e8495495b839c59d3944bfcf51ec8ad6d3d5721e38",
            "observed_external_height": "257082",
            "ballot_index": "0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31",
            "finalized_zeta_height": "10966764",
            "tx_finalization_status": "EXECUTED",
            "is_cross_chain_call": false,
            "status": "SUCCESS",
            "confirmation_mode": "SAFE"
        },
        "outbound_params": [
            {
                "receiver": "0x33c2f2B93798629f1311cA9ade3D4BF732011718",
                "receiver_chain_id": "7001",
                "coin_type": "GAS",
                "amount": "0",
                "tss_nonce": "0",
                "gas_limit": "0",
                "gas_price": "",
                "gas_priority_fee": "",
                "hash": "0xcd6c1391ca9950bb527b36b21ac75bccd29e7a2adf105662cb5eadd4bda5b4d5",
                "ballot_index": "",
                "observed_external_height": "10966764",
                "gas_used": "0",
                "effective_gas_price": "0",
                "effective_gas_limit": "0",
                "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                "tx_finalization_status": "EXECUTED",
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
            "revert_message": "",
            "revert_gas_limit": "0"
        }
    });

    let parsed_cctx: CrossChainTx = serde_json::from_value(response).unwrap();
    let expected_cctx: CrossChainTx = serde_json::from_value(expected_response).unwrap();

    assert_eq!(parsed_cctx.index, expected_cctx.index);
    assert_eq!(parsed_cctx.creator, expected_cctx.creator);
    assert_eq!(parsed_cctx.zeta_fees, expected_cctx.zeta_fees);
    assert_eq!(parsed_cctx.relayed_message, expected_cctx.relayed_message);
    assert!(parsed_cctx.cctx_status.is_some());
    let expected_cctx_status = expected_cctx.cctx_status.unwrap();
    let parsed_cctx_status = parsed_cctx.cctx_status.unwrap();
    assert_eq!(parsed_cctx_status.status, expected_cctx_status.status);
    assert_eq!(
        parsed_cctx_status.status_message,
        expected_cctx_status.status_message
    );
    assert_eq!(
        parsed_cctx_status.error_message,
        expected_cctx_status.error_message
    );
    assert_eq!(
        parsed_cctx_status.last_update_timestamp,
        expected_cctx_status.last_update_timestamp
    );
    assert_eq!(
        parsed_cctx_status.is_abort_refunded,
        expected_cctx_status.is_abort_refunded
    );
    assert_eq!(
        parsed_cctx_status.created_timestamp,
        expected_cctx_status.created_timestamp
    );
}

#[tokio::test]
async fn test_level_data_gap() {
    if std::env::var("TEST_TRACING").unwrap_or_default() == "true" {
        init_tests_logs().await;
    }
    let db = crate::helpers::init_db("test", "indexer_level_data_gap").await;

    //simulate some previous sync progress
    // let last_update_timestamp= chrono::DateTime::<Utc>::from_timestamp(1750344684 as i64, 0).unwrap();
    let insert_statement = format!(
        r#"
    INSERT INTO cross_chain_tx (creator, index, zeta_fees, processing_status, relayed_message, last_status_update_timestamp, protocol_contract_version,updated_by) 
    VALUES ('zeta18pksjzclks34qkqyaahf2rakss80mnusju77cm', '0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31', '0', 'unlocked'::processing_status, '', '2025-01-19 12:31:24', 'V2','test');
    INSERT INTO cctx_status (cross_chain_tx_id, status, status_message, error_message, last_update_timestamp, is_abort_refunded, created_timestamp, error_message_revert, error_message_abort) 
    VALUES (1, 'OutboundMined', '', '', '2025-01-19 12:31:24', false, 1750344684, '', '');
    "#
    );

    db.client()
        .execute_unprepared(&insert_statement)
        .await
        .unwrap();

    // suppress historical sync
    watermark::Entity::insert(watermark::ActiveModel {
        id: ActiveValue::NotSet,
        kind: ActiveValue::Set(Kind::Historical),
        pointer: ActiveValue::Set("end".to_string()),
        processing_status: ActiveValue::Set(ProcessingStatus::Unlocked),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_by: ActiveValue::Set("test".to_string()),
        ..Default::default()
    })
    .exec(db.client().as_ref())
    .await
    .unwrap();

    let mock_server = MockServer::start().await;
    //suppress historical sync
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "end"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response()))
        .mount(&mock_server)
        .await;

    // simulate realtime fetcher getting some relevant data
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(
                &["page_1_index_1", "page_1_index_2"],
                "SECOND_PAGE",
            ),
        ))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .and(query_param("pagination.key", "SECOND_PAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(
                &["page_2_index_1", "page_2_index_2"],
                "THIRD_PAGE",
            ),
        ))
        .mount(&mock_server)
        .await;

    // since these cctx are absent, the indexer is exprected to follow through and request the next page

    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .and(query_param("pagination.key", "THIRD_PAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            dummy_cctx_with_pagination_response(&["page_3_index_1", "page_3_index_2"], "end"),
        ))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    //supress status update/related cctx search
    Mock::given(method("GET"))
        .and(path_regex(r"/crosschain/cctx/.+"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(
            {"CrossChainTxs": []}
        )))
        .mount(&mock_server)
        .await;
    //supress status update/related cctx search
    Mock::given(method("GET"))
        .and(path_regex(r"crosschain/inboundHashToCctxData/.+"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(dummy_cctx_response("page_3_index_1", "OutboundMined")),
        )
        .mount(&mock_server)
        .await;

    let rpc_client = Client::new(RpcSettings {
        url: mock_server.uri(),
        request_per_second: 100,
        ..Default::default()
    });

    let indexer = Indexer::new(
        IndexerSettings {
            polling_interval: 100, // Fast polling for tests
            concurrency: 10,
            ..Default::default()
        },
        db.client().clone(),
        Arc::new(rpc_client),
        Arc::new(ZetachainCctxDatabase::new(db.client().clone())),
    );

    // Run indexer for a short time to process realtime data
    let indexer_handle = tokio::spawn(async move {
        // Run for a limited time to avoid infinite loop
        let timeout_duration = Duration::from_secs(1);
        tokio::time::timeout(timeout_duration, async {
            let _ = indexer.run().await;
        })
        .await
        .unwrap_or_else(|_| {
            // Timeout is expected as we want to stop after processing
        });
    });

    tokio::time::sleep(Duration::from_millis(1000)).await;

    indexer_handle.abort();

    let indices = vec![
        "page_1_index_1",
        "page_1_index_2",
        "page_2_index_1",
        "page_2_index_2",
        "page_3_index_1",
        "page_3_index_2",
    ];

    for index in indices {
        let cctx = cross_chain_tx::Entity::find()
            .filter(cross_chain_tx::Column::Index.eq(index))
            .one(db.client().as_ref())
            .await
            .unwrap();
        assert!(cctx.is_some(), "CCTX with index {} not found", index);
        assert_eq!(cctx.unwrap().processing_status, ProcessingStatus::Unlocked);
    }

    let cctx_count = cross_chain_tx::Entity::find()
        .count(db.client().as_ref())
        .await
        .unwrap();

    // 7 cctxs are expected: 1 historical, 6 realtime
    assert_eq!(cctx_count, 7);
}
