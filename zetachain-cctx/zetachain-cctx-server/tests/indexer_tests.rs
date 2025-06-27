use std::sync::Arc;
use std::time::Duration;
mod data;
mod helpers;
use pretty_assertions::assert_eq;
use sea_orm::{ActiveValue, QueryFilter, EntityTrait, ColumnTrait};
use wiremock::{
    matchers::{method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};
use zetachain_cctx_entity::{
    cross_chain_tx, watermark, sea_orm_active_enums::WatermarkType,
};
use zetachain_cctx_logic::{
    client::{Client, RpcSettings},
    indexer::{lock_watermark, unlock_watermark, Indexer},
    settings::IndexerSettings,
};
use crate::data::{FIRST_PAGE_RESPONSE, SECOND_PAGE_RESPONSE, THIRD_PAGE_RESPONSE, PENDING_TX_RESPONSE,FINALIZED_TX_RESPONSE};
use sea_orm::PaginatorTrait;

#[tokio::test]
async fn test_historical_sync_updates_pointer() {
    let db = crate::helpers::init_db("test", "historical_sync_updates_pointer").await;
    
    // Setup mock server
    let mock_server = MockServer::start().await;
    
    // Mock responses for different pagination scenarios
    setup_historical_mock_responses(&mock_server).await;
    
    // Create client pointing to mock server
    let client = Client::new(RpcSettings {
        url: mock_server.uri().to_string(),
        ..Default::default()
    });
    
    // Initialize database with historical watermark
    let db_conn = db.client();

    cross_chain_tx::Entity::delete_many()
        .exec(db_conn.as_ref())
        .await
        .unwrap();

    let watermark_model = watermark::ActiveModel {
        id: ActiveValue::NotSet,
        watermark_type: ActiveValue::Set(WatermarkType::Historical),
        pointer: ActiveValue::Set("MH==".to_string()), // Start from beginning
        lock: ActiveValue::Set(false),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    };
    
    watermark::Entity::insert(watermark_model)
        .exec(db_conn.as_ref())
        .await
        .unwrap();
    
    // Create indexer
    let indexer = Indexer::new(
        IndexerSettings {
            polling_interval: 100, // Fast polling for tests
            concurrency: 1,
            ..Default::default()
        },
        db_conn.clone(),
        Arc::new(client),
    );
    
    // Run indexer for a short time to process historical data
    let indexer_handle = tokio::spawn(async move {
        // Run for a limited time to avoid infinite loop
        let timeout_duration = Duration::from_secs(5);
        tokio::time::timeout(timeout_duration, async {
            indexer.run().await;
        })
        .await
        .unwrap_or_else(|_| {
            // Timeout is expected as we want to stop after processing
        });
    });
    
    // Wait for indexer to process
    tokio::time::sleep(Duration::from_millis(5000)).await;
    
    // Cancel the indexer
    indexer_handle.abort();
    
    // Verify watermark was updated to the final pagination key
    let final_watermark = watermark::Entity::find()
        .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
        .one(db_conn.as_ref())
        .await
        .unwrap()
        .unwrap();
    
    assert_eq!(final_watermark.pointer, "////////22c=");
    assert_eq!(final_watermark.lock, false);
    
    // Verify that all CCTX records were inserted
    let cctx_count = cross_chain_tx::Entity::find()
        .count(db_conn.as_ref())
        .await
        .unwrap();
    
    // We expect 9 total CCTX records (3 from each page)
    assert_eq!(cctx_count, 9);
    
    // // Verify specific CCTX records exist
    let first_page_cctx = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq("0x36b9bb2f1d745b41d8e64eb203752c02abe6f50c5e932563799d5cbf160f5117"))
        .one(db_conn.as_ref())
        .await
        .unwrap();
    assert!(first_page_cctx.is_some());
    
    let second_page_cctx = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq("0xff18366b92db12f7204eca924bc8ffbf93f655c7a35b68ec38b38d61d49fbaeb"))
        .one(db_conn.as_ref())
        .await
        .unwrap();
    assert!(second_page_cctx.is_some());
    
    let third_page_cctx = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq("0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31"))
        .one(db_conn.as_ref())
        .await
        .unwrap();
    assert!(third_page_cctx.is_some());
}


async fn test_status_update() {
    let db = crate::helpers::init_db("test", "indexer_status_update").await;
    
    // Setup mock server
    let mock_server = MockServer::start().await;

    setup_status_update_mock_responses(&mock_server).await;
    
}

#[tokio::test]
async fn test_parse_historical_data() {
    let db = crate::helpers::init_db("test", "indexer_parse_historical_data").await;
    let mock_server = MockServer::start().await;
    let mock_response = data::historic_response_from_file();
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "MH=="))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::from_str::<serde_json::Value>(&mock_response).unwrap()
        ))
        .mount(&mock_server)
        .await;

    let client = Client::new(RpcSettings {
        url: mock_server.uri().to_string(),
        ..Default::default()
    });

    
    //delete historical watermark
    watermark::Entity::delete_many()
        .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
        .exec(db.client().as_ref())
        .await
        .unwrap();

    cross_chain_tx::Entity::delete_many()
        .exec(db.client().as_ref())
        .await
        .unwrap();
    let indexer = Indexer::new(
        IndexerSettings {
            polling_interval: 100, // Fast polling for tests
            concurrency: 1,
            ..Default::default()
        },
        db.client().clone(),
        Arc::new(client),
    );
    
    // Run indexer for a short time to process historical data
    let indexer_handle = tokio::spawn(async move {
        // Run for a limited time to avoid infinite loop
        let timeout_duration = Duration::from_secs(5);
        tokio::time::timeout(timeout_duration, async {
            indexer.run().await;
        })
        .await
        .unwrap_or_else(|_| {
            // Timeout is expected as we want to stop after processing
        });
    });
    
    // Wait for indexer to process
    tokio::time::sleep(Duration::from_millis(2000)).await;
    
    // Cancel the indexer
    indexer_handle.abort();

    let cctx_count = cross_chain_tx::Entity::find()
        .count(db.client().as_ref())
        .await
        .unwrap();

    assert_eq!(cctx_count, 100);

        let first_page_cctx = cross_chain_tx::Entity::find()
            .filter(cross_chain_tx::Column::Index.eq("0x003d98f1eaa33775e97aef378d6d0dea466187239e90b3ecb0f0fca96f8facbf"))
            .one(db.client().as_ref())
            .await
            .unwrap();

    assert!(first_page_cctx.is_some());

    let second_page_cctx = cross_chain_tx::Entity::find()
        .filter(cross_chain_tx::Column::Index.eq("0x003d98f1eaa33775e97aef378d6d0dea466187239e90b3ecb0f0fca96f8facbf"))
        .one(db.client().as_ref())
        .await
        .unwrap();
    assert!(second_page_cctx.is_some());
    
}

#[tokio::test]
async fn test_lock_watermark() {
    let db = crate::helpers::init_db("test", "indexer_lock_watermark").await;

    watermark::Entity::insert(watermark::ActiveModel {
        id: ActiveValue::NotSet,
        watermark_type: ActiveValue::Set(WatermarkType::Historical),
        pointer: ActiveValue::Set("MH==".to_string()),
        lock: ActiveValue::Set(false),
        created_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
    }).exec(db.client().as_ref()).await.unwrap();

    let watermark = watermark::Entity::find()
        .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
        .one(db.client().as_ref())
        .await
        .unwrap();
    lock_watermark(db.client().as_ref(), watermark.clone().unwrap()).await.unwrap();
    unlock_watermark(db.client().as_ref(), watermark.clone().unwrap()).await.unwrap();

    let watermark = watermark::Entity::find()
        .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
        .one(db.client().as_ref())
        .await
        .unwrap();
    assert_eq!(watermark.unwrap().lock, false);
}
async fn setup_status_update_mock_responses(mock_server: &MockServer) {

    let pending_tx_index = "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759";
    Mock::given(method("GET"))
        .and(path(format!("/crosschain/cctx/{}", pending_tx_index).as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::from_str::<serde_json::Value>(PENDING_TX_RESPONSE).unwrap()
        ))
        .mount(mock_server)
        .await;
}
async fn setup_historical_mock_responses(mock_server: &MockServer) {
    
    // Mock first page response (default case)
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "MH=="))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::from_str::<serde_json::Value>(FIRST_PAGE_RESPONSE).unwrap()
        ))
        .mount(mock_server)
        .await;
    
    // Mock second page response when pagination.key == "SECOND_PAGE"
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "SECOND_PAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::from_str::<serde_json::Value>(SECOND_PAGE_RESPONSE).unwrap()
        ))
        .mount(mock_server)
        .await;
    
    // Mock third page response when pagination.key == "THIRD_PAGE"
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "true"))
        .and(query_param("pagination.key", "THIRD_PAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::from_str::<serde_json::Value>(THIRD_PAGE_RESPONSE).unwrap()
        ))
        .mount(mock_server)
        .await;

    let empty_response = serde_json::json!({
        "CrossChainTx": [],
        "pagination": {
            "next_key": "////////22c=",
            "total": "0"
        }
    });

     // Mock third page response when pagination.key == "THIRD_PAGE"
     Mock::given(method("GET"))
     .and(path("/crosschain/cctx"))
     .and(query_param("unordered", "true"))
     .and(query_param("pagination.key", "////////22c="))
     .respond_with(ResponseTemplate::new(200).set_body_json(
         serde_json::from_str::<serde_json::Value>(&empty_response.to_string()).unwrap()
     ))
     .mount(mock_server)
     .await;

    // Mock realtime fetch response (unordered=false)
    Mock::given(method("GET"))
        .and(path("/crosschain/cctx"))
        .and(query_param("unordered", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_response))
        .mount(mock_server)
        .await;
} 