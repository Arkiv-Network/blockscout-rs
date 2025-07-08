mod helpers;
use blockscout_service_launcher::test_server;
use uuid::Uuid;
use std::sync::Arc;
use zetachain_cctx_logic::{client::{Client, RpcSettings}, database::ZetachainCctxDatabase, models::CrossChainTx};
use sea_orm::TransactionTrait;
// use crate::helpers::{init_db, init_zetachain_cctx_server};

#[tokio::test]
#[ignore = "Needs database to run"]
async fn test_list_cctxs_endpoint() {
    let db = crate::helpers::init_db("test", "list_cctxs").await;
    let db_url = db.db_url();

    let client = Client::new(RpcSettings::default());
    let base = crate::helpers::init_zetachain_cctx_server(
        db_url,
        |mut x| {
            x.indexer.enabled = false;
            x
        },
        db.client(),
        Arc::new(client),
    )
    .await;

    let dummy_cctxs: Vec<CrossChainTx> = vec!["1"]
        .iter()
        .map(|x| 
            crate::helpers::dummy_cross_chain_tx(x, "PendingInbound")
        )
        
        .collect();

    let database = ZetachainCctxDatabase::new(db.client());

    let tx = db.client().begin().await.unwrap();
    database.batch_insert_transactions(Uuid::new_v4(), &dummy_cctxs, &tx).await.unwrap();
    tx.commit().await.unwrap();

    // Test the ListCctxs endpoint
    let response: serde_json::Value =
        test_server::send_get_request(&base, "/api/v1/CctxInfoService:list?limit=10&offset=0")
            .await;

    // The response should be a valid JSON object with a "cctxs" array
    assert!(response.is_object());
    assert!(response.get("cctxs").is_some());
    assert!(response.get("cctxs").unwrap().is_array());
}

#[tokio::test]
#[ignore = "Needs database to run"]
async fn test_list_cctxs_with_status_filter() {
    let db = crate::helpers::init_db("test", "list_cctxs_with_status").await;
    let db_url = db.db_url();

    let client = Client::new(RpcSettings::default());
    let base =
        crate::helpers::init_zetachain_cctx_server(db_url, |x| x, db.client(), Arc::new(client))
            .await;

    // Test the ListCctxs endpoint with status filter
    let response: serde_json::Value = test_server::send_get_request(
        &base,
        "/api/v1/CctxInfoService:list?limit=10&offset=0&status=PendingInbound",
    )
    .await;

    // The response should be a valid JSON object with a "cctxs" array
    assert!(response.is_object());
    assert!(response.get("cctxs").is_some());
    assert!(response.get("cctxs").unwrap().is_array());
}
