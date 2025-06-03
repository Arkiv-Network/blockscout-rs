use std::sync::Arc;

use blockscout_service_launcher::{
    test_database::TestDbGuard,
    test_server
};
use reqwest::Url;
use sea_orm::DatabaseConnection;
use zetachain_cctx_logic::client::Client;
use zetachain_cctx_server::Settings;

pub async fn init_db(db_prefix: &str, test_name: &str) -> TestDbGuard {
    let db_name = format!("{db_prefix}_{test_name}");
    TestDbGuard::new::<migration::Migrator>(db_name.as_str()).await
}
pub async fn init_zetachain_cctx_server<F>(
    db_url: String,
    settings_setup: F,
    db: Arc<DatabaseConnection>,
    client: Arc<Client>,
) -> Url
where
    F: Fn(Settings) -> Settings,
{
    let (settings, base) = {
        let mut settings = Settings::default(
            db_url
            );
        let (server_settings, base) = test_server::get_test_server_settings();
        settings.server = server_settings;
        settings.metrics.enabled = false;
        settings.tracing.enabled = false;
        settings.jaeger.enabled = false;

        (settings_setup(settings), base)
    };

    test_server::init_server(|| zetachain_cctx_server::run(settings, db, client), &base).await;
    base
}