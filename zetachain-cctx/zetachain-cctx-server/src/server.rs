use crate::{
    proto::{
        health_actix::route_health, health_server::HealthServer,
    },
    services::{
        cctx::CctxService, HealthService
    },
    settings::Settings,
};
use blockscout_service_launcher::{
    launcher::{self, GracefulShutdownHandler, LaunchSettings}, tracing};

use sea_orm::DatabaseConnection;
use zetachain_cctx_logic::{client::Client, indexer::Indexer};
use zetachain_cctx_proto::blockscout::zetachain_cctx::v1::cctx_info_service_actix::route_cctx_info_service;

use std::sync::Arc;

const SERVICE_NAME: &str = "zetachain_cctx";

#[derive(Clone)]
struct Router {
    // TODO: add services here
    health: Arc<HealthService>,
    cctx: Arc<CctxService>,
}

impl Router {
    pub fn grpc_router(&self) -> tonic::transport::server::Router {
        tonic::transport::Server::builder()
            .add_service(HealthServer::from_arc(self.health.clone()))
    }
}

impl launcher::HttpRouter for Router {
    fn register_routes(&self, service_config: &mut actix_web::web::ServiceConfig) {
        service_config.configure(|config| route_health(config, self.health.clone()));
        service_config.configure(|config| route_cctx_info_service(config, self.cctx.clone()));
    }
}

pub async fn run(settings: Settings, db: Arc<DatabaseConnection>, client: Arc<Client>) -> Result<(), anyhow::Error> {
    tracing::init_logs(SERVICE_NAME, &settings.tracing, &settings.jaeger)?;

    let health = Arc::new(HealthService::default());
    let cctx = Arc::new(CctxService::new(db.clone()));
    let indexer = Indexer::new(settings.indexer, db, client);

    indexer.run().await;

    let router = Router {
        cctx,
        health,
    };

    let grpc_router = router.grpc_router();
    let http_router = router;

    let launch_settings = LaunchSettings {
        service_name: SERVICE_NAME.to_string(),
        server: settings.server,
        metrics: settings.metrics,
        graceful_shutdown: GracefulShutdownHandler::default(),
    };

    launcher::launch(launch_settings, http_router, grpc_router).await
}
