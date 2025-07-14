use crate::{
    proto::{
        health_actix::route_health, health_server::HealthServer,
        {cctx_info_actix::route_cctx_info, cctx_info_server::CctxInfoServer},
        stats_actix::route_stats, stats_server::StatsServer
    },
    services::{
        cctx::CctxService, HealthService, stats::StatsService
    },
    settings::Settings,
};
use blockscout_service_launcher::{
    launcher::{self, GracefulShutdownHandler, LaunchSettings}, tracing};


use sea_orm::DatabaseConnection;
use zetachain_cctx_logic::{client::Client, database::ZetachainCctxDatabase, indexer::Indexer};


use std::sync::Arc;

const SERVICE_NAME: &str = "zetachain_cctx";

#[derive(Clone)]
struct Router {
    // TODO: add services here
    health: Arc<HealthService>,
    cctx: Arc<CctxService>,
    stats: Arc<StatsService>,
}

impl Router {
    pub fn grpc_router(&self) -> tonic::transport::server::Router {
        tonic::transport::Server::builder()
            .add_service(HealthServer::from_arc(self.health.clone()))
            .add_service(CctxInfoServer::from_arc(self.cctx.clone()))
    }
}

impl launcher::HttpRouter for Router {
    fn register_routes(&self, service_config: &mut actix_web::web::ServiceConfig) {
        service_config.configure(|config| route_health(config, self.health.clone()));
        service_config.configure(|config| route_cctx_info(config, self.cctx.clone()));
        service_config.configure(|config| route_stats(config, self.stats.clone()));
    }
}

pub async fn run(settings: Settings, db: Arc<DatabaseConnection>, client: Arc<Client>) -> Result<(), anyhow::Error> {
    tracing::init_logs(SERVICE_NAME, &settings.tracing, &settings.jaeger)?;

    let database = Arc::new(ZetachainCctxDatabase::new(db.clone()));
    let health = Arc::new(HealthService::default());
    let cctx = Arc::new(CctxService::new(database.clone()));
    let stats = Arc::new(StatsService::new(database.clone()));
    if settings.indexer.enabled {
        let indexer = Indexer::new(settings.indexer,  client, database);
        tokio::spawn(async move {
            //TODO: handle error, log it and restart the indexer
            let _ = indexer.run().await;
        });
    }

    let router = Router {
        cctx,   
        health,
        stats,
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
