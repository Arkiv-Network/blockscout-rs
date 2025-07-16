use crate::{
    proto::{
        health_actix::route_health, health_server::HealthServer,
        {cctx_info_actix::route_cctx_info, cctx_info_server::CctxInfoServer},
        stats_actix::route_stats
    },
    services::{
        cctx::CctxService, HealthService, stats::StatsService
    },
    settings::Settings,
};
use actix_web::{web, HttpRequest, Responder};
use blockscout_service_launcher::{
    launcher::{self, GracefulShutdownHandler, LaunchSettings}, tracing};

    use actix_web_actors::ws;
use sea_orm::DatabaseConnection;
use zetachain_cctx_logic::{client::Client, database::ZetachainCctxDatabase, indexer::Indexer};
use zetachain_cctx_proto::blockscout::zetachain_cctx::v1::{cctx_info_server::CctxInfo, stats_server::StatsServer};

use actix::ActorContext;
use std::sync::Arc;

const SERVICE_NAME: &str = "zetachain_cctx";

#[derive(Clone)]
struct Router {
    health: Arc<HealthService>,
    cctx: Arc<CctxService>,
    stats: Arc<StatsService>,
}
struct MyWebSocket;

use actix::{Actor, StreamHandler, AsyncContext};


impl Actor for MyWebSocket {
    type Context = ws::WebsocketContext<Self>;

    // Optional: heartbeat or setup logic
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.text("WebSocket connection established");
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MyWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                // Echo or handle your own protocol
                ctx.text(format!("Echo: {}", text));
            },
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            },
            _ => (),
        }
    }
}
async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
) -> impl Responder {
    ws::start(MyWebSocket {}, &req, stream)
}




pub fn route_ws(
    config: &mut ::actix_web::web::ServiceConfig,
    service: Arc<dyn CctxInfo + Send + Sync + 'static>,
) {
    config.app_data(::actix_web::web::Data::from(service));
    config.route("/ws/", ::actix_web::web::get().to(ws_handler));
}

impl Router {
    pub fn grpc_router(&self) -> tonic::transport::server::Router {
        tonic::transport::Server::builder()
            .add_service(HealthServer::from_arc(self.health.clone()))
            .add_service(CctxInfoServer::from_arc(self.cctx.clone()))
            .add_service(StatsServer::from_arc(self.stats.clone()))
    }
}

impl launcher::HttpRouter for Router {
    fn register_routes(&self, service_config: &mut actix_web::web::ServiceConfig) {
        service_config.configure(|config| route_health(config, self.health.clone()));
        service_config.configure(|config| route_cctx_info(config, self.cctx.clone()));
        service_config.configure(|config| route_stats(config, self.stats.clone()));
        service_config.configure(|config| route_ws(config, self.cctx.clone()));
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
