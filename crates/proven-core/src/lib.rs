mod attestation;
mod error;
mod http;
mod ws;

use attestation::create_attestation_handlers;
pub use error::{Error, Result};
use http::HttpsServer;
use ws::create_websocket_handler;

use std::net::{Ipv4Addr, SocketAddr};

use axum::routing::get;
use axum::Router;
use proven_sessions::SessionManagement;
use proven_store::Store;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct NewCoreArguments<SM: SessionManagement + 'static, CS: Store + 'static> {
    pub cert_store: CS,
    pub ip: Ipv4Addr,
    pub email: Vec<String>,
    pub fqdn: String,
    pub https_port: u16,
    pub production: bool,
    pub session_manager: SM,
    pub shutdown_token: CancellationToken,
}

pub struct Core<SM: SessionManagement + 'static, CS: Store + 'static> {
    cert_store: CS,
    ip: Ipv4Addr,
    email: Vec<String>,
    fqdn: String,
    https_port: u16,
    production: bool,
    session_manager: SM,
    shutdown_token: CancellationToken,
}

impl<SM: SessionManagement + 'static, CS: Store + 'static> Core<SM, CS> {
    pub fn new(args: NewCoreArguments<SM, CS>) -> Self {
        Self {
            cert_store: args.cert_store,
            ip: args.ip,
            email: args.email,
            fqdn: args.fqdn,
            https_port: args.https_port,
            production: args.production,
            session_manager: args.session_manager,
            shutdown_token: args.shutdown_token,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let session_handlers = create_attestation_handlers(self.session_manager.clone()).await;
        let websocket_handler = create_websocket_handler(self.session_manager.clone()).await;

        let https_app = Router::new()
            .route("/", get(|| async { "Hello Tls!" }))
            .nest("/", session_handlers)
            .nest("/", websocket_handler);

        let https_server = HttpsServer::new(
            SocketAddr::from((self.ip, self.https_port)),
            self.fqdn.clone(),
            self.email.clone(),
            !self.production,
            self.cert_store.clone(),
        );

        let https_handle = https_server.start(https_app)?;

        tokio::select! {
            _ = self.shutdown_token.cancelled() => {
                info!("shutdown command received");
            }
            e = https_handle => {
                error!("https server exited: {:?}", e);
            }
        }

        https_server.shutdown().await;

        Ok(())
    }
}
