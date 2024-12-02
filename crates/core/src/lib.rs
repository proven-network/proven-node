//! Core logic for the Proven node and the entrypoint for all user interactions.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

mod error;
mod rpc;
mod sessions;

pub use error::{Error, Result};
use sessions::create_session_router;

use std::sync::Arc;

use axum::response::Response;
use axum::routing::get;
use axum::Router;
use proven_applications::ApplicationManagement;
use proven_http::HttpServer;
use proven_runtime::{Pool, PoolOptions};
use proven_sessions::SessionManagement;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use radix_common::network::NetworkDefinition;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

/// Options for creating a new core.
pub struct CoreOptions<SM: SessionManagement, AM: ApplicationManagement> {
    /// The application manager.
    pub application_manager: AM,

    /// The session manager.
    pub session_manager: SM,
}

/// Options for starting the core.
pub struct CoreStartOptions<HS, AS, PS, NS, ASS, PSS, NSS>
where
    HS: HttpServer,
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    /// Application-scoped SQL store for runtime.
    pub application_sql_store: ASS,

    /// Application-scope KV store for runtime.
    pub application_store: AS,

    /// HTTP server for mounting RPC endpoints.
    pub http_server: HS,

    /// Persona-scoped SQL store for runtime.
    pub personal_sql_store: PSS,

    /// Persona-scoped KV store for runtime.
    pub personal_store: PS,

    /// NFT-scoped SQL store for runtime.
    pub nft_sql_store: NSS,

    /// NFT-scoped KV store for runtime.
    pub nft_store: NS,

    /// Origin to use for Radix Gateway requests.
    pub radix_gateway_origin: String,

    /// Current Radix network.
    pub radix_network_definition: NetworkDefinition,
}

/// Core logic for handling user interactions.
pub struct Core<SM, AM>
where
    SM: SessionManagement,
    AM: ApplicationManagement,
{
    application_manager: AM,
    session_manager: SM,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<SM, AM> Core<SM, AM>
where
    SM: SessionManagement,
    AM: ApplicationManagement,
{
    /// Create new core.
    pub fn new(
        CoreOptions {
            application_manager,
            session_manager,
        }: CoreOptions<SM, AM>,
    ) -> Self {
        Self {
            application_manager,
            session_manager,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Start the core.
    pub async fn start<HS, AS, PS, NS, ASS, PSS, NSS>(
        &self,
        CoreStartOptions {
            application_sql_store,
            application_store,
            http_server,
            personal_sql_store,
            personal_store,
            nft_sql_store,
            nft_store,
            radix_gateway_origin,
            radix_network_definition,
        }: CoreStartOptions<HS, AS, PS, NS, ASS, PSS, NSS>,
    ) -> Result<JoinHandle<Result<(), HS::Error>>, HS::Error>
    where
        HS: HttpServer,
        AS: Store2,
        PS: Store3,
        NS: Store3,
        ASS: SqlStore2,
        PSS: SqlStore3,
        NSS: SqlStore3,
    {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let pool = Pool::new(PoolOptions {
            application_sql_store,
            application_store,
            max_workers: 100,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin,
            radix_network_definition,
        })
        .await;

        let session_router = create_session_router(self.session_manager.clone()).await;
        let http_rpc_router = rpc::http::create_rpc_router(
            self.application_manager.clone(),
            self.session_manager.clone(),
            Arc::clone(&pool),
        );
        let websocket_router = rpc::ws::create_rpc_router(
            self.application_manager.clone(),
            self.session_manager.clone(),
            pool,
        );

        let redirect_response = Response::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .map_err(Error::Http)?;

        let https_app = Router::new()
            .route("/", get(|| async { redirect_response }))
            .nest("/", session_router)
            .nest("/", http_rpc_router)
            .nest("/", websocket_router);

        let shutdown_token = self.shutdown_token.clone();
        let https_handle = http_server.start(https_app).await?;
        let handle = self.task_tracker.spawn(async move {
            tokio::select! {
                () = shutdown_token.cancelled() => {
                    info!("shutdown command received");
                    http_server.shutdown().await;

                    Ok(())
                }
                _ = https_handle => {
                    error!("https server stopped unexpectedly");

                    Err(Error::HttpServerStopped)
                }
            }
        });

        self.task_tracker.close();

        Ok(handle)
    }

    /// Shutdown the core.
    pub async fn shutdown(&self) {
        info!("core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("core shutdown");
    }
}
