use crate::LightContext;
use crate::error::{Error, Result};
use crate::handlers::nats_cluster_endpoint_handler;

use std::collections::HashSet;

use axum::Router;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{any, get};
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_http::HttpServer;
use proven_network::{NATS_CLUSTER_ENDPOINT_API_PATH, ProvenNetwork};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

/// Options for creating a new core.
pub struct LightCoreOptions<A, G>
where
    A: Attestor,
    G: Governance,
{
    /// The network for peer discovery.
    pub network: ProvenNetwork<G, A>,

    /// The primary hostnames for RPC, WS, etc.
    pub primary_hostnames: HashSet<String>,
}

/// Core logic for handling user interactions.
pub struct LightCore<A, G>
where
    A: Attestor,
    G: Governance,
{
    network: ProvenNetwork<G, A>,
    primary_hostnames: HashSet<String>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<A, G> LightCore<A, G>
where
    A: Attestor,
    G: Governance,
{
    /// Create new core.
    pub fn new(
        LightCoreOptions {
            network,
            primary_hostnames,
        }: LightCoreOptions<A, G>,
    ) -> Self {
        Self {
            network,
            primary_hostnames,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Start the core.
    ///
    /// # Errors
    ///
    /// This function will return an error if the core has already been started or if the HTTP server fails to start.
    #[allow(clippy::missing_panics_doc)] // TODO: Remove with test code
    pub async fn start<HS>(&self, http_server: HS) -> Result<JoinHandle<Result<()>>>
    where
        HS: HttpServer,
    {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let redirect_response = Response::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .map_err(Error::Http)?;

        let light_ctx = LightContext {
            network: self.network.clone(),
        };

        let primary_router = Router::new()
            .route("/", get(|| async { redirect_response }))
            .route(
                NATS_CLUSTER_ENDPOINT_API_PATH,
                get(nats_cluster_endpoint_handler).with_state(light_ctx.clone()),
            )
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive());

        let error_404_router = Router::new()
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive());

        let primary_hostnames = self.primary_hostnames.clone();
        let shutdown_token = self.shutdown_token.clone();
        let handle = self.task_tracker.spawn(async move {
            let https_handle = http_server
                .start(primary_hostnames, primary_router, error_404_router)
                .await
                .map_err(|e| Error::HttpServer(e.to_string()))?;

            tokio::select! {
                () = shutdown_token.cancelled() => {
                    info!("shutdown command received");
                    http_server.shutdown().await;

                    Ok(())
                }
                _ = https_handle => {
                    error!("https server stopped unexpectedly");

                    Err(Error::HttpServer("https server stopped unexpectedly".to_string()))
                }
            }
        });

        self.task_tracker.close();

        Ok(handle)
    }

    /// Shutdown the core.
    pub async fn shutdown(&self) {
        info!("light core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("light core shutdown");
    }
}
