use crate::LightContext;
use crate::error::Error;
use crate::handlers::nats_cluster_endpoint_handler;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{any, get};
use axum::{Json, Router};
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_governance::Governance;
use proven_http::HttpServer;
use proven_network::{NATS_CLUSTER_ENDPOINT_API_PATH, ProvenNetwork};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

/// Options for creating a new core.
pub struct LightCoreOptions<A, G, HS>
where
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    /// The HTTP server.
    pub http_server: HS,

    /// The network for peer discovery.
    pub network: ProvenNetwork<G, A>,
}

/// Core logic for handling user interactions.
#[derive(Clone)]
pub struct LightCore<A, G, HS>
where
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    http_server: HS,
    network: ProvenNetwork<G, A>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<A, G, HS> LightCore<A, G, HS>
where
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    /// Create new core.
    pub fn new(
        LightCoreOptions {
            http_server,
            network,
        }: LightCoreOptions<A, G, HS>,
    ) -> Self {
        Self {
            http_server,
            network,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }
}

#[async_trait]
impl<A, G, HS> Bootable for LightCore<A, G, HS>
where
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    fn name(&self) -> &'static str {
        "core (light)"
    }

    /// Start the core.
    ///
    /// # Errors
    ///
    /// This function will return an error if the core has already been started or if the HTTP server fails to start.
    #[allow(clippy::missing_panics_doc)] // TODO: Remove with test code
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyStarted));
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

        let fqdn = self
            .network
            .fqdn()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        self.http_server
            .set_router_for_hostname(fqdn, primary_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        // Router for WebAuthn related origin requests
        let primary_auth_gateway = self
            .network
            .governance()
            .get_primary_auth_gateway()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let alternates_auth_gateways = self
            .network
            .governance()
            .get_alternates_auth_gateways()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let webauthn_router = Router::new().route(
            "/.well-known/webauthn",
            get(|| async move {
                Json(json!({
                    "origins": alternates_auth_gateways
                }))
            }),
        );

        self.http_server
            .set_router_for_hostname(primary_auth_gateway, webauthn_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        let http_server = self.http_server.clone();
        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            http_server
                .start()
                .await
                .map_err(|e| Error::HttpServer(e.to_string()))?;

            tokio::select! {
                () = shutdown_token.cancelled() => {
                    info!("shutdown command received");
                    let _ = http_server.shutdown().await;

                    Ok(())
                }
                () = http_server.wait() => {
                    error!("https server stopped unexpectedly");

                    Err(Error::HttpServer("https server stopped unexpectedly".to_string()))
                }
            }
        });

        self.task_tracker.close();

        Ok(())
    }

    /// Shutdown the core.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("light core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("light core shutdown");

        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}
