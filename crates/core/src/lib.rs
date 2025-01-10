//! Core logic for the Proven node and the entrypoint for all user
//! interactions.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

mod error;
mod rpc;
mod sessions;

pub use error::{Error, Result};
use sessions::create_session_router;

use axum::response::Response;
use axum::routing::get;
use axum::Router;
use proven_applications::ApplicationManagement;
use proven_http::HttpServer;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

/// Options for creating a new core.
pub struct CoreOptions<AM, RM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    /// The application manager.
    pub application_manager: AM,

    /// The runtime pool manager.
    pub runtime_pool_manager: RM,

    /// The session manager.
    pub session_manager: SM,
}

/// Core logic for handling user interactions.
pub struct Core<AM, RM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    application_manager: AM,
    runtime_pool_manager: RM,
    session_manager: SM,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<AM, RM, SM> Core<AM, RM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    /// Create new core.
    pub fn new(
        CoreOptions {
            application_manager,
            runtime_pool_manager,
            session_manager,
        }: CoreOptions<AM, RM, SM>,
    ) -> Self {
        Self {
            application_manager,
            runtime_pool_manager,
            session_manager,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Start the core.
    ///
    /// # Errors
    ///
    /// This function will return an error if the core has already been started or if the HTTP server fails to start.
    pub fn start<HS>(&self, http_server: HS) -> Result<JoinHandle<Result<(), HS::Error>>, HS::Error>
    where
        HS: HttpServer,
    {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let session_router = create_session_router(self.session_manager.clone());
        let http_rpc_router = rpc::http::create_rpc_router(
            self.application_manager.clone(),
            self.runtime_pool_manager.clone(),
            self.session_manager.clone(),
        );
        let websocket_router = rpc::ws::create_rpc_router(
            self.application_manager.clone(),
            self.runtime_pool_manager.clone(),
            self.session_manager.clone(),
        );

        let redirect_response = Response::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .map_err(Error::Http)?;

        let https_app = Router::new()
            .route("/", get(|| async { redirect_response }))
            .merge(session_router)
            .merge(http_rpc_router)
            .merge(websocket_router);

        let shutdown_token = self.shutdown_token.clone();
        let handle = self.task_tracker.spawn(async move {
            let https_handle = http_server.start(https_app).await?;

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
