mod attestation;
mod error;
mod rpc;

use attestation::create_session_router;
pub use error::{Error, Result};

use axum::response::Response;
use axum::routing::get;
use axum::Router;
use proven_http::HttpServer;
use proven_sessions::SessionManagement;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

pub struct NewCoreArguments<SM: SessionManagement + 'static> {
    pub session_manager: SM,
}

pub struct Core<SM: SessionManagement + 'static> {
    session_manager: SM,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<SM: SessionManagement + 'static> Core<SM> {
    pub fn new(args: NewCoreArguments<SM>) -> Self {
        Self {
            session_manager: args.session_manager,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start<HS: HttpServer + 'static>(&self, http_server: HS) -> Result<JoinHandle<()>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let session_router = create_session_router(self.session_manager.clone()).await;
        let http_rpc_router = rpc::http::create_rpc_router(self.session_manager.clone()).await;
        let websocket_router = rpc::ws::create_rpc_router(self.session_manager.clone()).await;

        let https_app = Router::new()
            .route(
                "/",
                get(|| async {
                    Response::builder()
                        .status(301)
                        .header("Location", "https://proven.network")
                        .body("".to_string())
                        .unwrap()
                }),
            )
            .nest("/", session_router)
            .nest("/", http_rpc_router)
            .nest("/", websocket_router);

        let shutdown_token = self.shutdown_token.clone();
        let handle = self.task_tracker.spawn(async move {
            let https_handle = http_server.start(https_app).await.unwrap();

            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("shutdown command received");
                    http_server.shutdown().await;
                }
                e = https_handle => {
                    error!("https server exited: {:?}", e);
                }
            }
        });

        self.task_tracker.close();

        Ok(handle)
    }

    pub async fn shutdown(&self) {
        info!("core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("core shutdown");
    }
}
