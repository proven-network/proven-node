mod error;
pub mod rpc;
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
use proven_store::{Store2, Store3};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

pub struct CoreOptions<SM: SessionManagement, AM: ApplicationManagement> {
    pub application_manager: AM,
    pub session_manager: SM,
}

pub struct Core<SM: SessionManagement, AM: ApplicationManagement> {
    application_manager: AM,
    session_manager: SM,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<SM: SessionManagement, AM: ApplicationManagement> Core<SM, AM> {
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

    pub async fn start<HS: HttpServer + 'static, AS: Store2, PS: Store3, NS: Store3>(
        &self,
        http_server: HS,
        application_store: AS,
        personal_store: PS,
        nft_store: NS,
        gateway_origin: String,
    ) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let pool = Pool::new(PoolOptions {
            application_store,
            gateway_origin,
            max_workers: 100,
            nft_store,
            personal_store,
        })
        .await;

        let session_router = create_session_router(self.session_manager.clone()).await;
        let http_rpc_router = rpc::http::create_rpc_router(
            self.application_manager.clone(),
            self.session_manager.clone(),
            Arc::clone(&pool),
        )
        .await;
        let websocket_router = rpc::ws::create_rpc_router(
            self.application_manager.clone(),
            self.session_manager.clone(),
            pool,
        )
        .await;

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

    pub async fn shutdown(&self) {
        info!("core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("core shutdown");
    }
}
