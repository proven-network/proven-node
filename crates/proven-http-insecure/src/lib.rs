mod error;

use error::Error;

use std::future::IntoFuture;
use std::net::SocketAddr;

use async_trait::async_trait;
use axum::http::Method;
use axum::Router;
use proven_http::HttpServer;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

pub struct InsecureHttpServer {
    listen_addr: SocketAddr,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl InsecureHttpServer {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }
}

#[async_trait]
impl HttpServer for InsecureHttpServer {
    type HE = Error;

    fn start(&self, router: Router) -> Result<JoinHandle<()>, Self::HE> {
        let listen_addr = self.listen_addr;
        let shutdown_token = self.shutdown_token.clone();

        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST])
            .allow_origin(Any);

        let router = router.layer(cors);

        let handle = self.task_tracker.spawn(async move {
            let listener = tokio::net::TcpListener::bind(listen_addr).await.unwrap();

            tokio::select! {
              e = axum::serve(listener, router.into_make_service()).into_future() => {
                info!("http server exited {:?}", e);
              }
              _ = shutdown_token.cancelled() => {}
            };
        });

        self.task_tracker.close();

        Ok(handle)
    }

    async fn shutdown(&self) {
        info!("http server shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("http server shutdown");
    }
}
