//! Implementation of simple non-secure HTTP for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::collections::{HashMap, HashSet};
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use parking_lot::RwLock;
use proven_http::HttpServer;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::Service;
use tracing::info;

/// Simple non-secure HTTP server.
pub struct InsecureHttpServer {
    listen_addr: SocketAddr,
    hostname_routers: Arc<RwLock<HashMap<String, Router>>>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl InsecureHttpServer {
    /// Creates a new instance of `InsecureHttpServer`.
    #[must_use]
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            hostname_routers: Arc::new(RwLock::new(HashMap::new())),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }
}

#[async_trait]
impl HttpServer for InsecureHttpServer {
    type Error = Error;

    async fn start(
        &self,
        primary_hostnames: HashSet<String>,
        primary_router: Router,
        fallback_router: Router,
    ) -> Result<JoinHandle<()>, Self::Error> {
        let listen_addr = self.listen_addr;
        let shutdown_token = self.shutdown_token.clone();

        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        {
            let mut routers = self.hostname_routers.write();
            for primary_hostname in primary_hostnames {
                routers.insert(primary_hostname, primary_router.clone());
            }
        }

        let routers = self.hostname_routers.clone();
        let router = Router::new().fallback_service(tower::service_fn(
            move |req: axum::http::Request<_>| {
                let mut fallback = fallback_router.clone();
                let routers = routers.clone();

                async move {
                    let host = req
                        .headers()
                        .get(axum::http::header::HOST)
                        .and_then(|hv| {
                            hv.to_str()
                                // Ignore port component of host header
                                .map(|h| h.split(':').next().unwrap_or_default())
                                .ok()
                        })
                        .unwrap_or_default();

                    let router = if host.is_empty() {
                        None
                    } else {
                        routers.read().get(host).cloned()
                    };

                    match router {
                        Some(mut r) => r.call(req).await,
                        None => fallback.call(req).await,
                    }
                }
            },
        ));

        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .map_err(Error::Bind)?;

        let handle = self.task_tracker.spawn(async move {
            tokio::select! {
                e = axum::serve(listener, router.into_make_service()).into_future() => {
                    info!("http server exited {:?}", e);
                }
                () = shutdown_token.cancelled() => {}
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

    async fn set_router_for_hostname(
        &self,
        hostname: String,
        router: Router,
    ) -> Result<(), Self::Error> {
        self.hostname_routers.write().insert(hostname, router);
        Ok(())
    }

    async fn remove_hostname(&self, hostname: String) -> Result<(), Self::Error> {
        self.hostname_routers.write().remove(&hostname);
        Ok(())
    }
}
