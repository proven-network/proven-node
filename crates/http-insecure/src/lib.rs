//! Implementation of simple non-secure HTTP for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::collections::HashMap;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use parking_lot::RwLock;
use proven_bootable::Bootable;
use proven_http::HttpServer;
use proven_logger::info;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::Service;

/// Simple non-secure HTTP server.
#[derive(Clone)]
pub struct InsecureHttpServer {
    fallback_router: Router,
    listen_addr: SocketAddr,
    hostname_routers: Arc<RwLock<HashMap<String, Router>>>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl InsecureHttpServer {
    /// Creates a new instance of `InsecureHttpServer`.
    #[must_use]
    pub fn new(listen_addr: SocketAddr, fallback_router: Router) -> Self {
        Self {
            fallback_router,
            listen_addr,
            hostname_routers: Arc::new(RwLock::new(HashMap::new())),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }
}

#[async_trait]
impl Bootable for InsecureHttpServer {
    fn bootable_name(&self) -> &'static str {
        "http (insecure)"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        let routers = self.hostname_routers.clone();
        let fallback_router = self.fallback_router.clone();
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

        let listener = tokio::net::TcpListener::bind(self.listen_addr)
            .await
            .map_err(Error::Bind)?;

        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            tokio::select! {
                e = axum::serve(listener, router.into_make_service()).into_future() => {
                    info!("http server exited {e:?}");
                }
                () = shutdown_token.cancelled() => {}
            };
        });

        self.task_tracker.close();

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("http server shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("http server shutdown");

        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}

#[async_trait]
impl HttpServer for InsecureHttpServer {
    type Error = Error;

    async fn set_router_for_hostname(&self, hostname: String, router: Router) -> Result<(), Error> {
        self.hostname_routers.write().insert(hostname, router);

        Ok(())
    }

    async fn remove_hostname(&self, hostname: String) -> Result<(), Error> {
        self.hostname_routers.write().remove(&hostname);

        Ok(())
    }
}
