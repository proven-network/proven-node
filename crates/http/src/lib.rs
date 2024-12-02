use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use axum::Router;
use tokio::task::JoinHandle;

/// Marker trait for `HttpServer` errors
pub trait HttpServerError: Debug + Error + Send + Sync {}

#[async_trait]
pub trait HttpServer: Send + Sync + 'static {
    type Error: HttpServerError;

    async fn start(&self, router: Router) -> Result<JoinHandle<()>, Self::Error>;
    async fn shutdown(&self);
}
