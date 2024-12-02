//! Abstract interface for running an Axum-based HTTP server.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use axum::Router;
use tokio::task::JoinHandle;

/// Marker trait for `HttpServer` errors
pub trait HttpServerError: Debug + Error + Send + Sync {}

/// Abstract interface for managing Axum HTTP servers.
#[async_trait]
pub trait HttpServer
where
    Self: Send + Sync + 'static,
{
    /// The error type for this server.
    type Error: HttpServerError;

    /// Start the server with the given Axum router.
    async fn start(&self, router: Router) -> Result<JoinHandle<()>, Self::Error>;

    /// Shutdown the server.
    async fn shutdown(&self);
}
