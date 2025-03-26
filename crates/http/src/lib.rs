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

    /// Start the server with the given Axum routers.
    /// - `primary_hostname`: The hostname to match for the primary router
    /// - `primary_router`: Router to use when hostname matches
    /// - `fallback_router`: Router to use when hostname doesn't match
    async fn start(&self, fallback_router: Router) -> Result<JoinHandle<()>, Self::Error>;

    /// Shutdown the server.
    async fn shutdown(&self);

    /// Set the router for a hostname, creating or updating the mapping as needed.
    async fn set_router_for_hostname(
        &self,
        hostname: String,
        router: Router,
    ) -> Result<(), Self::Error>;

    /// Remove the router for the given hostname.
    async fn remove_hostname(&self, hostname: String) -> Result<(), Self::Error>;
}
