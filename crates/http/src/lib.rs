//! Abstract interface for running an Axum-based HTTP server.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use axum::Router;
use proven_bootable::Bootable;

/// Marker trait for `HttpServer` errors
pub trait HttpServerError: Debug + Error + Send + Sync {}

/// Abstract interface for managing Axum HTTP servers.
#[async_trait]
pub trait HttpServer
where
    Self: Bootable + Send + Sync + 'static,
{
    /// The error type for this server.
    type Error: HttpServerError;

    /// Set the router for a hostname, creating or updating the mapping as needed.
    async fn set_router_for_hostname(
        &self,
        hostname: String,
        router: Router,
    ) -> Result<(), <Self as HttpServer>::Error>;

    /// Remove the router for the given hostname.
    async fn remove_hostname(&self, hostname: String) -> Result<(), <Self as HttpServer>::Error>;
}
