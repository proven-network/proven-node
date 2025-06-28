//! Abstract interface for bootable services.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use async_trait::async_trait;

/// Trait for bootable services.
#[async_trait]
pub trait Bootable
where
    Self: Send + Sync + 'static,
{
    /// Get the name of the bootable service.
    fn name(&self) -> &str;

    /// Start the bootable service.
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Shutdown the bootable service.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Wait for the bootable service to exit.
    async fn wait(&self);
}
