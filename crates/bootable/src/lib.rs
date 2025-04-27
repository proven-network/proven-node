//! Abstract interface for bootable services.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for `Bootable` errors.
pub trait BootableError: Debug + Error + Send + Sync {}

/// Trait for bootable services.
#[async_trait]
pub trait Bootable
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the bootable service.
    type Error: BootableError;

    /// Start the bootable service.
    async fn start(&self) -> Result<(), Self::Error>;

    /// Shutdown the bootable service.
    async fn shutdown(&self) -> Result<(), Self::Error>;

    /// Wait for the bootable service to exit.
    async fn wait(&self);
}
