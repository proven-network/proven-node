//! Provides a tracing subscriber to allow logs to be sent from enclave to host
//! for processing.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

/// Predefined port for vsock tracing.
pub static VSOCK_LOG_PORT: u32 = 1026;

/// Enclave-specific functionality.
pub mod enclave;

/// Host-specific functionality.
pub mod host;
