//! Command and Control RPC implementation for host-enclave communication.
//!
//! This crate provides the command-specific functionality built on top of
//! the generic `vsock-rpc-core` framework. It handles initialization,
//! shutdown, and other control operations.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod client;
pub mod commands;
pub mod error;
pub mod server;

pub use client::CacClient;
pub use commands::{InitializeRequest, InitializeResponse, ShutdownResponse};
pub use error::{Error, Result};
pub use server::{CacHandler, CacServer};
