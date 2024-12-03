//! Manages RPC between host and enclave over virtio sockets. Defines commands
//! like initialize, shutdown, add peer, etc.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod client;
mod common;
mod error;
mod server;

pub use client::RpcClient;
pub use common::*;
pub use error::{Error, Result};
pub use server::{RpcCall, RpcServer};
