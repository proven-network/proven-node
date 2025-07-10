//! Generic VSOCK RPC framework for host-enclave communication.
//!
//! This crate provides a high-performance, generic RPC framework built on VSOCK
//! that supports multiple communication patterns, connection pooling, and
//! pluggable serialization.
//!
//! # Features
//!
//! - **Multiple Patterns**: Request/response, streaming, and fire-and-forget
//! - **Connection Pooling**: Reuse connections for better performance
//! - **CBOR Serialization**: Efficient binary format using ciborium
//! - **Automatic Retry**: Configurable retry policies with exponential backoff
//! - **Type Safety**: Strongly typed messages with compile-time guarantees
//!
//! # Example
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use vsock_rpc_core::{RpcClient, RpcMessage};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Echo {
//!     message: String,
//! }
//!
//! impl RpcMessage for Echo {
//!     type Response = Echo;
//!
//!     fn message_id(&self) -> &'static str {
//!         "echo"
//!     }
//! }
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = RpcClient::builder()
//!         .vsock_addr((2, 5000).into())
//!         .build()
//!         .await?;
//!
//!     let response = client
//!         .request(Echo {
//!             message: "Hello, VSOCK!".to_string(),
//!         })
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod error;
pub mod protocol;
pub mod transport;

// Re-export commonly used types
pub use error::{Error, Result};
pub use protocol::{
    codec,
    message::{MessageId, RpcMessage},
    patterns::{MessagePattern, RequestOptions},
};
pub use transport::{
    client::{ClientBuilder, ClientConfig, RpcClient},
    connection::{ConnectionPool, PoolConfig},
    server::{HandlerResponse, RpcHandler, RpcServer, ServerConfig},
};

// Re-export dependencies that are part of our public API
pub use bytes::Bytes;
pub use tokio_vsock::VsockAddr;
