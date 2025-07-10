//! Transport layer for the RPC framework.
//!
//! This module handles the low-level connection management,
//! including connection pooling, client/server implementations,
//! and health checking.

pub mod client;
pub mod connection;
pub mod server;

pub use client::{ClientBuilder, ClientConfig, RpcClient};
pub use connection::{ConnectionPool, PoolConfig, PooledConnection, ResponseSender};
pub use server::{HandlerResponse, RpcHandler, RpcServer, ServerConfig};
