//! Proven Node TUI library

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

/// App module for SQL version
pub mod app;
/// IP address allocation
pub mod ip_allocator;
/// Log viewing (SQL-based)
pub mod logs_viewer;
/// Messages and types
pub mod messages;
/// Node ID management
pub mod node_id;
/// Node manager
pub mod node_manager;
/// RPC client
pub mod rpc_client;
/// UI components (SQL-based)
pub mod ui;
