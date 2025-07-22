//! Proven Node TUI library

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

/// Application state and logic
pub mod app;
/// IP address allocation
pub mod ip_allocator;
/// Log level adapters  
pub mod log_level_adapter;
/// Logger context adapters
pub mod logger_context;
/// Log categorization
pub mod logs_categorizer;
/// Log formatting
pub mod logs_formatter;
/// Log viewing (memory-mapped)
pub mod logs_viewer;
/// Log writing
pub mod logs_writer;
/// Messages and types
pub mod messages;
/// Node ID management
pub mod node_id;
/// Node manager
pub mod node_manager;
/// RPC client
pub mod rpc_client;
/// UI components
pub mod ui;
