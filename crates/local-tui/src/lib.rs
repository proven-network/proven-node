//! TUI for managing local Proven clusters
//!
//! This crate provides an interactive terminal user interface for managing
//! Proven nodes using the LocalCluster backend from proven-local-cluster.

pub mod app;
pub mod cluster_adapter;
pub mod logs_reader;
pub mod node_id;
pub mod rpc_operations;
pub mod ui;

pub use app::App;
