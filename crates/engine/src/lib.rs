//! Clean consensus implementation with pluggable transports
//!
//! This crate provides a simplified consensus system where:
//! - Consensus owns all business logic
//! - Transports handle pure networking
//! - No circular dependencies
//! - Single builder pattern for initialization

#![warn(missing_docs)]
#![warn(clippy::all)]
#![allow(clippy::result_large_err)]
// TODO: Remove this soon
#![allow(dead_code)]
#![allow(unused_imports)]

/// Builder for the engine
pub mod builder;

/// Client API
pub mod client;

/// Configuration types
pub mod config;

/// Coordinator for the engine
pub(crate) mod coordinator;

/// Pure consensus logic
pub mod consensus;

/// Engine - orchestration layer
pub(crate) mod engine;

/// Error types
pub mod error;

/// Foundation module with core types and traits
pub mod foundation;

/// Services
pub(crate) mod services;

/// Stream subsystem
pub mod stream;

pub use {
    builder::EngineBuilder, client::Client, config::EngineConfig, engine::Engine,
    engine::EngineState,
};

// Re-export cluster types for the public API
pub use services::cluster::{ClusterInfo, ClusterState};
