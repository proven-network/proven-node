//! Next-generation streaming network layer for proven-node
//!
//! This crate provides a high-performance, streaming-oriented network layer that:
//! - Supports both request-response and streaming communication patterns
//! - Uses flume channels for efficient message passing
//! - Provides multiplexing of logical streams over physical connections
//! - Implements per-stream flow control and backpressure
//! - Maintains backward compatibility with existing network patterns

pub mod attestation;
pub mod connection;
pub mod connection_pool;
pub mod connection_verifier;
pub mod cose;
pub mod error;
pub mod manager;
pub mod message;
pub mod service;
pub mod stream;

// Re-export commonly used types
pub use error::{NetworkError, NetworkResult};
pub use manager::NetworkManager;
pub use message::{NetworkMessage, ServiceMessage, StreamingServiceMessage};
pub use service::{Service, ServiceContext, StreamingService};
pub use stream::{Stream, StreamConfig, StreamHandle};

// Re-export flume for consistent channel usage
pub use flume;
