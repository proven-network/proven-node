//! VSOCK-based logger for Nitro Enclave environments
//!
//! This crate provides both client and server components for high-performance
//! logging over VSOCK. The client batches logs and sends them to a host-side
//! collector with minimal overhead.

#![warn(missing_docs, unreachable_pub)]
#![forbid(unsafe_code)]

mod error;
mod messages;

// Private modules first
mod config;
mod logger;
pub mod server;

/// Client-side subscriber implementation
pub mod client {
    pub use crate::logger::{VsockLoggerConfig, VsockLoggerConfigBuilder, VsockSubscriber};
}

// Re-export common types at crate root
pub use error::{Error, Result};
pub use messages::{LogBatch, LogBatchAck, LogEntry, LogLevel};
pub use server::{
    ChannelLogProcessor, LogProcessor, StdoutLogProcessor, VsockLogCollector,
    VsockLogServerBuilder, run_stdout_collector,
};
