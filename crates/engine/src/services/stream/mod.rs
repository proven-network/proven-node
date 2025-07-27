//! Stream service module
//!
//! This service manages stream storage and provides stream operations.

pub mod command_handlers;
pub mod commands;
pub mod config;
pub mod event_handlers;
pub mod events;
mod handler;
pub mod messages;
pub mod service;
pub mod storage;
pub mod streaming_commands;
pub mod streaming_handlers;
pub mod subscribers;
pub mod types;

// Re-export the main types
pub use commands::StreamInfo;
pub use service::{StreamService, StreamServiceConfig};
pub use types::{MessageData, MessageData as StreamMessage};

// Re-export from foundation
pub use crate::foundation::{StoredMessage, deserialize_stored_message};
