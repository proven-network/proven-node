//! Stream service module
//!
//! This service manages stream storage and provides stream operations.

pub mod command_handlers;
pub mod commands;
pub mod events;
mod handler;
pub mod messages;
pub mod service;
pub mod streaming;

// Internal implementation details
mod internal;

// Re-export the main types
pub use service::{StreamService, StreamServiceConfig};

// Re-export from foundation
pub use crate::foundation::PersistenceType;
