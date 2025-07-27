//! PubSub service for subject-based messaging
//!
//! This service provides a lightweight publish-subscribe system that operates
//! alongside the consensus system. It enables:
//! - Subject-based routing with pattern matching
//! - Distributed interest tracking across nodes
//! - Optional persistence through event bridge
//! - Request-response messaging patterns

pub mod command_handlers;
pub mod commands;
pub mod event_handlers;
pub mod events;
pub mod interest;
pub mod internal;
pub mod service;
pub mod streaming;
pub mod streaming_router;
pub mod types;

pub use commands::{PublishMessage, Subscribe, SubscriptionControl};
pub use service::PubSubService;
pub use types::PubSubError;
