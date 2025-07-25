//! Event system v2 - High-performance event bus using flume channels
//!
//! Key improvements over v1:
//! - Zero task spawning for event delivery
//! - Flume channels for better performance
//! - Support for both fire-and-forget and request-response patterns
//! - Built-in metrics and observability
//! - Backpressure handling
//! - Event filtering and routing
//! - Graceful shutdown support

mod bus;
mod error;
mod metrics;
mod traits;
mod types;

pub use bus::{EventBus, EventBusBuilder};
pub use error::{Error, Result};
pub use traits::{Event, EventHandler, Request, RequestHandler, StreamHandler, StreamRequest};
pub use types::{EventId, EventMetadata, Priority};

// Re-export flume types for convenience
pub use flume::{Receiver, Sender};
