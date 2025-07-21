//! Event service for consensus operations
//!
//! This service provides a type-safe event-driven communication system that allows
//! different components to communicate asynchronously while maintaining clear boundaries.
//!
//! ## Overview
//!
//! The event service enables:
//! - Type-safe async communication between services
//! - Per-service event definitions
//! - Priority-based event handling
//! - Event statistics and monitoring
//!
//! ## Architecture
//!
//! - **Event Bus**: Central message broker for events
//! - **Service Events**: Per-service event enums implementing ServiceEvent trait
//! - **Event Handlers**: Type-safe handlers for specific event types
//! - **Event Service**: Lifecycle management and statistics
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Define service-specific events
//! #[derive(Debug, Clone)]
//! enum MyServiceEvent {
//!     TaskCompleted { id: u64 },
//!     ErrorOccurred { message: String },
//! }
//!
//! impl ServiceEvent for MyServiceEvent {
//!     fn event_name(&self) -> &'static str {
//!         match self {
//!             Self::TaskCompleted { .. } => "MyServiceEvent::TaskCompleted",
//!             Self::ErrorOccurred { .. } => "MyServiceEvent::ErrorOccurred",
//!         }
//!     }
//! }
//!
//! // Subscribe to events
//! let handler = MyEventHandler::new();
//! event_bus.subscribe(handler).await;
//!
//! // Publish events
//! event_bus.publish(MyServiceEvent::TaskCompleted { id: 42 }).await;
//! ```

pub mod bus;
pub mod traits;

// Main event service
mod service;

// Re-export event service as the main EventService
pub use service::{EventService, EventServiceConfig};

// Core exports
pub use bus::{EventBus, EventBusStats, SubscriptionId};
pub use traits::{EventHandler, EventPriority, ServiceEvent};
