//! Event service for consensus operations
//!
//! This service provides an event-driven communication system that allows
//! different components to communicate asynchronously while maintaining clear boundaries.
//!
//! ## Overview
//!
//! The event service enables:
//! - Async communication between consensus layers
//! - Event routing and filtering
//! - Request-response patterns via reply channels
//! - Event persistence and replay
//! - Monitoring and metrics
//!
//! ## Architecture
//!
//! - **Event Bus**: Central message broker for events
//! - **Event Router**: Routes events to appropriate handlers
//! - **Event Store**: Optional persistence for events
//! - **Event Filters**: Subscription filtering capabilities
//!
//! ## Usage
//!
//! ```rust,ignore
//! let event_service = EventService::new(config);
//! event_service.start().await?;
//!
//! // Subscribe to events
//! let mut subscriber = event_service.subscribe("my-component", EventFilter::All).await?;
//!
//! // Publish an event
//! event_service.publish(Event::StreamCreated { ... }).await?;
//!
//! // Handle events
//! while let Some(event) = subscriber.recv().await {
//!     match event {
//!         Event::StreamCreated { reply_to, ... } => {
//!             // Process and reply
//!             reply_to.reply(EventResult::Success);
//!         }
//!         _ => {}
//!     }
//! }
//! ```

mod bus;
mod filters;
mod router;
mod service;
mod store;
mod types;

pub use bus::{EventBus, EventPublisher, EventSubscriber};
pub use filters::{EventFilter, FilterExpression, FilterOperator, FilterValue};
pub use router::{EventHandler, EventRoute, EventRoutePattern, EventRouter};
pub use service::{EventConfig, EventService};
pub use store::{EventHistory, EventQuery, EventStore};
pub use types::{
    Event, EventEnvelope, EventError, EventId, EventMetadata, EventPriority, EventResult,
    EventTimestamp, EventType, EventingResult,
};
