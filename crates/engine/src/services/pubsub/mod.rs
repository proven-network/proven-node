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
pub mod messages;
pub mod service;
pub mod streaming_commands;
pub mod streaming_handlers;
pub mod streaming_router;
pub mod types;

pub use commands::{PublishMessage, Subscribe, Unsubscribe};
pub use events::{PubSubMessage, PubSubServiceEvent};
pub use interest::InterestTracker;
pub use messages::{MessageNotification, PubSubServiceMessage, PubSubServiceResponse};
pub use service::PubSubService;
pub use streaming_commands::{StreamingSubscriptionHandle, SubscribeStream, SubscriptionControl};
pub use streaming_router::{MessageChannel, StreamingMessageRouter};
pub use types::{
    GlobalConsensusHandle, GroupConsensusHandle, InterestUpdateMessage, PubSubError,
    PubSubMessageType, PubSubNetworkMessage, PubSubRequest, PubSubResponse, PubSubResult,
    StreamMapping, Subscription,
};
