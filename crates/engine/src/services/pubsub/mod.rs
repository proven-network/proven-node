//! PubSub service for subject-based messaging
//!
//! This service provides a lightweight publish-subscribe system that operates
//! alongside the consensus system. It enables:
//! - Subject-based routing with pattern matching
//! - Distributed interest tracking across nodes
//! - Optional persistence through event bridge
//! - Request-response messaging patterns

pub mod events;
pub mod interest;
pub mod messages;
pub mod router;
pub mod service;
pub mod subscribers;
pub mod types;

pub use events::{PubSubMessage, PubSubServiceEvent};
pub use interest::InterestTracker;
pub use messages::{MessageNotification, PubSubServiceMessage, PubSubServiceResponse};
pub use router::MessageRouter;
pub use service::PubSubService;
pub use types::{
    GlobalConsensusHandle, GroupConsensusHandle, InterestUpdateMessage, PubSubError,
    PubSubMessageType, PubSubNetworkMessage, PubSubRequest, PubSubResponse, PubSubResult,
    StreamMapping, Subscription,
};
