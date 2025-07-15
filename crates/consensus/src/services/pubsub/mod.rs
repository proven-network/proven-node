//! PubSub service for subject-based messaging
//!
//! This service provides a lightweight publish-subscribe system that operates
//! alongside the consensus system. It enables:
//! - Subject-based routing with pattern matching
//! - Distributed interest tracking across nodes
//! - Optional persistence through event bridge
//! - Request-response messaging patterns

pub mod interest;
pub mod messages;
pub mod router;
pub mod service;
pub mod subject;
pub mod types;

pub use interest::InterestTracker;
pub use messages::{InterestUpdateMessage, PubSubMessageType, PubSubNetworkMessage};
pub use router::MessageRouter;
pub use service::PubSubService;
pub use subject::{Subject, SubjectPattern, subject_matches_pattern};
pub use types::{
    DeliveryMode, GlobalConsensusHandle, GroupConsensusHandle, PubSubError, PubSubRequest,
    PubSubResponse, PubSubResult, StreamMapping, Subscription,
};
