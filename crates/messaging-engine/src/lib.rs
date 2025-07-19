//! Engine-based messaging implementation.
//!
//! This implementation provides messaging functionality by:
//! - Using the proven engine's stream functionality
//! - Leveraging engine's consensus for message ordering
//! - Providing pub/sub patterns on top of streams
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

/// Clients send requests to services in the consensus network.
pub mod client;

/// Consumers are stateful views of consensus streams.
pub mod consumer;

/// Error types for the messaging-consensus system.
pub mod error;

/// Services are special consumers that respond to requests in the consensus network.
pub mod service;

/// Service responders handle responses in the consensus network.
pub mod service_responder;

/// Consensus-based streams with immediate consistency.
pub mod stream;

/// Subjects in the consensus messaging system.
pub mod subject;

/// Subscription management in the consensus network.
pub mod subscription;

/// Subscription responders handle subscription responses.
pub mod subscription_responder;

// Re-export engine types for convenience
pub use proven_engine::{Client as EngineClient, StreamConfig};

// Re-export messaging-specific error types
pub use error::MessagingEngineError;

// Re-export messaging-specific types
pub use client::EngineMessagingClient;
pub use consumer::{
    EngineMessagingConsumer, EngineMessagingConsumerError, EngineMessagingConsumerOptions,
};
pub use service::{
    EngineMessagingService, EngineMessagingServiceError, EngineMessagingServiceOptions,
};
pub use service_responder::{EngineMessagingServiceResponder, EngineMessagingUsedResponder};
pub use stream::{
    EngineStream, EngineStream1, EngineStream2, EngineStream3, EngineStreamOptions,
    InitializedEngineStream,
};
pub use subject::EngineMessagingSubject;
pub use subscription::{EngineMessagingSubscription, EngineMessagingSubscriptionOptions};
pub use subscription_responder::{
    EngineMessagingSubscriptionResponder, EngineMessagingUsedSubscriptionResponder,
};
