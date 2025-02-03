//! NATS implementation of the messaging crate.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

/// Clients send requests to services.
pub mod client;

/// Consumers are stateful views of streams.
pub mod consumer;

/// Services are special consumers that respond to requests.
pub mod service;

/// Service responders are responders for services.
pub mod service_responder;

/// Streams are persistent, ordered, and append-only sequences of messages.
pub mod stream;

/// Subjects can be published to and subscribed to (by passing a handler).
pub mod subject;

/// Subscribers are created by subscribing to a subject.
pub mod subscription;

/// Subscription responders are responders for subscriptions.
pub mod subscription_responder;
