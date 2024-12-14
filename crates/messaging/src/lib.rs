//! Abstract interface for managing distributed messaging.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

/// Clients send requests to services.
pub mod client;

/// Consumers are stateful views of streams.
pub mod consumer;

/// Consumer handlers process messages for consumers.
pub mod consumer_handler;

/// Service responders handle responses generated in service handlers.
pub mod service_responder;

/// Services are special consumers that respond to requests.
pub mod service;

/// Service handlers process messages for services.
pub mod service_handler;

/// Streams are persistent, ordered, and append-only sequences of messages.
pub mod stream;

/// Subjects are named channels for messages.
pub mod subject;

/// Subscribers consume messages from subjects.
pub mod subscription;

/// Subscribption handlers process messages for subscribers.
pub mod subscription_handler;

/// Subscription responders handle responses generated in subscription handlers.
pub mod subscription_responder;
