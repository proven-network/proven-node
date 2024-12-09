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

/// Serivces special consumers that respond to requests.
pub mod service;

/// Streams are persistent, ordered, and append-only sequences of messages.
pub mod stream;

/// Subjects are named channels for messages.
pub mod subject;

/// Subscribers consume messages from subjects.
pub mod subscription;

/// Subscribption handlers process messages for subscribers.
pub mod subscription_handler;
