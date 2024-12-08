//! NATS implementation of the messaging crate.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

/// Subjects can be published to and subscribed to (by passing a handler).
pub mod subject;

/// Subscribers are created by subscribing to a subject.
pub mod subscriber;
