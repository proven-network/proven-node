//! Client service for handling client requests
//!
//! This service acts as the bridge between external clients and the internal
//! consensus system. It receives client requests and routes them through the
//! event bus to the appropriate consensus services.

mod event_handlers;
pub mod events;
mod handlers;
mod messages;
mod network;
mod service;
mod types;

pub use events::ClientServiceEvent;
pub use messages::*;
pub use service::ClientService;
pub use types::*;
