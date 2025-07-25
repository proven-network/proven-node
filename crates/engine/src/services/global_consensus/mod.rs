//! Global consensus service module

mod adaptor;
mod callbacks;
pub mod command_handlers;
pub mod commands;
mod config;
pub mod events;
mod messages;
mod service;

pub use adaptor::{GlobalNetworkFactory, GlobalRaftNetworkAdapter};
pub use config::{GlobalConsensusConfig, ServiceState};
pub use events::GlobalConsensusEvent;
pub use messages::*;
pub use service::GlobalConsensusService;
