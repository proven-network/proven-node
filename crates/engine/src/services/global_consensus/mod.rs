//! Global consensus service module

mod adaptor;
mod callbacks;
mod config;
pub mod events;
mod messages;
mod service;
mod subscribers;
mod topology_monitor;

pub use adaptor::{GlobalNetworkFactory, GlobalRaftNetworkAdapter};
pub use config::{GlobalConsensusConfig, ServiceState};
pub use events::GlobalConsensusEvent;
pub use messages::*;
pub use service::GlobalConsensusService;
