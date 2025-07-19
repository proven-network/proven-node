//! Global consensus service module

mod adaptor;
mod callbacks;
mod config;
mod messages;
mod service;

pub use adaptor::{GlobalNetworkFactory, GlobalRaftNetworkAdapter};
pub use config::{GlobalConsensusConfig, ServiceState};
pub use messages::*;
pub use service::GlobalConsensusService;
