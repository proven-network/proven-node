//! Group consensus service module

mod adaptor;
mod config;
mod messages;
mod service;

pub use adaptor::{GroupNetworkFactory, GroupRaftNetworkAdapter};
pub use config::{GroupConsensusConfig, ServiceState};
pub use messages::*;
pub use service::GroupConsensusService;
