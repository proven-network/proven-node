//! Group consensus service module

mod adaptor;
mod callbacks;
mod config;
mod messages;
mod service;
mod types;

pub use adaptor::{GroupNetworkFactory, GroupRaftNetworkAdapter};
pub use config::{GroupConsensusConfig, ServiceState};
pub use messages::*;
pub use service::GroupConsensusService;
pub use types::{GroupStateInfo, StreamInfo};
