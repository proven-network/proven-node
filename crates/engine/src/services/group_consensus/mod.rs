//! Group consensus service module

mod adaptor;
mod callbacks;
mod config;
pub mod events;
mod messages;
mod service;
pub mod subscribers;
mod types;

pub use adaptor::{GroupNetworkFactory, GroupRaftNetworkAdapter};
pub use config::{GroupConsensusConfig, ServiceState};
pub use events::GroupConsensusEvent;
pub use messages::*;
pub use service::GroupConsensusService;
pub use types::{GroupStateInfo, StreamInfo};
