//! Group consensus service module

mod adaptor;
mod callbacks;
pub mod command_handlers;
pub mod commands;
mod config;
pub mod events;
mod handler;
mod messages;
mod service;
pub mod subscribers;
mod types;

pub use config::GroupConsensusConfig;
pub use service::GroupConsensusService;
pub use types::GroupStateInfo;
