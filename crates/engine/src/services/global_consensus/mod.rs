//! Global consensus service module

mod adaptor;
mod callbacks;
pub mod command_handlers;
pub mod commands;
mod config;
pub mod events;
mod handler;
mod messages;
mod service;

pub use config::GlobalConsensusConfig;
pub use service::GlobalConsensusService;
