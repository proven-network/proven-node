//! Raft networking interface
//!
//! This module bridges OpenRaft with our transport layer, handling the
//! serialization/deserialization of Raft messages and providing access
//! to real raft state for transports.

pub mod adaptor;
mod cluster_state;
pub mod messages;
pub mod network_manager;

pub use cluster_state::{ClusterState, InitiatorReason};
