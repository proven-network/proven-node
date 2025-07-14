//! Raft networking interface
//!
//! This module bridges OpenRaft with our transport layer, handling the
//! serialization/deserialization of Raft messages and providing access
//! to real raft state for transports.

mod cluster_state;
pub mod messages;

pub use cluster_state::{ClusterState, InitiatorReason};
