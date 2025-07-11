//! Raft networking interface
//!
//! This module bridges OpenRaft with our transport layer, handling the
//! serialization/deserialization of Raft messages and providing access
//! to real raft state for transports.

pub mod adaptor;
pub mod attestation;
mod cluster_state;
pub mod cose;
pub mod messages;
pub mod network_manager;
pub mod transport;
pub mod verification;

pub use cluster_state::{ClusterState, InitiatorReason};
