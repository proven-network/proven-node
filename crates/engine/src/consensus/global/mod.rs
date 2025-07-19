//! Global consensus layer
//!
//! This module implements the global consensus layer that manages:
//! - Cluster membership
//! - Stream to group assignments
//! - Group lifecycle
//! - Node assignments

pub mod callbacks;
pub mod dispatcher;
pub mod operations;
pub mod raft;
pub mod snapshot;
pub mod state;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use callbacks::GlobalConsensusCallbacks;
pub use operations::{GlobalOperation, GlobalOperationHandler};
pub use raft::{GlobalConsensusLayer, GlobalTypeConfig};
pub use state::GlobalState;
pub use types::{GlobalRequest, GlobalResponse, GroupInfo};
