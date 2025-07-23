//! Group consensus layer
//!
//! This module implements the group consensus layer that manages:
//! - Stream operations within a group
//! - Message ordering and persistence
//! - Group-level state management

pub mod callbacks;
pub mod dispatcher;
pub mod operations;
pub mod raft;
pub mod snapshot;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use callbacks::GroupConsensusCallbacks;
pub use operations::{GroupOperation, GroupOperationHandler};
pub use raft::{GroupConsensusLayer, GroupTypeConfig};
pub use types::{GroupRequest, GroupResponse, MessageData, StreamOperation};
