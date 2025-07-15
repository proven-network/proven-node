//! Pure consensus logic module
//!
//! This module contains the core consensus implementation without any
//! service orchestration or external dependencies. It focuses purely on
//! the consensus algorithms and state management.

pub mod global;
pub mod group;

// Re-export main types
pub use global::{
    GlobalConsensusLayer, GlobalOperation, GlobalRequest, GlobalResponse, GlobalState,
    GlobalTypeConfig,
};

pub use group::{
    GroupConsensusLayer, GroupOperation, GroupRequest, GroupResponse, GroupState, GroupTypeConfig,
};
