//! Pure consensus logic module
//!
//! This module contains the core consensus implementation without any
//! service orchestration or external dependencies. It focuses purely on
//! the consensus algorithms and state management.

pub mod global;
pub mod group;
pub mod log_index;

// Re-export main types
pub use global::{
    GlobalConsensusLayer, GlobalOperation, GlobalRequest, GlobalResponse, GlobalTypeConfig,
};

pub use group::{
    GroupConsensusLayer, GroupOperation, GroupRequest, GroupResponse, GroupTypeConfig,
};

pub use log_index::{LogIndexExt, OptionLogIndexExt, from_openraft_option, from_openraft_u64};
