//! Foundation module containing core types and traits
//!
//! This module contains the fundamental building blocks used throughout
//! the consensus system. It has no dependencies on other modules.

pub mod traits;
pub mod types;

// Re-export commonly used types
pub use types::{ConsensusGroupId, GroupState, NodeState, OperationId};

pub use traits::{ConsensusLayer, EventHandler, OperationHandler, ServiceLifecycle, StateStore};
