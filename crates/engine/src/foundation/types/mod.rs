//! Core types used throughout the consensus system

pub mod enums;
pub mod ids;
pub mod timestamp;

// Re-export commonly used types
pub use enums::{ConsensusRole, GroupStatus, NodeState, OperationPriority};
pub use ids::{ConsensusGroupId, OperationId, Term};
pub use timestamp::ConsensusTimestamp;
