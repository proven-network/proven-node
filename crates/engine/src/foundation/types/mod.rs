//! Core types used throughout the consensus system

pub mod enums;
pub mod health;
pub mod ids;
pub mod node;
pub mod stream;
pub mod subject;
pub mod timestamp;

// Re-export commonly used types
pub use enums::{ConsensusRole, GroupStatus, NodeState, OperationPriority};
pub use health::{ComponentHealth, HealthStatus, StreamHealth};
pub use ids::{ConsensusGroupId, OperationId, Term};
pub use node::{ClusterFormationState, NodeRole, NodeStatus};
pub use stream::StreamName;
pub use subject::{Subject, SubjectError, SubjectPattern, subject_matches_pattern};
pub use timestamp::ConsensusTimestamp;
