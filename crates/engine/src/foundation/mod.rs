//! Foundation module containing core types and traits
//!
//! This module contains the fundamental building blocks used throughout
//! the consensus system. It has no dependencies on other modules.

pub mod message;
pub mod message_format;
pub mod traits;
pub mod types;
pub mod validations;

// Re-export commonly used types
pub use message::{Message, headers};
pub use message_format::{deserialize_entry, serialize_entry};
pub use types::{ConsensusGroupId, GroupState, NodeState, OperationId};

pub use traits::{ConsensusLayer, EventHandler, OperationHandler, ServiceLifecycle, StateStore};
