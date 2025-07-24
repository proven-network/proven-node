//! Foundation module containing core types and traits
//!
//! This module contains the fundamental building blocks used throughout
//! the consensus system. It has no dependencies on other modules.

pub mod messages;
pub mod models;
pub mod state;
pub mod traits;
pub mod types;
pub mod validations;

// Re-export commonly used items for backward compatibility and convenience

// From messages
pub use messages::{Message, deserialize_entry, headers, serialize_entry};

// From models
pub use models::{GroupInfo, GroupMetadata, NodeInfo, StreamInfo, StreamState, StreamStats};

// From state
pub use state::{
    GlobalState, GlobalStateRead, GlobalStateReader, GlobalStateWrite, GlobalStateWriter,
    GroupState, GroupStateRead, GroupStateReader, GroupStateWrite, GroupStateWriter,
    create_group_state_access, create_state_access,
};

// From traits
pub use traits::consensus::StateStore;
pub use traits::{
    ConsensusLayer, EventHandler, GroupManager, OperationHandler, ServiceCoordinator,
    ServiceLifecycle, StreamManager,
};

// From types
pub use types::{
    ConsensusGroupId, ConsensusRole, ConsensusTimestamp, GroupStatus, LogIndex, NodeState,
    OperationId, OperationPriority, Term,
};
