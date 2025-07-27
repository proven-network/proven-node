//! Types for group consensus service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_storage::ConsensusStorage;

use crate::consensus::group::GroupConsensusLayer;
use crate::foundation::GroupStateReader;
use crate::foundation::types::ConsensusGroupId;

// Re-export types from foundation
pub use crate::foundation::{GroupStateInfo, GroupStreamInfo as StreamInfo};

/// Type alias for the consensus layers map
pub type ConsensusLayers<S> = Arc<
    RwLock<
        std::collections::HashMap<ConsensusGroupId, Arc<GroupConsensusLayer<ConsensusStorage<S>>>>,
    >,
>;

/// Type alias for the group states map
pub type States = Arc<RwLock<std::collections::HashMap<ConsensusGroupId, GroupStateReader>>>;
