//! Events emitted by the global consensus service (new event system)

use proven_topology::NodeId;

use crate::foundation::events::Event;
use crate::foundation::types::ConsensusGroupId;
use crate::services::stream::{StreamConfig, StreamName};

/// Snapshot of global consensus state for synchronization
#[derive(Debug, Clone)]
pub struct GlobalStateSnapshot {
    /// All consensus groups
    pub groups: Vec<GroupSnapshot>,
    /// All streams
    pub streams: Vec<StreamSnapshot>,
}

/// Snapshot of a consensus group
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: ConsensusGroupId,
    pub members: Vec<NodeId>,
}

/// Snapshot of a stream
#[derive(Debug, Clone)]
pub struct StreamSnapshot {
    pub stream_name: StreamName,
    pub config: StreamConfig,
    pub group_id: ConsensusGroupId,
}

/// Events emitted by the global consensus service
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum GlobalConsensusEvent {
    /// Global state synchronized (initial sync)
    StateSynchronized { snapshot: Box<GlobalStateSnapshot> },

    /// New consensus group created
    GroupCreated {
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    },

    /// Consensus group dissolved
    GroupDissolved { group_id: ConsensusGroupId },

    /// Stream created and assigned to a group
    StreamCreated {
        stream_name: StreamName,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    },

    /// Stream deleted
    StreamDeleted { stream_name: StreamName },

    /// Global consensus membership changed
    MembershipChanged {
        added_members: Vec<NodeId>,
        removed_members: Vec<NodeId>,
    },

    /// Leader changed in global consensus (added for tracking leadership)
    LeaderChanged {
        old_leader: Option<NodeId>,
        new_leader: Option<NodeId>,
        term: u64,
    },
}

impl Event for GlobalConsensusEvent {
    fn event_type() -> &'static str {
        "GlobalConsensusEvent"
    }
}
