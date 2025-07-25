//! Events emitted by the group consensus service (new event system)

use std::num::NonZero;

use proven_topology::NodeId;

use crate::consensus::group::types::MessageData;
use crate::foundation::events::Event;
use crate::foundation::types::ConsensusGroupId;
use std::sync::Arc;

/// Messages appended event details
#[derive(Debug, Clone)]
pub struct MessagesAppendedData {
    pub group_id: ConsensusGroupId,
    pub stream_name: String,
    pub message_count: usize,
}

/// Membership changed event details
#[derive(Debug, Clone)]
pub struct MembershipChangedData {
    pub group_id: ConsensusGroupId,
    pub added_members: Vec<NodeId>,
    pub removed_members: Vec<NodeId>,
}

/// Events emitted by the group consensus service
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum GroupConsensusEvent {
    /// Group state synchronized (after initial sync or recovery)
    StateSynchronized { group_id: ConsensusGroupId },

    /// Stream created in this group
    StreamCreated {
        group_id: ConsensusGroupId,
        stream_name: String,
    },

    /// Stream removed from this group
    StreamRemoved {
        group_id: ConsensusGroupId,
        stream_name: String,
    },

    /// Messages appended to a stream
    MessagesAppended(Box<MessagesAppendedData>),

    /// Group membership changed
    MembershipChanged(Box<MembershipChangedData>),

    /// Leader changed in this group (added for tracking leadership)
    LeaderChanged {
        group_id: ConsensusGroupId,
        old_leader: Option<NodeId>,
        new_leader: Option<NodeId>,
    },
}

impl Event for GroupConsensusEvent {
    fn event_type() -> &'static str {
        "GroupConsensusEvent"
    }
}
