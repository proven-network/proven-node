//! Events emitted by the group consensus service

use std::num::NonZero;

use proven_topology::NodeId;

use crate::consensus::group::types::MessageData;
use crate::foundation::types::ConsensusGroupId;
use crate::services::event::traits::ServiceEvent;
use std::sync::Arc;

/// Messages appended event details
#[derive(Debug, Clone)]
pub struct MessagesAppendedData {
    pub group_id: ConsensusGroupId,
    pub stream_name: String,
    pub message_count: usize,
}

/// Messages to persist event - includes pre-serialized entries for zero-copy transfer
#[derive(Debug, Clone)]
pub struct MessagesToPersist {
    pub stream_name: String,
    /// Pre-serialized entries ready for storage
    pub entries: Arc<Vec<bytes::Bytes>>,
}

impl MessagesToPersist {
    /// Consume the event and return the pre-serialized entries for storage append
    pub fn into_bytes(self) -> Arc<Vec<bytes::Bytes>> {
        self.entries
    }
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

    /// Messages to persist - includes actual message data
    MessagesToPersist(Box<MessagesToPersist>),

    /// Group membership changed
    MembershipChanged(Box<MembershipChangedData>),

    /// Leader changed in this group (added for tracking leadership)
    LeaderChanged {
        group_id: ConsensusGroupId,
        old_leader: Option<NodeId>,
        new_leader: Option<NodeId>,
    },
}

impl ServiceEvent for GroupConsensusEvent {
    fn event_name(&self) -> &'static str {
        match self {
            Self::StateSynchronized { .. } => "GroupConsensusEvent::StateSynchronized",
            Self::StreamCreated { .. } => "GroupConsensusEvent::StreamCreated",
            Self::StreamRemoved { .. } => "GroupConsensusEvent::StreamRemoved",
            Self::MessagesAppended(_) => "GroupConsensusEvent::MessagesAppended",
            Self::MessagesToPersist(_) => "GroupConsensusEvent::MessagesToPersist",
            Self::MembershipChanged(_) => "GroupConsensusEvent::MembershipChanged",
            Self::LeaderChanged { .. } => "GroupConsensusEvent::LeaderChanged",
        }
    }
}
