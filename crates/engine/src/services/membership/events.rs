//! Events emitted by the membership service (new event system)

use std::time::Duration;

use proven_topology::{Node, NodeId};

use crate::foundation::events::Event;

/// Events emitted by the membership service
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    /// Initial cluster formation completed
    ClusterFormed {
        members: Vec<NodeId>,
        coordinator: NodeId,
    },

    /// Node became reachable/online
    NodeOnline { node_id: NodeId, node_info: Node },

    /// Node became unreachable (might be temporary)
    NodeUnreachable { node_id: NodeId, last_seen_ms: u64 },

    /// Node confirmed offline (should be removed from consensus)
    NodeOffline {
        node_id: NodeId,
        offline_duration: Duration,
    },

    /// New node discovered and verified online
    NodeDiscovered { node_id: NodeId, node_info: Node },

    /// This node joined an existing cluster
    ClusterJoined {
        members: Vec<NodeId>,
        leader: NodeId,
    },

    /// Node announced graceful shutdown
    NodeGracefulShutdown {
        node_id: NodeId,
        reason: Option<String>,
    },

    /// Membership change should be applied
    MembershipChangeRequired {
        add_nodes: Vec<(NodeId, Node)>,
        remove_nodes: Vec<NodeId>,
        reason: String,
    },
}

impl Event for MembershipEvent {
    fn event_type() -> &'static str {
        "MembershipEvent"
    }
}
