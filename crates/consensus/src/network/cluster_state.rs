use crate::types::NodeId;

/// Reason for becoming cluster initiator
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitiatorReason {
    /// Single node in topology
    SingleNode,
    /// Discovery timeout expired
    DiscoveryTimeout,
}

/// Represents the current state of cluster membership
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterState {
    /// Currently discovering existing clusters or waiting for topology
    Discovering,

    /// Cluster initialization failed
    Failed {
        /// When the failure occurred
        failed_at: std::time::Instant,
        /// Error message
        error: String,
    },

    /// Became the cluster initiator (single node or timeout)
    Initiator {
        /// When became initiator
        initiated_at: std::time::Instant,
        /// Whether this was due to timeout or single-node topology
        reason: InitiatorReason,
    },

    /// Successfully joined an existing cluster
    Joined {
        /// When the cluster was joined
        joined_at: std::time::Instant,
        /// Number of nodes in the cluster when joined
        cluster_size: usize,
    },

    /// Transport started but cluster not yet initialized
    TransportReady,

    /// Waiting to join an existing cluster (sent join request)
    WaitingToJoin {
        /// When the join request was sent
        requested_at: std::time::Instant,
        /// Leader we sent the request to
        leader_id: NodeId,
    },
}
