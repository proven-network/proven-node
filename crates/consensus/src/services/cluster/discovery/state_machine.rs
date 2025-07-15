//! State machine for cluster discovery process

use proven_topology::NodeId;
use std::time::{Duration, Instant};

/// Discovery state machine states
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryState {
    /// Initial state - not started
    Idle,
    /// Currently discovering clusters
    Discovering {
        /// When discovery started
        started_at: Instant,
        /// Discovery timeout
        timeout: Duration,
        /// Number of discovery rounds completed
        rounds_completed: u32,
    },
    /// Waiting to be elected as coordinator
    WaitingForElection {
        /// When we started waiting
        started_at: Instant,
        /// List of all known peers
        peers: Vec<NodeId>,
    },
    /// Elected as coordinator - will initialize cluster
    ElectedCoordinator {
        /// Peers that will join our cluster
        peers_to_join: Vec<NodeId>,
    },
    /// Found existing cluster - attempting to join
    JoiningCluster {
        /// Leader of the cluster we're joining
        leader_id: NodeId,
        /// When we sent the join request
        requested_at: Instant,
    },
    /// Successfully joined a cluster
    Joined {
        /// Leader of the cluster
        leader_id: NodeId,
        /// Size of the cluster
        cluster_size: usize,
    },
    /// Failed to discover or join - will become single node
    Failed {
        /// Reason for failure
        reason: String,
    },
}

/// Events that can trigger state transitions
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// Start discovery process
    StartDiscovery { timeout: Duration },
    /// Discovery round completed
    DiscoveryRoundComplete {
        found_clusters: Vec<(NodeId, u64)>, // (node_id, term)
        responding_peers: Vec<NodeId>,
    },
    /// Discovery timeout reached
    DiscoveryTimeout,
    /// Elected as coordinator
    ElectedAsCoordinator { peers: Vec<NodeId> },
    /// Received join approval
    JoinApproved {
        leader_id: NodeId,
        cluster_size: usize,
    },
    /// Join request rejected
    JoinRejected { reason: String },
    /// General failure
    Failed { reason: String },
}

impl DiscoveryState {
    /// Create new idle state
    pub fn new() -> Self {
        DiscoveryState::Idle
    }

    /// Check if discovery is complete
    pub fn is_complete(&self) -> bool {
        matches!(
            self,
            DiscoveryState::Joined { .. }
                | DiscoveryState::ElectedCoordinator { .. }
                | DiscoveryState::Failed { .. }
        )
    }

    /// Check if discovery is in progress
    pub fn is_discovering(&self) -> bool {
        matches!(self, DiscoveryState::Discovering { .. })
    }

    /// Apply event to current state
    pub fn apply_event(self, event: DiscoveryEvent) -> Self {
        match (self, event) {
            // Start discovery from idle
            (DiscoveryState::Idle, DiscoveryEvent::StartDiscovery { timeout }) => {
                DiscoveryState::Discovering {
                    started_at: Instant::now(),
                    timeout,
                    rounds_completed: 0,
                }
            }

            // Discovery round completed
            (
                DiscoveryState::Discovering {
                    started_at,
                    timeout,
                    rounds_completed,
                },
                DiscoveryEvent::DiscoveryRoundComplete {
                    found_clusters,
                    responding_peers: _,
                },
            ) => {
                if !found_clusters.is_empty() {
                    // Found cluster(s) - join the one with highest term
                    let (leader_id, _term) = found_clusters
                        .into_iter()
                        .max_by_key(|(_, term)| *term)
                        .unwrap();
                    DiscoveryState::JoiningCluster {
                        leader_id,
                        requested_at: Instant::now(),
                    }
                } else {
                    // No clusters found yet
                    DiscoveryState::Discovering {
                        started_at,
                        timeout,
                        rounds_completed: rounds_completed + 1,
                    }
                }
            }

            // Discovery timeout - proceed to election
            (DiscoveryState::Discovering { .. }, DiscoveryEvent::DiscoveryTimeout) => {
                DiscoveryState::WaitingForElection {
                    started_at: Instant::now(),
                    peers: vec![],
                }
            }

            // Elected as coordinator
            (
                DiscoveryState::WaitingForElection { .. },
                DiscoveryEvent::ElectedAsCoordinator { peers },
            ) => DiscoveryState::ElectedCoordinator {
                peers_to_join: peers,
            },

            // Join approved
            (
                DiscoveryState::JoiningCluster { .. },
                DiscoveryEvent::JoinApproved {
                    leader_id,
                    cluster_size,
                },
            ) => DiscoveryState::Joined {
                leader_id,
                cluster_size,
            },

            // Join rejected
            (DiscoveryState::JoiningCluster { .. }, DiscoveryEvent::JoinRejected { reason }) => {
                DiscoveryState::Failed { reason }
            }

            // General failure
            (_, DiscoveryEvent::Failed { reason }) => DiscoveryState::Failed { reason },

            // Invalid transitions - stay in current state
            (state, _) => state,
        }
    }
}

impl Default for DiscoveryState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_state_transitions() {
        // Start from idle
        let state = DiscoveryState::new();
        assert_eq!(state, DiscoveryState::Idle);

        // Start discovery
        let state = state.apply_event(DiscoveryEvent::StartDiscovery {
            timeout: Duration::from_secs(15),
        });
        assert!(matches!(state, DiscoveryState::Discovering { .. }));

        // Found clusters
        let state = state.apply_event(DiscoveryEvent::DiscoveryRoundComplete {
            found_clusters: vec![(NodeId::from_seed(1), 1)],
            responding_peers: vec![],
        });
        assert!(matches!(state, DiscoveryState::JoiningCluster { .. }));

        // Join approved
        let state = state.apply_event(DiscoveryEvent::JoinApproved {
            leader_id: NodeId::from_seed(1),
            cluster_size: 3,
        });
        assert!(matches!(state, DiscoveryState::Joined { .. }));
        assert!(state.is_complete());
    }
}
