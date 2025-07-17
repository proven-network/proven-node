//! Discovery manager for cluster service

use super::{
    coordinator::CoordinatorElection,
    protocol::DiscoveryProtocol,
    state_machine::{DiscoveryEvent, DiscoveryState},
};
use crate::error::ConsensusResult;
use crate::services::cluster::messages::{CLUSTER_NAMESPACE, DiscoveryRequest, DiscoveryResponse};
use crate::services::event::EventPublisher;

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use proven_network::NetworkManager;
use proven_topology::TopologyAdaptor;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Transport;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Type alias for cluster state tuple
type ClusterState = (bool, Option<u64>, Option<NodeId>, Option<usize>);
/// Type alias for optional cluster state reference
type OptionalClusterState = Arc<RwLock<Option<ClusterState>>>;

/// Configuration for discovery service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Discovery timeout
    pub discovery_timeout: Duration,
    /// Time between discovery rounds
    pub discovery_interval: Duration,
    /// Maximum discovery rounds before giving up
    pub max_discovery_rounds: u32,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            discovery_timeout: Duration::from_secs(15),
            discovery_interval: Duration::from_secs(1),
            max_discovery_rounds: 10,
        }
    }
}

/// Discovery manager for cluster formation
pub struct DiscoveryManager<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Local node ID
    local_node_id: NodeId,
    /// Network manager
    pub(crate) network: Arc<NetworkManager<T, G>>,
    /// Topology manager
    pub(crate) topology: Arc<TopologyManager<G>>,
    /// Discovery protocol
    protocol: DiscoveryProtocol<T, G>,
    /// Current discovery state
    state: Arc<RwLock<DiscoveryState>>,
    /// Discovery configuration
    config: DiscoveryConfig,
    /// Event publisher
    event_publisher: EventPublisher,
    /// Cluster state (shared with cluster service)
    pub(crate) cluster_state: OptionalClusterState,
}

impl<T, G> DiscoveryManager<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create new discovery service
    pub fn new(
        local_node_id: NodeId,
        network: Arc<NetworkManager<T, G>>,
        topology: Arc<TopologyManager<G>>,
        config: DiscoveryConfig,
        event_publisher: EventPublisher,
    ) -> Self {
        let protocol = DiscoveryProtocol::new(network.clone(), local_node_id.clone());

        Self {
            local_node_id,
            network,
            topology,
            protocol,
            state: Arc::new(RwLock::new(DiscoveryState::new())),
            config,
            event_publisher,
            cluster_state: Arc::new(RwLock::new(None)),
        }
    }

    /// Register namespace-based handlers
    pub async fn register_namespace_handlers(&self) -> ConsensusResult<()> {
        info!("Registering namespace handlers for discovery service");

        // Register namespace
        self.network.register_namespace(CLUSTER_NAMESPACE).await?;

        info!("Namespace '{}' registered", CLUSTER_NAMESPACE);

        // Register discovery request handler
        let local_node_id = self.local_node_id.clone();
        let cluster_state = self.cluster_state.clone();

        self.network
            .register_namespaced_request_handler::<DiscoveryRequest, DiscoveryResponse, _, _>(
                CLUSTER_NAMESPACE,
                "discovery_request",
                move |sender, _request| {
                    let local_node_id = local_node_id.clone();
                    let cluster_state = cluster_state.clone();

                    async move {
                        debug!("Handling discovery request from {}", sender);

                        // Check current cluster state
                        let state = cluster_state.read().await;
                        let (has_active_cluster, current_term, current_leader, cluster_size) =
                            if let Some((active, term, leader, size)) = &*state {
                                info!("Discovery handler: Have cluster state - active: {}, term: {:?}, leader: {:?}, size: {:?}", active, term, leader, size);
                                (*active, *term, leader.clone(), *size)
                            } else {
                                info!("Discovery handler: No cluster state yet");
                                (false, None, None, None)
                            };
                        drop(state);

                        // Return the response
                        Ok(DiscoveryResponse {
                            responder_id: local_node_id,
                            has_active_cluster,
                            current_term,
                            current_leader,
                            cluster_size,
                        })
                    }
                },
            )
            .await?;

        info!("Discovery request handler registered successfully");

        // Note: Join handler is now managed by ClusterService
        Ok(())
    }

    /// Update cluster state (called by cluster service when state changes)
    pub async fn update_cluster_state(
        &self,
        active: bool,
        term: Option<u64>,
        leader: Option<NodeId>,
        size: Option<usize>,
    ) {
        let mut state = self.cluster_state.write().await;
        *state = Some((active, term, leader.clone(), size));
        info!(
            "Updated discovery cluster state - active: {}, term: {:?}, leader: {:?}, size: {:?}",
            active, term, leader, size
        );
    }

    /// Start discovery process
    pub async fn start_discovery(&self) -> ConsensusResult<DiscoveryOutcome> {
        info!("Starting cluster discovery for node {}", self.local_node_id);

        // Update state to discovering
        {
            let mut state = self.state.write().await;
            *state = state.clone().apply_event(DiscoveryEvent::StartDiscovery {
                timeout: self.config.discovery_timeout,
            });
        }

        // Get all peers from topology
        let all_peers = self.topology.get_all_peers().await;
        let peer_ids: Vec<NodeId> = all_peers.iter().map(|n| n.node_id().clone()).collect();

        if peer_ids.is_empty() {
            info!("No peers found in topology, initializing as single-node cluster");
            return Ok(DiscoveryOutcome::SingleNode);
        }

        info!("Found {} peers in topology", peer_ids.len());

        // Execute discovery rounds
        let mut rounds_completed = 0;
        let start_time = std::time::Instant::now();

        while rounds_completed < self.config.max_discovery_rounds
            && start_time.elapsed() < self.config.discovery_timeout
        {
            // Execute discovery round
            let round_result = self
                .protocol
                .execute_discovery_round(peer_ids.clone(), self.config.discovery_interval)
                .await?;

            // Check if we found any clusters
            if !round_result.nodes_with_clusters.is_empty() {
                // Found cluster(s) - join the one with highest term
                let (leader_id, response) = round_result
                    .nodes_with_clusters
                    .into_iter()
                    .max_by_key(|(_, r)| r.current_term.unwrap_or(0))
                    .unwrap();

                info!(
                    "Found existing cluster led by {} (term: {:?})",
                    leader_id, response.current_term
                );

                // Update state to joining
                {
                    let mut state = self.state.write().await;
                    *state = DiscoveryState::JoiningCluster {
                        leader_id: leader_id.clone(),
                        requested_at: std::time::Instant::now(),
                    };
                }

                return Ok(DiscoveryOutcome::FoundCluster { leader_id });
            }

            rounds_completed += 1;

            // Wait before next round
            if rounds_completed < self.config.max_discovery_rounds {
                tokio::time::sleep(self.config.discovery_interval).await;
            }
        }

        // No clusters found - proceed to coordinator election
        info!(
            "No existing clusters found after {} rounds",
            rounds_completed
        );

        // Determine if we should become coordinator
        if CoordinatorElection::should_become_coordinator(&self.local_node_id, &peer_ids) {
            info!("Elected as coordinator, will initialize multi-node cluster");

            // Update state
            {
                let mut state = self.state.write().await;
                *state = state
                    .clone()
                    .apply_event(DiscoveryEvent::ElectedAsCoordinator {
                        peers: peer_ids.clone(),
                    });
            }

            Ok(DiscoveryOutcome::BecomeCoordinator { peers: peer_ids })
        } else {
            // Not elected - return the expected coordinator
            match CoordinatorElection::get_expected_coordinator(&peer_ids) {
                Some(coordinator) => {
                    info!(
                        "Not elected as coordinator, expected coordinator is: {}",
                        coordinator
                    );

                    // Return that we should join the coordinator
                    Ok(DiscoveryOutcome::ShouldJoinCoordinator {
                        coordinator,
                        peers: peer_ids,
                    })
                }
                None => {
                    warn!("Could not determine coordinator from peer list");
                    Ok(DiscoveryOutcome::RetryDiscovery)
                }
            }
        }
    }

    /// Get current discovery state
    pub async fn get_state(&self) -> DiscoveryState {
        self.state.read().await.clone()
    }

    /// Get current cluster state
    pub async fn get_cluster_state(&self) -> Option<ClusterState> {
        self.cluster_state.read().await.clone()
    }
}

/// Outcome of discovery process
#[derive(Debug, Clone)]
pub enum DiscoveryOutcome {
    /// No peers found - initialize as single node
    SingleNode,
    /// Found existing cluster to join
    FoundCluster { leader_id: NodeId },
    /// Elected as coordinator - initialize multi-node cluster
    BecomeCoordinator { peers: Vec<NodeId> },
    /// Should join the coordinator's cluster
    ShouldJoinCoordinator {
        coordinator: NodeId,
        peers: Vec<NodeId>,
    },
    /// Should retry discovery (non-coordinator waiting)
    RetryDiscovery,
}
