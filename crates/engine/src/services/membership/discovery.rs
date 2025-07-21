//! Discovery protocol for membership service

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use uuid::Uuid;

use proven_network::NetworkManager;
use proven_topology::{Node, NodeId, TopologyAdaptor, TopologyManager};
use proven_transport::Transport;

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::services::membership::messages::{
    ClusterState, DiscoverClusterRequest, DiscoverClusterResponse, MembershipMessage,
    MembershipResponse, ProposeClusterRequest, ProposeClusterResponse,
};
use crate::services::membership::types::{ClusterFormationState, NodeStatus};

/// Result of discovery process
#[derive(Debug)]
pub enum DiscoveryResult {
    /// No cluster exists, we should coordinate formation
    ShouldCoordinate { online_peers: Vec<(NodeId, Node)> },
    /// No cluster exists, wait for coordinator
    WaitForCoordinator {
        coordinator: NodeId,
        online_peers: Vec<(NodeId, Node)>,
    },
    /// Active cluster found, should join
    JoinExistingCluster {
        leader: NodeId,
        members: Vec<NodeId>,
    },
    /// Cluster is forming, wait or join
    JoinFormingCluster {
        coordinator: NodeId,
        formation_id: Uuid,
    },
    /// Single node cluster
    SingleNode,
}

/// Discovery manager for cluster formation
pub struct DiscoveryManager<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    node_id: NodeId,
    network: Arc<NetworkManager<T, G>>,
    topology: Arc<TopologyManager<G>>,
    config: super::config::DiscoveryConfig,
    current_round: Arc<RwLock<Option<Uuid>>>,
}

impl<T, G> DiscoveryManager<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    pub fn new(
        node_id: NodeId,
        network: Arc<NetworkManager<T, G>>,
        topology: Arc<TopologyManager<G>>,
        config: super::config::DiscoveryConfig,
    ) -> Self {
        Self {
            node_id,
            network,
            topology,
            config,
            current_round: Arc::new(RwLock::new(None)),
        }
    }

    /// Execute discovery to determine cluster state
    pub async fn discover_cluster(&self) -> ConsensusResult<DiscoveryResult> {
        info!("Starting cluster discovery for node {}", self.node_id);

        let round_id = Uuid::new_v4();
        *self.current_round.write().await = Some(round_id);

        // Get all peers from topology
        let all_peers = self.topology.get_all_peers().await;
        if all_peers.is_empty() {
            info!("No peers found, initializing as single node");
            return Ok(DiscoveryResult::SingleNode);
        }

        // Execute discovery rounds
        let mut best_result = None;
        let start_time = std::time::Instant::now();

        for round in 0..self.config.max_discovery_rounds {
            if start_time.elapsed() > self.config.discovery_timeout {
                warn!("Discovery timeout reached after {} rounds", round);
                break;
            }

            info!(
                "Discovery round {}/{}",
                round + 1,
                self.config.max_discovery_rounds
            );

            // Query all peers
            let responses = self.query_peers(&all_peers, round_id).await?;

            // Analyze responses
            if let Some(result) = self.analyze_responses(responses, &all_peers).await? {
                best_result = Some(result);
                break;
            }

            // Wait before next round
            if round < self.config.max_discovery_rounds - 1 {
                tokio::time::sleep(self.config.discovery_interval).await;
            }
        }

        *self.current_round.write().await = None;

        best_result.ok_or_else(|| {
            Error::with_context(
                ErrorKind::Timeout,
                "Failed to complete discovery within timeout",
            )
        })
    }

    /// Query all peers for their cluster state
    async fn query_peers(
        &self,
        peers: &[Node],
        round_id: Uuid,
    ) -> ConsensusResult<HashMap<NodeId, DiscoverClusterResponse>> {
        let request = MembershipMessage::DiscoverCluster(DiscoverClusterRequest {
            round_id,
            timestamp: now_timestamp(),
        });

        let mut futures = Vec::new();
        for peer in peers {
            let network = self.network.clone();
            let peer_id = peer.node_id.clone();
            let request = request.clone();
            let timeout_duration = self.config.node_request_timeout;

            futures.push(async move {
                match timeout(
                    timeout_duration,
                    network.service_request(peer_id.clone(), request, timeout_duration),
                )
                .await
                {
                    Ok(Ok(MembershipResponse::DiscoverCluster(response))) => {
                        Some((peer_id, response))
                    }
                    Ok(Ok(_)) => {
                        warn!("Unexpected response type from {}", peer_id);
                        None
                    }
                    Ok(Err(e)) => {
                        debug!("Failed to query {}: {}", peer_id, e);
                        None
                    }
                    Err(_) => {
                        debug!("Query to {} timed out", peer_id);
                        None
                    }
                }
            });
        }

        let results = join_all(futures).await;
        let responses: HashMap<_, _> = results.into_iter().flatten().collect();

        info!(
            "Discovery query complete: {}/{} nodes responded",
            responses.len(),
            peers.len()
        );

        Ok(responses)
    }

    /// Analyze discovery responses to determine action
    async fn analyze_responses(
        &self,
        responses: HashMap<NodeId, DiscoverClusterResponse>,
        all_peers: &[Node],
    ) -> ConsensusResult<Option<DiscoveryResult>> {
        // Check response ratio
        let response_ratio = responses.len() as f64 / all_peers.len() as f64;
        if response_ratio < self.config.min_response_ratio
            || responses.len() < self.config.min_responding_nodes
        {
            info!(
                "Insufficient responses: {} nodes ({:.1}%), need {} nodes ({:.1}%)",
                responses.len(),
                response_ratio * 100.0,
                self.config.min_responding_nodes,
                self.config.min_response_ratio * 100.0
            );
            return Ok(None);
        }

        // Log what each peer reported
        for (peer_id, response) in &responses {
            info!(
                "Peer {} reported: status={:?}, cluster_state={:?}",
                peer_id, response.node_status, response.cluster_state
            );
        }

        // Look for existing clusters
        let active_clusters: Vec<_> = responses
            .iter()
            .filter_map(|(id, r)| match &r.cluster_state {
                ClusterState::Active {
                    leader,
                    members,
                    term,
                    ..
                } => Some((id, leader.clone(), members.clone(), *term)),
                _ => None,
            })
            .collect();

        if !active_clusters.is_empty() {
            // If ANY cluster exists, we should never form a new one
            // Find the cluster with the highest term
            let (node_id, leader, members, term) = active_clusters
                .into_iter()
                .max_by_key(|(_, _, _, term)| *term)
                .unwrap();

            if let Some(leader) = leader {
                info!("Found active cluster with leader {}", leader);
                return Ok(Some(DiscoveryResult::JoinExistingCluster {
                    leader,
                    members,
                }));
            } else {
                // Cluster exists but has no leader - we should wait and retry
                // The existing cluster needs to elect a leader first
                info!(
                    "Found active cluster from {} with {} members but no leader (term {}), waiting for leader election",
                    node_id,
                    members.len(),
                    term
                );

                // Return None to trigger a retry in the next discovery round
                return Ok(None);
            }
        }

        // Look for forming clusters
        let forming_clusters: Vec<_> = responses
            .iter()
            .filter_map(|(_, r)| match &r.cluster_state {
                ClusterState::Forming {
                    coordinator,
                    formation_id,
                    ..
                } => Some((coordinator.clone(), *formation_id)),
                _ => None,
            })
            .collect();

        if !forming_clusters.is_empty() {
            let (coordinator, formation_id) = forming_clusters[0].clone();
            info!("Found forming cluster with coordinator {}", coordinator);
            return Ok(Some(DiscoveryResult::JoinFormingCluster {
                coordinator,
                formation_id,
            }));
        }

        // No active cluster exists - prepare to form one with all online nodes

        // Collect all online nodes (regardless of their cluster state)
        let online_nodes: Vec<(NodeId, Node)> = responses
            .iter()
            .filter(|(_, r)| r.node_status == NodeStatus::Online)
            .filter_map(|(id, _)| {
                all_peers
                    .iter()
                    .find(|p| &p.node_id == id)
                    .map(|p| (id.clone(), p.clone()))
            })
            .collect();

        // Include ourselves in the online nodes list
        let mut all_online_nodes = online_nodes.clone();
        if let Some(our_node) = all_peers.iter().find(|n| n.node_id == self.node_id) {
            all_online_nodes.push((self.node_id.clone(), our_node.clone()));
        }

        info!(
            "Found {} total online nodes (including ourselves)",
            all_online_nodes.len()
        );

        // Check if we're truly alone in the topology
        if all_online_nodes.len() == 1 && all_online_nodes[0].0 == self.node_id {
            info!("We are the only node in the topology, forming single-node cluster");
            return Ok(Some(DiscoveryResult::SingleNode));
        }

        // Sort by node ID for deterministic coordinator selection
        all_online_nodes.sort_by(|a, b| a.0.cmp(&b.0));

        let coordinator = &all_online_nodes[0].0;
        if coordinator == &self.node_id {
            info!(
                "We are the coordinator for cluster formation with {} nodes",
                all_online_nodes.len()
            );
            // Remove ourselves from the online_peers list as we'll be added separately
            let peers_without_us: Vec<(NodeId, Node)> = all_online_nodes
                .into_iter()
                .filter(|(id, _)| id != &self.node_id)
                .collect();

            Ok(Some(DiscoveryResult::ShouldCoordinate {
                online_peers: peers_without_us,
            }))
        } else {
            info!("Node {} will coordinate cluster formation", coordinator);
            Ok(Some(DiscoveryResult::WaitForCoordinator {
                coordinator: coordinator.clone(),
                online_peers: online_nodes,
            }))
        }
    }
}

/// Get current timestamp in milliseconds
fn now_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
