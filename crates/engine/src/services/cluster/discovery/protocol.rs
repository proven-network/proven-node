//! Discovery protocol implementation

use crate::error::ConsensusResult;
use crate::services::cluster::messages::{
    ClusterServiceMessage, ClusterServiceResponse, DiscoveryRound,
};

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// Discovery protocol implementation
pub struct DiscoveryProtocol<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    network: Arc<NetworkManager<T, G>>,
    local_node_id: NodeId,
}

impl<T, G> DiscoveryProtocol<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create new discovery protocol
    pub fn new(network: Arc<NetworkManager<T, G>>, local_node_id: NodeId) -> Self {
        Self {
            network,
            local_node_id,
        }
    }

    /// Execute a discovery round
    pub async fn execute_discovery_round(
        &self,
        peers: Vec<NodeId>,
        round_timeout: Duration,
        node_request_timeout: Duration,
        enable_retry: bool,
        max_retries: u32,
    ) -> ConsensusResult<DiscoveryRound> {
        info!(
            "Starting discovery round with {} peers, timeout: {:?}, node timeout: {:?}",
            peers.len(),
            round_timeout,
            node_request_timeout
        );

        let started_at = std::time::Instant::now();
        let mut responding_nodes = Vec::new();
        let mut nodes_with_clusters = Vec::new();

        // Send discovery requests to all peers in parallel
        let mut discovery_futures = Vec::new();
        let peers_len = peers.len();
        for peer_id in peers {
            let network = self.network.clone();
            let local_id = self.local_node_id.clone();

            let fut = async move {
                let request = ClusterServiceMessage::Discovery {
                    requester_id: local_id,
                };

                debug!("Sending discovery request to peer {}", peer_id);

                // Try with retries if enabled
                let mut attempts = 0;
                let max_attempts = if enable_retry { max_retries + 1 } else { 1 };

                while attempts < max_attempts {
                    let retry_timeout = if attempts > 0 {
                        // Exponential backoff for retries
                        node_request_timeout + Duration::from_millis(500 * (1 << (attempts - 1)))
                    } else {
                        node_request_timeout
                    };

                    match timeout(
                        retry_timeout,
                        network.request_service(peer_id.clone(), request.clone(), retry_timeout),
                    )
                    .await
                    {
                        Ok(Ok(response)) => {
                            if attempts > 0 {
                                debug!(
                                    "Discovery request to {} succeeded after {} retries",
                                    peer_id, attempts
                                );
                            }
                            return Some((peer_id, response));
                        }
                        Ok(Err(e)) => {
                            if attempts + 1 < max_attempts {
                                debug!(
                                    "Discovery request to {} failed (attempt {}): {}, retrying...",
                                    peer_id,
                                    attempts + 1,
                                    e
                                );
                            } else {
                                warn!(
                                    "Discovery request to {} failed after {} attempts: {}",
                                    peer_id,
                                    attempts + 1,
                                    e
                                );
                            }
                        }
                        Err(_) => {
                            if attempts + 1 < max_attempts {
                                debug!(
                                    "Discovery request to {} timed out (attempt {}), retrying...",
                                    peer_id,
                                    attempts + 1
                                );
                            } else {
                                warn!(
                                    "Discovery request to {} timed out after {} attempts",
                                    peer_id,
                                    attempts + 1
                                );
                            }
                        }
                    }

                    attempts += 1;
                }

                None
            };

            discovery_futures.push(fut);
        }

        // Wait for all responses
        let responses = join_all(discovery_futures).await;

        // Process responses
        for response in responses.into_iter().flatten() {
            let (peer_id, service_response) = response;
            responding_nodes.push(peer_id.clone());

            match &service_response {
                ClusterServiceResponse::Discovery {
                    responder_id: _,
                    has_active_cluster,
                    current_term,
                    current_leader,
                    cluster_size,
                } => {
                    if *has_active_cluster {
                        debug!(
                            "Node {} has active cluster (term: {:?}, leader: {:?}, size: {:?})",
                            peer_id, current_term, current_leader, cluster_size
                        );
                        nodes_with_clusters.push((peer_id, service_response));
                    }
                }
                _ => {
                    warn!("Unexpected response type from {}", peer_id);
                }
            }
        }

        info!(
            "Discovery round complete: {}/{} nodes responded, {} have active clusters",
            responding_nodes.len(),
            peers_len,
            nodes_with_clusters.len()
        );

        Ok(DiscoveryRound {
            responding_nodes,
            nodes_with_clusters,
            started_at,
        })
    }
}
