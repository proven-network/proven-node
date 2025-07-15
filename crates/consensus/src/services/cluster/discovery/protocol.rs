//! Discovery protocol implementation

use crate::error::ConsensusResult;
use crate::services::cluster::messages::{
    CLUSTER_NAMESPACE, DiscoveryRequest, DiscoveryResponse, DiscoveryRound,
};

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_transport::Transport;
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// Discovery protocol implementation
pub struct DiscoveryProtocol<T, G>
where
    T: Transport,
    G: Governance,
{
    network: Arc<NetworkManager<T, G>>,
    local_node_id: NodeId,
}

impl<T, G> DiscoveryProtocol<T, G>
where
    T: Transport,
    G: Governance,
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
    ) -> ConsensusResult<DiscoveryRound> {
        info!(
            "Starting discovery round with {} peers, timeout: {:?}",
            peers.len(),
            round_timeout
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
                let request = DiscoveryRequest {
                    requester_id: local_id,
                };

                debug!("Sending discovery request to peer {}", peer_id);

                match timeout(
                    Duration::from_secs(10),
                    network.request_namespaced::<DiscoveryRequest, DiscoveryResponse>(
                        CLUSTER_NAMESPACE,
                        peer_id.clone(),
                        request,
                        Duration::from_secs(10),
                    ),
                )
                .await
                {
                    Ok(Ok(response)) => Some((peer_id, response)),
                    Ok(Err(e)) => {
                        warn!("Discovery request to {} failed: {}", peer_id, e);
                        None
                    }
                    Err(_) => {
                        warn!("Discovery request to {} timed out", peer_id);
                        None
                    }
                }
            };

            discovery_futures.push(fut);
        }

        // Wait for all responses
        let responses = join_all(discovery_futures).await;

        // Process responses
        for response in responses.into_iter().flatten() {
            let (peer_id, discovery_response) = response;
            responding_nodes.push(peer_id.clone());

            if discovery_response.has_active_cluster {
                debug!(
                    "Node {} has active cluster (term: {:?}, leader: {:?}, size: {:?})",
                    peer_id,
                    discovery_response.current_term,
                    discovery_response.current_leader,
                    discovery_response.cluster_size
                );
                nodes_with_clusters.push((peer_id, discovery_response));
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

    /// Send discovery response to a requester
    pub async fn send_discovery_response(
        &self,
        requester_id: NodeId,
        has_active_cluster: bool,
        current_term: Option<u64>,
        current_leader: Option<NodeId>,
        cluster_size: Option<usize>,
    ) -> ConsensusResult<()> {
        let response = DiscoveryResponse {
            responder_id: self.local_node_id.clone(),
            has_active_cluster,
            current_term,
            current_leader,
            cluster_size,
        };

        self.network
            .send(requester_id.clone(), response)
            .await
            .map_err(|e| {
                crate::error::ConsensusError::network(format!(
                    "Failed to send discovery response to {requester_id}: {e}"
                ))
            })
    }
}
