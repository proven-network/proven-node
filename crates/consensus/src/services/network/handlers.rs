//! Message handlers for network service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkResult;
use proven_topology::NodeId;

use crate::services::{cluster::ClusterStateInfo as ClusterState, monitoring::HealthResponse};

// ClusterDiscoveryRequest has been replaced by DiscoveryRequest in the discovery service

/// Handle health check request
pub async fn handle_health_check(
    cluster_state: Arc<RwLock<ClusterState>>,
) -> NetworkResult<HealthResponse> {
    let state = cluster_state.read().await;

    Ok(HealthResponse {
        healthy: true,
        leader: state.current_leader().cloned(),
    })
}
