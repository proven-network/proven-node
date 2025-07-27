//! Health monitoring for membership service

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proven_attestation::Attestor;
use tokio::sync::RwLock;
use tokio::time::{interval, timeout};
use tracing::{debug, info, warn};

use proven_network::NetworkManager;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

use crate::error::ConsensusResult;
use crate::foundation::NodeStatus;
use crate::services::membership::messages::{
    GlobalConsensusInfo, HealthCheckRequest, HealthCheckResponse, MembershipMessage,
    MembershipResponse,
};
use crate::services::membership::types::MembershipView;

/// Health monitor for cluster members
pub struct HealthMonitor<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    node_id: NodeId,
    network_manager: Arc<NetworkManager<T, G, A>>,
    config: super::config::HealthConfig,
    health_sequence: Arc<RwLock<u64>>,
}

impl<T, G, A> HealthMonitor<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    pub fn new(
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G, A>>,
        config: super::config::HealthConfig,
    ) -> Self {
        Self {
            node_id,
            network_manager,
            config,
            health_sequence: Arc::new(RwLock::new(0)),
        }
    }

    /// Start health monitoring loop
    pub async fn start_monitoring(
        self: Arc<Self>,
        membership_view: Arc<RwLock<MembershipView>>,
        event_bus: Arc<crate::foundation::events::EventBus>,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) {
        let mut check_interval = interval(self.config.health_check_interval);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    if let Err(e) = self.check_all_members(&membership_view, &event_bus).await {
                        warn!("Health check round failed: {}", e);
                    }
                }
                _ = cancellation_token.cancelled() => {
                    info!("Health monitoring stopped");
                    break;
                }
            }
        }
    }

    /// Check health of all known members
    async fn check_all_members(
        &self,
        membership_view: &Arc<RwLock<MembershipView>>,
        event_bus: &Arc<crate::foundation::events::EventBus>,
    ) -> ConsensusResult<()> {
        let nodes_to_check: Vec<NodeId> = {
            let view = membership_view.read().await;
            view.nodes
                .iter()
                .filter(|(id, _)| *id != &self.node_id)
                .filter(|(_, member)| {
                    matches!(
                        member.status,
                        NodeStatus::Online | NodeStatus::Unreachable { .. }
                    )
                })
                .map(|(id, _)| id.clone())
                .collect()
        };

        if nodes_to_check.is_empty() {
            return Ok(());
        }

        debug!("Health checking {} nodes", nodes_to_check.len());

        // Check all nodes concurrently
        let mut handles = Vec::new();
        for node_id in nodes_to_check {
            let network_manager = self.network_manager.clone();
            let sequence = self.next_sequence().await;
            let timeout_duration = self.config.health_check_timeout;

            handles.push(tokio::spawn(async move {
                let request = MembershipMessage::HealthCheck(HealthCheckRequest {
                    sequence,
                    timestamp: now_timestamp(),
                });

                let start = std::time::Instant::now();
                let result = timeout(
                    timeout_duration,
                    network_manager.request_with_timeout(
                        node_id.clone(),
                        request,
                        timeout_duration,
                    ),
                )
                .await;

                let latency_ms = start.elapsed().as_millis() as u32;

                (node_id, result, latency_ms)
            }));
        }

        // Process results
        let results = futures::future::join_all(handles).await;
        let mut health_updates = HashMap::new();

        for result in results {
            match result {
                Ok((node_id, Ok(Ok(MembershipResponse::HealthCheck(response))), latency_ms)) => {
                    health_updates.insert(
                        node_id,
                        HealthCheckResult::Success(Box::new(HealthCheckSuccess {
                            response,
                            latency_ms,
                        })),
                    );
                }
                Ok((node_id, _, _)) => {
                    health_updates.insert(node_id, HealthCheckResult::Failed);
                }
                Err(e) => {
                    warn!("Health check task failed: {}", e);
                }
            }
        }

        // Update membership view with health results
        self.update_health_status(membership_view, health_updates, event_bus)
            .await;

        Ok(())
    }

    /// Update health status based on check results
    async fn update_health_status(
        &self,
        membership_view: &Arc<RwLock<MembershipView>>,
        results: HashMap<NodeId, HealthCheckResult>,
        event_bus: &Arc<crate::foundation::events::EventBus>,
    ) {
        let mut view = membership_view.write().await;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        for (node_id, result) in results {
            if let Some(member) = view.nodes.get_mut(&node_id) {
                match result {
                    HealthCheckResult::Success(success) => {
                        let response = success.response;
                        let latency_ms = success.latency_ms;
                        // Update health info
                        member.health.last_check_ms = now_ms;
                        member.health.consecutive_failures = 0;
                        member.health.latency_ms = Some(latency_ms);
                        member.health.load = response.load;
                        member.last_seen_ms = now_ms;

                        // Update status if needed
                        match member.status {
                            NodeStatus::Unreachable { .. } => {
                                info!("Node {} is back online", node_id);
                                member.status = NodeStatus::Online;
                            }
                            NodeStatus::Online => {
                                // Still online
                            }
                            _ => {}
                        }

                        // Update roles based on consensus info
                        if let Some(consensus_info) = response.global_consensus_info {
                            self.update_consensus_roles(member, consensus_info);
                        }
                    }
                    HealthCheckResult::Failed => {
                        member.health.consecutive_failures += 1;
                        member.health.last_check_ms = now_ms;

                        // Check if we should mark as unreachable
                        if member.health.consecutive_failures >= self.config.unreachable_threshold {
                            match member.status {
                                NodeStatus::Online => {
                                    warn!(
                                        "Node {} is now unreachable (failed {} checks)",
                                        node_id, member.health.consecutive_failures
                                    );
                                    member.status = NodeStatus::Unreachable { since_ms: now_ms };
                                }
                                NodeStatus::Unreachable { since_ms } => {
                                    // Check if should mark as offline
                                    let duration_ms = now_ms.saturating_sub(since_ms);
                                    if Duration::from_millis(duration_ms)
                                        > self.config.offline_threshold
                                    {
                                        warn!(
                                            "Node {} is now offline (unreachable for {:?})",
                                            node_id,
                                            Duration::from_millis(duration_ms)
                                        );
                                        member.status = NodeStatus::Offline { since_ms: now_ms };

                                        // Publish event for membership change
                                        use crate::services::membership::events::MembershipEvent;

                                        event_bus.emit(MembershipEvent::MembershipChangeRequired {
                                            add_nodes: vec![],
                                            remove_nodes: vec![node_id.clone()],
                                            reason: "Node offline due to failed health checks"
                                                .to_string(),
                                        });

                                        // Update global consensus membership to remove the failed node
                                        info!(
                                            "Updating global consensus membership to remove failed node {}",
                                            node_id
                                        );

                                        use crate::services::global_consensus::commands::RemoveNodeFromConsensus;
                                        let remove_node_cmd = RemoveNodeFromConsensus {
                                            node_id: node_id.clone(),
                                        };

                                        let event_bus_clone = event_bus.clone();
                                        let node_id_clone = node_id.clone();
                                        tokio::spawn(async move {
                                            match event_bus_clone.request(remove_node_cmd).await {
                                                Ok(members) => {
                                                    info!(
                                                        "Successfully removed failed node {} from global consensus membership. Current members: {:?}",
                                                        node_id_clone, members
                                                    );
                                                }
                                                Err(e) => {
                                                    warn!(
                                                        "Failed to remove failed node {} from global consensus membership: {}",
                                                        node_id_clone, e
                                                    );
                                                    // Continue anyway - the membership monitor will retry
                                                }
                                            }
                                        });
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        view.last_updated_ms = now_ms;
    }

    /// Update node roles based on consensus info
    fn update_consensus_roles(
        &self,
        member: &mut crate::services::membership::types::NodeMembership,
        info: GlobalConsensusInfo,
    ) {
        use crate::foundation::NodeRole;

        // Clear existing global consensus roles
        member.roles.retain(|r| {
            !matches!(
                r,
                NodeRole::GlobalConsensusLeader | NodeRole::GlobalConsensusMember
            )
        });

        if info.is_member {
            if info.current_leader == Some(member.node_id.clone()) {
                member.roles.insert(NodeRole::GlobalConsensusLeader);
            } else {
                member.roles.insert(NodeRole::GlobalConsensusMember);
            }
        }
    }

    /// Get next health check sequence number
    async fn next_sequence(&self) -> u64 {
        let mut seq = self.health_sequence.write().await;
        *seq += 1;
        *seq
    }
}

/// Result of a health check
enum HealthCheckResult {
    Success(Box<HealthCheckSuccess>),
    Failed,
}

struct HealthCheckSuccess {
    response: HealthCheckResponse,
    latency_ms: u32,
}

/// Get current timestamp in milliseconds
fn now_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
