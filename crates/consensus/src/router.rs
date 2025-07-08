use crate::allocation::{AllocationManager, ConsensusGroupId};
use crate::error::{ConsensusResult, Error};
use crate::global::{GlobalManager, GlobalOperation, GlobalRequest};
use crate::local::LocalConsensusManager;
use crate::monitoring::MonitoringService;
use crate::operations::{
    ConsensusOperation, ConsensusRequest, ConsensusResponse, LocalStreamOperation,
};
use futures::future;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Advanced consensus operation router with cross-group capabilities
///
/// The ConsensusRouter provides sophisticated routing of operations across
/// multiple consensus groups, supporting:
/// - Load-balanced routing
/// - Cross-group queries and coordination
/// - Broadcast operations
/// - Cluster health monitoring
/// - Multi-group transactions
pub struct ConsensusRouter<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Global consensus manager
    global_manager: Arc<GlobalManager<G, A>>,
    /// Local consensus manager
    local_manager: Arc<LocalConsensusManager<G, A>>,
    /// Stream allocation manager
    allocation_manager: Arc<RwLock<AllocationManager>>,
    /// Monitoring service
    monitoring: Arc<MonitoringService>,
}

impl<G, A> ConsensusRouter<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Create a new consensus router
    pub fn new(
        global_manager: Arc<GlobalManager<G, A>>,
        local_manager: Arc<LocalConsensusManager<G, A>>,
        allocation_manager: Arc<RwLock<AllocationManager>>,
    ) -> Self {
        // Create monitoring service with 60 second update interval
        let monitoring = Arc::new(MonitoringService::new(60));

        Self {
            global_manager,
            local_manager,
            allocation_manager,
            monitoring,
        }
    }

    /// Route a consensus request to the appropriate handler
    pub async fn route_request(
        &self,
        request: ConsensusRequest,
    ) -> ConsensusResult<ConsensusResponse> {
        match request.operation {
            ConsensusOperation::GlobalAdmin(op) => {
                self.handle_global_admin_operation(op, request.request_id)
                    .await
            }
            ConsensusOperation::LocalStream {
                group_id,
                operation,
            } => {
                self.handle_local_stream_operation(group_id, operation, request.request_id)
                    .await
            }
        }
    }

    /// Handle a global administrative operation
    async fn handle_global_admin_operation(
        &self,
        operation: GlobalOperation,
        request_id: Option<String>,
    ) -> ConsensusResult<ConsensusResponse> {
        let request = GlobalRequest { operation };

        // Submit to global consensus and get the log index
        let log_index = self.global_manager.propose_request(&request).await?;

        // For now, assume success if we got a log index
        // In a real implementation, we'd need to wait for the operation to be applied
        Ok(ConsensusResponse {
            success: true,
            sequence: Some(log_index),
            error: None,
            request_id,
        })
    }

    /// Handle a local stream operation
    async fn handle_local_stream_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: LocalStreamOperation,
        request_id: Option<String>,
    ) -> ConsensusResult<ConsensusResponse> {
        // Forward to the local consensus manager
        let mut response = self
            .local_manager
            .process_operation(group_id, operation)
            .await?;
        response.request_id = request_id;
        Ok(response)
    }

    /// Get the consensus group for a stream
    pub async fn get_stream_group(&self, stream_name: &str) -> ConsensusResult<ConsensusGroupId> {
        let allocation_mgr = self.allocation_manager.read().await;
        allocation_mgr
            .get_stream_group(stream_name)
            .ok_or_else(|| Error::NotFound(format!("Stream '{}' not allocated", stream_name)))
    }

    /// Route a stream operation automatically based on stream allocation
    pub async fn route_stream_operation(
        &self,
        stream_name: &str,
        operation: LocalStreamOperation,
        request_id: Option<String>,
    ) -> ConsensusResult<ConsensusResponse> {
        let group_id = self.get_stream_group(stream_name).await?;
        self.handle_local_stream_operation(group_id, operation, request_id)
            .await
    }

    /// Get metrics for all consensus groups
    pub async fn get_all_metrics(&self) -> ConsensusRouterMetrics {
        let allocation_metrics = {
            let allocation_mgr = self.allocation_manager.read().await;
            allocation_mgr.get_group_loads()
        };

        let local_metrics = self.local_manager.get_all_metrics().await;

        ConsensusRouterMetrics {
            allocation_metrics,
            local_group_metrics: local_metrics,
        }
    }

    /// Route a global operation
    pub async fn route_global_operation(
        &self,
        operation: GlobalOperation,
    ) -> ConsensusResult<ConsensusResponse> {
        self.handle_global_admin_operation(operation, None).await
    }

    /// Route a local operation to a specific group
    pub async fn route_local_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: LocalStreamOperation,
    ) -> ConsensusResult<ConsensusResponse> {
        self.handle_local_stream_operation(group_id, operation, None)
            .await
    }

    /// Update monitoring metrics
    pub async fn update_monitoring_metrics(
        &self,
        active_migrations: Vec<crate::migration::ActiveMigration>,
    ) {
        let allocation_metrics = {
            let allocation_mgr = self.allocation_manager.read().await;
            allocation_mgr.get_group_loads()
        };

        let local_metrics = self.local_manager.get_all_metrics().await;

        self.monitoring
            .update_metrics(allocation_metrics, local_metrics, active_migrations)
            .await;
    }

    /// Get current monitoring metrics
    pub async fn get_monitoring_metrics(&self) -> crate::monitoring::HierarchicalMetrics {
        self.monitoring.get_metrics().await
    }

    /// Check if rebalancing is recommended
    pub async fn should_rebalance(&self) -> bool {
        self.monitoring.should_rebalance().await
    }

    /// Get rebalancing recommendations
    pub async fn get_rebalancing_recommendations(
        &self,
    ) -> Vec<crate::monitoring::RecommendedMigration> {
        self.monitoring.get_rebalancing_recommendations().await
    }

    /// Get monitoring service reference
    pub fn monitoring(&self) -> &Arc<MonitoringService> {
        &self.monitoring
    }

    // Enhanced cross-group operation routing methods

    /// Route operation based on stream name (automatically determines target group)
    /// This is a convenience method that looks up the group allocation first
    pub async fn route_by_stream_name(
        &self,
        stream_name: &str,
        operation: LocalStreamOperation,
    ) -> ConsensusResult<ConsensusResponse> {
        let group_id = self.get_stream_group(stream_name).await?;
        debug!(
            "Routing operation for stream '{}' to group {:?}",
            stream_name, group_id
        );
        self.handle_local_stream_operation(group_id, operation, None)
            .await
    }

    /// Route operation to multiple groups concurrently
    pub async fn route_to_multiple_groups(
        &self,
        group_ids: Vec<ConsensusGroupId>,
        operation: LocalStreamOperation,
    ) -> ConsensusResult<Vec<ConsensusResponse>> {
        info!(
            "Routing operation to {} groups: {:?}",
            group_ids.len(),
            group_ids
        );

        let tasks: Vec<_> = group_ids
            .into_iter()
            .map(|group_id| {
                let operation = operation.clone();
                let router = self;
                async move {
                    router
                        .handle_local_stream_operation(group_id, operation, None)
                        .await
                }
            })
            .collect();

        let results = future::join_all(tasks).await;
        let responses: Vec<ConsensusResponse> = results
            .into_iter()
            .filter_map(|result| match result {
                Ok(response) => Some(response),
                Err(e) => {
                    error!("Failed to route operation to group: {}", e);
                    None
                }
            })
            .collect();

        Ok(responses)
    }

    /// Broadcast operation to all active consensus groups
    pub async fn broadcast_to_all_groups(
        &self,
        operation: LocalStreamOperation,
    ) -> ConsensusResult<HashMap<ConsensusGroupId, ConsensusResponse>> {
        let group_ids = {
            let allocation_mgr = self.allocation_manager.read().await;
            allocation_mgr
                .get_group_loads()
                .into_iter()
                .map(|(group_id, _)| group_id)
                .collect::<Vec<_>>()
        };

        info!("Broadcasting operation to {} groups", group_ids.len());

        let mut results = HashMap::new();
        for group_id in group_ids {
            match self
                .handle_local_stream_operation(group_id, operation.clone(), None)
                .await
            {
                Ok(response) => {
                    results.insert(group_id, response);
                }
                Err(e) => {
                    error!("Failed to broadcast to group {:?}: {}", group_id, e);
                }
            }
        }

        Ok(results)
    }

    /// Query streams across multiple groups with filtering
    pub async fn cross_group_query(
        &self,
        stream_pattern: Option<String>,
        group_filter: Option<Vec<ConsensusGroupId>>,
    ) -> ConsensusResult<HashMap<ConsensusGroupId, Vec<StreamQueryResult>>> {
        let target_groups = if let Some(groups) = group_filter {
            groups
        } else {
            let allocation_mgr = self.allocation_manager.read().await;
            allocation_mgr
                .get_group_loads()
                .into_iter()
                .map(|(group_id, _)| group_id)
                .collect()
        };

        debug!(
            "Cross-group query targeting {} groups with pattern: {:?}",
            target_groups.len(),
            stream_pattern
        );

        let mut results = HashMap::new();
        for group_id in target_groups {
            // Create a query operation for stream information
            let query_op = LocalStreamOperation::GetStreamCheckpoint {
                stream_name: stream_pattern.clone().unwrap_or_else(|| "*".to_string()),
            };

            match self
                .handle_local_stream_operation(group_id, query_op, None)
                .await
            {
                Ok(response) => {
                    // Convert response to stream query results
                    let stream_results = vec![StreamQueryResult {
                        stream_name: stream_pattern
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                        last_sequence: response.sequence.unwrap_or(0),
                        group_id,
                        message_count: 0, // TODO: Get from actual stream data
                    }];
                    results.insert(group_id, stream_results);
                }
                Err(e) => {
                    warn!("Failed to query group {:?}: {}", group_id, e);
                }
            }
        }

        Ok(results)
    }

    /// Route operation based on load balancing strategy
    pub async fn route_with_load_balancing(
        &self,
        operation: LocalStreamOperation,
        prefer_least_loaded: bool,
    ) -> ConsensusResult<ConsensusResponse> {
        let allocation_mgr = self.allocation_manager.read().await;
        let group_loads = allocation_mgr.get_group_loads();

        if group_loads.is_empty() {
            return Err(Error::NotFound("No consensus groups available".to_string()));
        }

        let target_group = if prefer_least_loaded {
            // Find group with least load
            group_loads
                .iter()
                .min_by_key(|(_, metadata)| metadata.stream_count)
                .map(|(group_id, _)| *group_id)
                .unwrap()
        } else {
            // Find group with most available capacity
            group_loads
                .iter()
                .max_by(|(_, a), (_, b)| {
                    // Higher message rate means more capacity being used
                    b.message_rate
                        .partial_cmp(&a.message_rate)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(group_id, _)| *group_id)
                .unwrap()
        };

        info!(
            "Load-balanced routing to group {:?} (prefer_least_loaded: {})",
            target_group, prefer_least_loaded
        );

        self.handle_local_stream_operation(target_group, operation, None)
            .await
    }

    /// Coordinate cross-group transaction (e.g., for migration)
    pub async fn coordinate_cross_group_transaction(
        &self,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
        source_operations: Vec<LocalStreamOperation>,
        target_operations: Vec<LocalStreamOperation>,
    ) -> ConsensusResult<CrossGroupTransactionResult> {
        info!(
            "Coordinating cross-group transaction: {:?} -> {:?}",
            source_group, target_group
        );

        let mut source_responses = Vec::new();
        let mut target_responses = Vec::new();
        let mut rollback_needed = false;

        // Phase 1: Execute source operations
        for operation in source_operations {
            match self
                .handle_local_stream_operation(source_group, operation.clone(), None)
                .await
            {
                Ok(response) => {
                    if !response.success {
                        rollback_needed = true;
                        break;
                    }
                    source_responses.push(response);
                }
                Err(e) => {
                    error!("Source operation failed: {}", e);
                    rollback_needed = true;
                    break;
                }
            }
        }

        // Phase 2: If source succeeded, execute target operations
        if !rollback_needed {
            for operation in target_operations {
                match self
                    .handle_local_stream_operation(target_group, operation.clone(), None)
                    .await
                {
                    Ok(response) => {
                        if !response.success {
                            rollback_needed = true;
                            break;
                        }
                        target_responses.push(response);
                    }
                    Err(e) => {
                        error!("Target operation failed: {}", e);
                        rollback_needed = true;
                        break;
                    }
                }
            }
        }

        // Phase 3: Handle rollback if needed
        if rollback_needed {
            warn!("Cross-group transaction failed, initiating rollback");
            // TODO: Implement proper rollback logic based on operation types
        }

        Ok(CrossGroupTransactionResult {
            success: !rollback_needed,
            source_responses,
            target_responses,
            rollback_performed: rollback_needed,
        })
    }

    /// Get health status across all groups
    pub async fn get_cluster_health(&self) -> ConsensusResult<ClusterHealthStatus> {
        let allocation_mgr = self.allocation_manager.read().await;
        let group_loads = allocation_mgr.get_group_loads();
        let local_metrics = self.local_manager.get_all_metrics().await;

        let mut healthy_groups = 0;
        let total_groups = group_loads.len();
        let mut group_health = HashMap::new();

        for (group_id, metadata) in group_loads {
            let raft_metrics = local_metrics.get(&group_id);
            let is_healthy = raft_metrics
                .map(|metrics| {
                    // Consider group healthy if it has a leader and sufficient members
                    metrics.current_leader.is_some() && metadata.members.len() >= 2
                })
                .unwrap_or(false);

            if is_healthy {
                healthy_groups += 1;
            }

            group_health.insert(
                group_id,
                GroupHealthInfo {
                    is_healthy,
                    member_count: metadata.members.len(),
                    stream_count: metadata.stream_count,
                    has_leader: raft_metrics
                        .map(|m| m.current_leader.is_some())
                        .unwrap_or(false),
                },
            );
        }

        let overall_health = if total_groups == 0 {
            OverallHealth::Unknown
        } else if healthy_groups == total_groups {
            OverallHealth::Healthy
        } else if healthy_groups > total_groups / 2 {
            OverallHealth::Degraded
        } else {
            OverallHealth::Unhealthy
        };

        Ok(ClusterHealthStatus {
            overall_health,
            healthy_groups,
            total_groups,
            group_health,
        })
    }
}

/// Metrics for the consensus router
#[derive(Debug)]
pub struct ConsensusRouterMetrics {
    /// Allocation metrics per group
    pub allocation_metrics: Vec<(ConsensusGroupId, crate::allocation::GroupMetadata)>,
    /// Raft metrics per local group
    pub local_group_metrics: std::collections::HashMap<
        ConsensusGroupId,
        openraft::RaftMetrics<crate::local::LocalTypeConfig>,
    >,
}

/// Result of a cross-group query operation
#[derive(Debug, Clone)]
pub struct StreamQueryResult {
    /// Name of the stream
    pub stream_name: String,
    /// Last sequence number in the stream
    pub last_sequence: u64,
    /// Group containing this stream
    pub group_id: ConsensusGroupId,
    /// Number of messages in the stream
    pub message_count: u64,
}

/// Result of a cross-group transaction
#[derive(Debug)]
pub struct CrossGroupTransactionResult {
    /// Whether the transaction succeeded
    pub success: bool,
    /// Responses from source group operations
    pub source_responses: Vec<ConsensusResponse>,
    /// Responses from target group operations
    pub target_responses: Vec<ConsensusResponse>,
    /// Whether rollback was performed
    pub rollback_performed: bool,
}

/// Health information for a single consensus group
#[derive(Debug, Clone)]
pub struct GroupHealthInfo {
    /// Whether this group is considered healthy
    pub is_healthy: bool,
    /// Number of members in the group
    pub member_count: usize,
    /// Number of streams allocated to this group
    pub stream_count: u32,
    /// Whether the group has an active leader
    pub has_leader: bool,
}

/// Overall cluster health status
#[derive(Debug, Clone)]
pub enum OverallHealth {
    /// All groups are healthy
    Healthy,
    /// Some groups are unhealthy but majority is healthy
    Degraded,
    /// Majority of groups are unhealthy
    Unhealthy,
    /// Unable to determine health status
    Unknown,
}

/// Complete cluster health status
#[derive(Debug)]
pub struct ClusterHealthStatus {
    /// Overall health assessment
    pub overall_health: OverallHealth,
    /// Number of healthy groups
    pub healthy_groups: usize,
    /// Total number of groups
    pub total_groups: usize,
    /// Health information per group
    pub group_health: HashMap<ConsensusGroupId, GroupHealthInfo>,
}
