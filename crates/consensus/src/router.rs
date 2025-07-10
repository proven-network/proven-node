use crate::allocation::{AllocationManager, ConsensusGroupId};
use crate::error::{ConsensusResult, Error};
use crate::global::GlobalResponse;
use crate::global::global_manager::GlobalManager;
use crate::local::LocalConsensusManager;
use crate::local::LocalResponse;
use crate::monitoring::MonitoringService;
use crate::operations::GlobalOperation;
use crate::operations::{LocalStreamOperation, MigrationOperation, StreamOperation};
use futures::future;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Unified operation type that can be routed to appropriate consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusOperation {
    /// Global admin operation
    GlobalAdmin(GlobalOperation),
    /// Local stream operation
    LocalStream {
        /// Target consensus group
        group_id: ConsensusGroupId,
        /// The operation
        operation: LocalStreamOperation,
    },
}

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

    /// Route a consensus operation to the appropriate handler
    pub async fn route_operation(
        &self,
        operation: ConsensusOperation,
        request_id: Option<String>,
    ) -> ConsensusResult<LocalResponse> {
        match operation {
            ConsensusOperation::GlobalAdmin(op) => {
                self.handle_global_admin_operation(op, request_id).await
            }
            ConsensusOperation::LocalStream {
                group_id,
                operation,
            } => {
                self.handle_local_stream_operation(group_id, operation, request_id)
                    .await
            }
        }
    }

    /// Handle a global administrative operation
    async fn handle_global_admin_operation(
        &self,
        operation: GlobalOperation,
        _request_id: Option<String>,
    ) -> ConsensusResult<LocalResponse> {
        // Submit the operation directly to global manager
        let response = self.global_manager.submit_operation(operation).await?;

        Ok(LocalResponse {
            success: response.success,
            sequence: Some(response.sequence),
            error: response.error,
            checkpoint_data: None,
        })
    }

    /// Handle a local stream operation
    async fn handle_local_stream_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: LocalStreamOperation,
        _request_id: Option<String>,
    ) -> ConsensusResult<LocalResponse> {
        // Forward to the local consensus manager
        let response = self
            .local_manager
            .process_operation(group_id, operation)
            .await?;
        Ok(response)
    }

    /// Get the consensus group for a stream
    pub async fn get_stream_group(&self, stream_name: &str) -> ConsensusResult<ConsensusGroupId> {
        let allocation_mgr = self.allocation_manager.read().await;
        allocation_mgr
            .get_stream_group(stream_name)
            .ok_or_else(|| Error::not_found(format!("Stream '{}' not allocated", stream_name)))
    }

    /// Route a stream operation automatically based on stream allocation
    pub async fn route_stream_operation(
        &self,
        stream_name: &str,
        operation: LocalStreamOperation,
        request_id: Option<String>,
    ) -> ConsensusResult<LocalResponse> {
        let group_id = self.get_stream_group(stream_name).await?;

        // Check if we're in a migration scenario
        if self.local_manager.is_in_migration().await {
            // During migration, we might need to route to multiple groups
            self.route_stream_operation_with_migration(stream_name, group_id, operation, request_id)
                .await
        } else {
            // Normal single-group routing
            self.handle_local_stream_operation(group_id, operation, request_id)
                .await
        }
    }

    /// Route a stream operation during migration (dual membership)
    async fn route_stream_operation_with_migration(
        &self,
        _stream_name: &str,
        primary_group: ConsensusGroupId,
        operation: LocalStreamOperation,
        request_id: Option<String>,
    ) -> ConsensusResult<LocalResponse> {
        let my_groups = self.local_manager.get_my_groups().await;

        // Find groups where this node is in migration state
        let migration_groups: Vec<_> = my_groups
            .iter()
            .filter(|(_, state)| *state != crate::local::NodeMigrationState::Active)
            .collect();

        if migration_groups.is_empty() {
            // No migration, route normally
            return self
                .handle_local_stream_operation(primary_group, operation, request_id)
                .await;
        }

        // During migration, we need to consider:
        // 1. If we're joining a new group, write to both old and new
        // 2. If we're leaving a group, still serve reads but not writes

        let is_write_operation = matches!(
            &operation,
            LocalStreamOperation::Stream(StreamOperation::Publish { .. })
                | LocalStreamOperation::Stream(StreamOperation::PublishBatch { .. })
                | LocalStreamOperation::Stream(StreamOperation::Rollup { .. })
                | LocalStreamOperation::Stream(StreamOperation::Delete { .. })
        );

        if is_write_operation {
            // For writes during migration, we need to ensure consistency
            // Write to the primary group first
            let response = self
                .handle_local_stream_operation(primary_group, operation.clone(), request_id.clone())
                .await?;

            // If we're joining a new group, also write there
            for (group_id, state) in &migration_groups {
                if matches!(state, crate::local::NodeMigrationState::Joining) {
                    // Best effort write to new group
                    match self
                        .handle_local_stream_operation(
                            *group_id,
                            operation.clone(),
                            request_id.clone(),
                        )
                        .await
                    {
                        Ok(_) => debug!(
                            "Successfully replicated write to joining group {:?}",
                            group_id
                        ),
                        Err(e) => warn!(
                            "Failed to replicate write to joining group {:?}: {}",
                            group_id, e
                        ),
                    }
                }
            }

            Ok(response)
        } else {
            // For reads, prefer the primary group
            self.handle_local_stream_operation(primary_group, operation, request_id)
                .await
        }
    }

    /// Reallocate a stream to a new group (used during migration)
    pub async fn reallocate_stream(
        &self,
        stream_name: &str,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        info!(
            "Reallocating stream '{}' from group {:?} to {:?}",
            stream_name, from_group, to_group
        );

        // Update the allocation manager
        let mut allocation_mgr = self.allocation_manager.write().await;
        allocation_mgr
            .reallocate_stream(stream_name, to_group)
            .map_err(|e| Error::InvalidOperation(format!("Failed to reallocate stream: {}", e)))?;

        // The actual data migration happens at the consensus level
        // This just updates the routing information
        Ok(())
    }

    /// Handle stream operations for a node that is in both old and new groups
    pub async fn handle_dual_membership_operation(
        &self,
        stream_name: &str,
        operation: LocalStreamOperation,
        old_group: ConsensusGroupId,
        new_group: ConsensusGroupId,
        request_id: Option<String>,
    ) -> ConsensusResult<LocalResponse> {
        let is_write = matches!(
            &operation,
            LocalStreamOperation::Stream(StreamOperation::Publish { .. })
                | LocalStreamOperation::Stream(StreamOperation::PublishBatch { .. })
                | LocalStreamOperation::Stream(StreamOperation::Rollup { .. })
                | LocalStreamOperation::Stream(StreamOperation::Delete { .. })
        );

        if is_write {
            // For writes, we need to write to both groups
            // First write to the old group (primary)
            let response = self
                .handle_local_stream_operation(old_group, operation.clone(), request_id.clone())
                .await?;

            // Then write to the new group (best effort)
            match self
                .handle_local_stream_operation(new_group, operation, request_id.clone())
                .await
            {
                Ok(_) => info!(
                    "Successfully replicated write for '{}' to new group {:?}",
                    stream_name, new_group
                ),
                Err(e) => warn!(
                    "Failed to replicate write for '{}' to new group {:?}: {}",
                    stream_name, new_group, e
                ),
            }

            Ok(response)
        } else {
            // For reads, prefer the old group as it has the complete data
            self.handle_local_stream_operation(old_group, operation, request_id)
                .await
        }
    }

    /// Check if a stream is currently being migrated
    pub async fn is_stream_in_migration(&self, _stream_name: &str) -> bool {
        // This would check with the global state to see if there's an active migration
        // For now, we check if the node is in migration
        self.local_manager.is_in_migration().await
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
    ) -> ConsensusResult<GlobalResponse> {
        // Submit the operation directly to global manager
        let response = self.global_manager.submit_operation(operation).await?;
        Ok(response)
    }

    /// Route a local operation to a specific group
    pub async fn route_local_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: LocalStreamOperation,
    ) -> ConsensusResult<LocalResponse> {
        self.handle_local_stream_operation(group_id, operation, None)
            .await
    }

    /// Update monitoring metrics
    pub async fn update_monitoring_metrics(
        &self,
        active_migrations: Vec<crate::migration::ActiveMigration>,
    ) {
        let local_metrics = self.local_manager.get_all_metrics().await;

        // Update allocation manager with current message rates and storage sizes
        {
            let mut allocation_mgr = self.allocation_manager.write().await;
            for group_id in local_metrics.keys() {
                // Get the local state metrics if available
                if let Ok(response) = self
                    .local_manager
                    .process_operation(*group_id, LocalStreamOperation::get_metrics())
                    .await
                {
                    if response.success {
                        // The response contains the serialized LocalMetrics
                        if let Some(metrics_data) = response.checkpoint_data {
                            if let Ok(state_metrics) =
                                serde_json::from_slice::<crate::local::LocalStateMetrics>(
                                    &metrics_data[..],
                                )
                            {
                                // Update message rate
                                let _ = allocation_mgr.update_group_message_rate(
                                    *group_id,
                                    state_metrics.message_rate,
                                );

                                // Update storage size
                                let _ = allocation_mgr.update_group_storage_size(
                                    *group_id,
                                    state_metrics.total_bytes,
                                );
                            }
                        }
                    }
                }
            }
        }

        let allocation_metrics = {
            let allocation_mgr = self.allocation_manager.read().await;
            allocation_mgr.get_group_loads()
        };

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
    ) -> ConsensusResult<LocalResponse> {
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
    ) -> ConsensusResult<Vec<LocalResponse>> {
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
        let responses: Vec<LocalResponse> = results
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
    ) -> ConsensusResult<HashMap<ConsensusGroupId, LocalResponse>> {
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
            let query_op = LocalStreamOperation::get_stream_checkpoint(
                stream_pattern.clone().unwrap_or_else(|| "*".to_string()),
            );

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
    ) -> ConsensusResult<LocalResponse> {
        let allocation_mgr = self.allocation_manager.read().await;
        let group_loads = allocation_mgr.get_group_loads();

        if group_loads.is_empty() {
            return Err(Error::not_found("No consensus groups available"));
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
        let mut executed_source_ops = Vec::new();
        let mut executed_target_ops = Vec::new();

        // Phase 1: Execute source operations
        for (idx, operation) in source_operations.iter().enumerate() {
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
                    executed_source_ops.push((idx, operation.clone()));
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
            for (idx, operation) in target_operations.iter().enumerate() {
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
                        executed_target_ops.push((idx, operation.clone()));
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

            // Rollback target operations first (in reverse order)
            for (_idx, operation) in executed_target_ops.iter().rev() {
                if let Some(rollback_op) = self.create_rollback_operation(operation) {
                    if let Err(e) = self
                        .handle_local_stream_operation(target_group, rollback_op, None)
                        .await
                    {
                        error!("Failed to rollback target operation: {}", e);
                    }
                }
            }

            // Rollback source operations (in reverse order)
            for (_idx, operation) in executed_source_ops.iter().rev() {
                if let Some(rollback_op) = self.create_rollback_operation(operation) {
                    if let Err(e) = self
                        .handle_local_stream_operation(source_group, rollback_op, None)
                        .await
                    {
                        error!("Failed to rollback source operation: {}", e);
                    }
                }
            }
        }

        Ok(CrossGroupTransactionResult {
            success: !rollback_needed,
            source_responses,
            target_responses,
            rollback_performed: rollback_needed,
        })
    }

    /// Create a compensating operation to rollback a given operation
    fn create_rollback_operation(
        &self,
        operation: &LocalStreamOperation,
    ) -> Option<LocalStreamOperation> {
        match operation {
            LocalStreamOperation::Migration(MigrationOperation::CreateStream {
                stream_name,
                ..
            }) => {
                // Rollback: Remove the stream
                Some(LocalStreamOperation::remove_stream(stream_name.clone()))
            }
            LocalStreamOperation::Migration(MigrationOperation::PauseStream { stream_name }) => {
                // Rollback: Resume the stream
                Some(LocalStreamOperation::resume_stream(stream_name.clone()))
            }
            LocalStreamOperation::Migration(MigrationOperation::RemoveStream { stream_name }) => {
                // Cannot rollback a removal - data is lost
                warn!("Cannot rollback stream removal for {}", stream_name);
                None
            }
            LocalStreamOperation::Migration(MigrationOperation::ApplyCheckpoint { .. })
            | LocalStreamOperation::Migration(MigrationOperation::ApplyIncrementalCheckpoint {
                ..
            }) => {
                // Cannot easily rollback checkpoint application
                warn!("Cannot rollback checkpoint application");
                None
            }
            LocalStreamOperation::Stream(StreamOperation::Publish { .. })
            | LocalStreamOperation::Stream(StreamOperation::PublishBatch { .. }) => {
                // Cannot rollback publishes - would need to track sequences
                warn!("Cannot rollback publish operations");
                None
            }
            _ => {
                // For other operations, no rollback needed or possible
                None
            }
        }
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

    /// Query information about a specific stream
    pub async fn query_stream_info(&self, stream_name: &str) -> ConsensusResult<StreamInfo> {
        // Get stream info from global state through global manager
        let global_state = self.global_manager.get_current_state().await?;

        // Get stream data
        let streams = global_state.streams.read().await;
        let stream_data = streams
            .get(stream_name)
            .ok_or_else(|| Error::not_found(format!("Stream {} not found", stream_name)))?;

        // Get stream config which contains consensus group allocation
        let stream_configs = global_state.stream_configs.read().await;
        let stream_config = stream_configs.get(stream_name).cloned();

        // Check if stream is paused by looking for it in any local group
        // For now, assume not paused (this would need to be checked via local groups)
        let is_paused = false;

        Ok(StreamInfo {
            name: stream_name.to_string(),
            next_sequence: stream_data.next_sequence,
            message_count: stream_data.messages.len(),
            consensus_group: stream_config.and_then(|c| c.consensus_group),
            is_paused,
            subscriptions: stream_data.subscriptions.clone(),
        })
    }

    /// Get information about a consensus group
    pub async fn get_consensus_group_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> Option<crate::global::global_state::ConsensusGroupInfo> {
        let global_state = self.global_manager.get_current_state().await.ok()?;
        let consensus_groups = global_state.consensus_groups.read().await;
        consensus_groups.get(&group_id).cloned()
    }

    /// Check health of a specific consensus group
    pub async fn check_group_health(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GroupHealthStatus> {
        // Get group info from global state
        let global_state = self.global_manager.get_current_state().await?;
        let consensus_groups = global_state.consensus_groups.read().await;
        let group_info = consensus_groups
            .get(&group_id)
            .ok_or_else(|| Error::not_found(format!("Group {:?} not found", group_id)))?;

        // Get Raft metrics for the group
        let local_metrics = self.local_manager.get_all_metrics().await;
        let raft_metrics = local_metrics.get(&group_id);

        // Check if we have a leader and enough members for quorum
        let has_leader = raft_metrics
            .map(|m| m.current_leader.is_some())
            .unwrap_or(false);

        let available_nodes = if has_leader {
            group_info.members.len()
        } else {
            // If no leader, check how many nodes we can reach
            // For now, assume all configured members are available
            group_info.members.len()
        };

        let required_nodes = (group_info.members.len() / 2) + 1; // Majority quorum
        let has_quorum = available_nodes >= required_nodes;

        Ok(GroupHealthStatus {
            has_quorum,
            has_leader,
            available_nodes,
            required_nodes,
            members: group_info.members.clone(),
        })
    }

    /// Get the number of streams allocated to a group
    pub async fn get_group_stream_count(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<usize> {
        let allocation_mgr = self.allocation_manager.read().await;
        let group_loads = allocation_mgr.get_group_loads();

        Ok(group_loads
            .into_iter()
            .find(|(gid, _)| *gid == group_id)
            .map(|(_, metadata)| metadata.stream_count as usize)
            .unwrap_or(0))
    }

    /// Get a reference to the global manager
    pub fn global_manager(&self) -> &Arc<GlobalManager<G, A>> {
        &self.global_manager
    }

    /// Perform periodic cleanup of pending operations across all groups
    pub async fn cleanup_pending_operations(&self, max_age_secs: u64) -> ConsensusResult<u64> {
        let groups = {
            let allocation_mgr = self.allocation_manager.read().await;
            allocation_mgr
                .get_group_loads()
                .into_iter()
                .map(|(group_id, _)| group_id)
                .collect::<Vec<_>>()
        };

        let mut total_cleaned = 0u64;

        for group_id in groups {
            let operation = LocalStreamOperation::cleanup_pending_operations(max_age_secs);

            match self.route_local_operation(group_id, operation).await {
                Ok(response) => {
                    if response.success {
                        if let Some(cleaned) = response.sequence {
                            total_cleaned += cleaned;
                            if cleaned > 0 {
                                info!(
                                    "Cleaned {} pending operations from group {:?}",
                                    cleaned, group_id
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to cleanup pending operations for group {:?}: {}",
                        group_id, e
                    );
                }
            }
        }

        if total_cleaned > 0 {
            info!(
                "Total pending operations cleaned across all groups: {}",
                total_cleaned
            );
        }

        Ok(total_cleaned)
    }

    /// Register a consensus group with the allocation manager
    pub async fn register_consensus_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<crate::NodeId>,
    ) -> ConsensusResult<()> {
        let mut allocation_mgr = self.allocation_manager.write().await;
        allocation_mgr.add_group(group_id, members);
        info!(
            "Registered consensus group {:?} with allocation manager",
            group_id
        );
        Ok(())
    }

    /// Unregister a consensus group from the allocation manager
    pub async fn unregister_consensus_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        let mut allocation_mgr = self.allocation_manager.write().await;
        allocation_mgr.remove_group(group_id).map_err(|e| {
            Error::InvalidOperation(format!("Failed to remove group {:?}: {}", group_id, e))
        })?;
        info!(
            "Unregistered consensus group {:?} from allocation manager",
            group_id
        );
        Ok(())
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
    pub source_responses: Vec<LocalResponse>,
    /// Responses from target group operations
    pub target_responses: Vec<LocalResponse>,
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

/// Health status for a specific consensus group
#[derive(Debug, Clone)]
pub struct GroupHealthStatus {
    /// Whether the group has quorum
    pub has_quorum: bool,
    /// Whether the group has an active leader
    pub has_leader: bool,
    /// Number of available nodes
    pub available_nodes: usize,
    /// Number of nodes required for quorum
    pub required_nodes: usize,
    /// Member node IDs
    pub members: Vec<crate::NodeId>,
}

/// Information about a stream
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Stream name
    pub name: String,
    /// Next sequence number
    pub next_sequence: u64,
    /// Number of messages in the stream
    pub message_count: usize,
    /// Consensus group this stream is allocated to
    pub consensus_group: Option<ConsensusGroupId>,
    /// Whether the stream is paused
    pub is_paused: bool,
    /// Subject subscriptions
    pub subscriptions: std::collections::HashSet<String>,
}
