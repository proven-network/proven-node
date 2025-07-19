//! Main routing service implementation

use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::foundation::ConsensusGroupId;
use crate::services::event::{
    Event, EventEnvelope, EventFilter, EventHandler, EventResult, EventService, EventType,
    EventingResult as EventResult2,
};
use proven_topology::NodeId;

use super::balancer::LoadBalancer;
use super::router::{OperationRouter, RouteDecision};
use super::table::RoutingTable;
use super::types::*;

/// Routing service for consensus operations
pub struct RoutingService {
    /// Service configuration
    config: RoutingConfig,

    /// Local node ID
    local_node_id: NodeId,

    /// Operation router
    router: Arc<OperationRouter>,

    /// Routing table
    routing_table: Arc<RoutingTable>,

    /// Load balancer
    load_balancer: Arc<LoadBalancer>,

    /// Routing metrics
    metrics: Arc<RwLock<RoutingMetrics>>,

    /// Background tasks
    background_tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Shutdown signal
    shutdown_signal: Arc<tokio::sync::Notify>,

    /// Service state
    state: Arc<RwLock<ServiceState>>,

    /// Event service reference
    event_service: Arc<RwLock<Option<Arc<EventService>>>>,
}

/// Service state
#[derive(Debug, Clone, Copy, PartialEq)]
enum ServiceState {
    /// Not started
    NotStarted,
    /// Running
    Running,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
}

impl RoutingService {
    /// Create a new routing service
    pub fn new(config: RoutingConfig, local_node_id: NodeId) -> Self {
        let routing_table = Arc::new(RoutingTable::new(config.cache_ttl));
        let load_balancer = Arc::new(LoadBalancer::new(
            config.default_strategy,
            config.load_thresholds.clone(),
        ));
        let router = Arc::new(OperationRouter::new(
            routing_table.clone(),
            load_balancer.clone(),
            config.enable_sticky_routing,
        ));

        Self {
            config,
            local_node_id,
            router,
            routing_table,
            load_balancer,
            metrics: Arc::new(RwLock::new(RoutingMetrics::default())),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            state: Arc::new(RwLock::new(ServiceState::NotStarted)),
            event_service: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the event service for this service
    pub async fn set_event_service(&self, event_service: Arc<EventService>) {
        *self.event_service.write().await = Some(event_service);
    }

    /// Start the routing service internally
    async fn start_internal(&self) -> RoutingResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotStarted | ServiceState::Stopped => {
                *state = ServiceState::Running;
            }
            _ => {
                return Err(RoutingError::Internal(format!(
                    "Service cannot be started from {:?} state",
                    *state
                )));
            }
        }
        drop(state);

        info!("Starting routing service internal");

        // Initialize with default group info
        let default_group_id = ConsensusGroupId::new(1);
        let default_group_route = GroupRoute {
            group_id: default_group_id,
            members: vec![], // Will be populated when group is actually created
            leader: None,
            health: GroupHealth::Healthy,
            last_updated: SystemTime::now(),
            stream_count: 0,
            location: GroupLocation::Remote, // Default to remote until we know it's local
        };
        self.routing_table
            .update_group_route(default_group_id, default_group_route)
            .await?;

        let mut tasks = self.background_tasks.write().await;

        // Start load monitoring if enabled
        if self.config.enable_load_balancing {
            tasks.push(self.spawn_load_monitor());
        }

        // Start metrics aggregation
        tasks.push(self.spawn_metrics_aggregator());

        // Register event handler for synchronous processing
        if let Some(event_service) = self.event_service.read().await.as_ref() {
            let handler = Arc::new(RoutingEventHandler::new(
                self.routing_table.clone(),
                self.local_node_id.clone(),
            ));

            if let Err(e) = event_service
                .register_handler("routing-handler".to_string(), handler)
                .await
            {
                error!("Failed to register routing event handler: {}", e);
            }

            // Add routes for the events we care about
            use crate::services::event::{EventRoute, EventRoutePattern};

            let stream_route = EventRoute {
                pattern: EventRoutePattern::ByType(EventType::Stream),
                handler_name: "routing-handler".to_string(),
                priority: 100, // High priority for immediate consistency
            };

            if let Err(e) = event_service.add_route(stream_route).await {
                error!("Failed to add stream event route: {}", e);
            }

            let consensus_route = EventRoute {
                pattern: EventRoutePattern::ByType(EventType::Consensus),
                handler_name: "routing-handler".to_string(),
                priority: 100,
            };

            if let Err(e) = event_service.add_route(consensus_route).await {
                error!("Failed to add consensus event route: {}", e);
            }

            let group_route = EventRoute {
                pattern: EventRoutePattern::ByType(EventType::Group),
                handler_name: "routing-handler".to_string(),
                priority: 100,
            };

            if let Err(e) = event_service.add_route(group_route).await {
                error!("Failed to add group event route: {}", e);
            }

            // No longer spawn the event processor - we're using handlers only
            info!("RoutingService: Using synchronous event handlers only");
        }

        Ok(())
    }

    /// Stop the routing service internally
    async fn stop_internal(&self) -> RoutingResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }

        *state = ServiceState::Stopping;
        drop(state);

        info!("Stopping routing service");

        // Signal shutdown
        self.shutdown_signal.notify_waiters();

        // Wait for tasks
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error stopping routing task: {}", e);
            }
        }

        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;

        Ok(())
    }

    /// Route a stream operation
    pub async fn route_stream_operation(
        &self,
        stream_name: &str,
        operation: Vec<u8>, // Simplified - would be actual operation type
    ) -> RoutingResult<RouteDecision> {
        self.ensure_running().await?;

        let start = std::time::Instant::now();

        // Get routing decision
        let decision = self
            .router
            .route_stream_operation(stream_name, operation)
            .await?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.total_operations += 1;

        match &decision {
            RouteDecision::RouteToGroup(group_id) => {
                metrics.successful_routes += 1;
                *metrics.routes_by_group.entry(*group_id).or_insert(0) += 1;
            }
            RouteDecision::Reject(_) => {
                metrics.failed_routes += 1;
            }
            _ => {
                metrics.successful_routes += 1;
            }
        }

        // Update latency
        let latency_ms = start.elapsed().as_millis() as f64;
        let total_ops = metrics.total_operations as f64;
        metrics.avg_latency_ms =
            (metrics.avg_latency_ms * (total_ops - 1.0) + latency_ms) / total_ops;

        Ok(decision)
    }

    /// Route a global operation
    pub async fn route_global_operation(
        &self,
        _operation: Vec<u8>,
    ) -> RoutingResult<RouteDecision> {
        self.ensure_running().await?;

        // Global operations always go to global consensus
        let mut metrics = self.metrics.write().await;
        metrics.total_operations += 1;
        metrics.successful_routes += 1;

        Ok(RouteDecision::RouteToGlobal)
    }

    /// Get global consensus leader
    pub async fn get_global_leader(&self) -> Option<NodeId> {
        self.routing_table.get_global_leader().await
    }

    /// Update stream assignment
    pub async fn update_stream_assignment(
        &self,
        stream_name: String,
        group_id: ConsensusGroupId,
    ) -> RoutingResult<()> {
        self.ensure_running().await?;

        let route = StreamRoute {
            stream_name: stream_name.clone(),
            group_id,
            assigned_at: SystemTime::now(),
            strategy: self.config.default_strategy,
            is_active: true,
            config: None, // Config will be set when StreamCreated event is received
        };

        self.routing_table
            .update_stream_route(stream_name, route)
            .await?;
        Ok(())
    }

    /// Remove stream assignment
    pub async fn remove_stream_assignment(&self, stream_name: &str) -> RoutingResult<()> {
        self.ensure_running().await?;
        self.routing_table.remove_stream_route(stream_name).await?;
        Ok(())
    }

    /// Update group information
    pub async fn update_group_info(
        &self,
        group_id: ConsensusGroupId,
        info: GroupRoute,
    ) -> RoutingResult<()> {
        self.ensure_running().await?;
        self.routing_table
            .update_group_route(group_id, info)
            .await?;
        Ok(())
    }

    /// Get routing information for a stream
    pub async fn get_stream_routing_info(
        &self,
        stream_name: &str,
    ) -> RoutingResult<Option<StreamRoute>> {
        self.routing_table.get_stream_route(stream_name).await
    }

    /// Get all routing information
    pub async fn get_routing_info(&self) -> RoutingResult<RoutingInfo> {
        let (stream_routes, group_routes) = self.routing_table.get_all_routes().await?;
        let metrics = self.metrics.read().await.clone();

        Ok(RoutingInfo {
            stream_routes,
            group_routes,
            default_strategy: self.config.default_strategy,
            metrics,
            last_refresh: SystemTime::now(),
        })
    }

    /// Get routing health
    pub async fn get_health(&self) -> RoutingResult<RoutingHealth> {
        let group_routes = self.routing_table.get_all_group_routes().await?;

        let total_groups = group_routes.len();
        let healthy_groups = group_routes
            .values()
            .filter(|g| g.health == GroupHealth::Healthy)
            .count();

        let metrics = self.metrics.read().await;
        let success_rate = if metrics.total_operations > 0 {
            metrics.successful_routes as f32 / metrics.total_operations as f32
        } else {
            1.0
        };

        let status = if healthy_groups == total_groups && success_rate > 0.95 {
            HealthStatus::Healthy
        } else if healthy_groups > total_groups / 2 && success_rate > 0.8 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        let mut issues = Vec::new();
        if healthy_groups < total_groups {
            issues.push(format!(
                "{} out of {} groups are unhealthy",
                total_groups - healthy_groups,
                total_groups
            ));
        }
        if success_rate < 0.9 {
            issues.push(format!(
                "Low routing success rate: {:.1}%",
                success_rate * 100.0
            ));
        }

        Ok(RoutingHealth {
            status,
            healthy_groups,
            total_groups,
            success_rate,
            avg_latency_ms: metrics.avg_latency_ms,
            issues,
        })
    }

    /// Get load information for all groups
    pub async fn get_load_info(&self) -> RoutingResult<Vec<LoadInfo>> {
        self.load_balancer.get_all_load_info().await
    }

    /// Check if a group is local to this node
    pub async fn is_group_local(&self, group_id: ConsensusGroupId) -> RoutingResult<bool> {
        let group_routes = self.routing_table.get_all_group_routes().await?;

        if let Some(route) = group_routes.get(&group_id) {
            Ok(route.location == GroupLocation::Local
                || route.location == GroupLocation::Distributed)
        } else {
            Err(RoutingError::GroupNotFound(group_id))
        }
    }

    /// Get the location information for a group
    pub async fn get_group_location(
        &self,
        group_id: ConsensusGroupId,
    ) -> RoutingResult<GroupLocationInfo> {
        let group_routes = self.routing_table.get_all_group_routes().await?;

        if let Some(route) = group_routes.get(&group_id) {
            let is_local = route.location == GroupLocation::Local
                || route.location == GroupLocation::Distributed;

            // Find nodes that have this group
            let nodes_with_group: Vec<NodeId> = if is_local {
                // If local, include this node
                let mut nodes = vec![self.local_node_id.clone()];
                // Add other members that are not this node
                for member in &route.members {
                    if member != &self.local_node_id {
                        nodes.push(member.clone());
                    }
                }
                nodes
            } else {
                // If remote, return all members
                route.members.clone()
            };

            Ok(GroupLocationInfo {
                group_id,
                location: route.location,
                is_local,
                nodes: nodes_with_group,
                leader: route.leader.clone(),
            })
        } else {
            Err(RoutingError::GroupNotFound(group_id))
        }
    }

    /// Get the best node to forward a request to for a group
    pub async fn get_best_node_for_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> RoutingResult<Option<NodeId>> {
        let location_info = self.get_group_location(group_id).await?;

        if location_info.is_local {
            // If local, no need to forward
            Ok(None)
        } else {
            // Prefer the leader if available, otherwise pick the first node
            Ok(location_info
                .leader
                .or_else(|| location_info.nodes.first().cloned()))
        }
    }

    // Private helper methods

    /// Ensure service is running
    async fn ensure_running(&self) -> RoutingResult<()> {
        let state = self.state.read().await;
        if *state != ServiceState::Running {
            error!("Routing service state is {:?}, expected Running", *state);
            return Err(RoutingError::NotStarted);
        }
        Ok(())
    }

    /// Spawn load monitoring task
    fn spawn_load_monitor(&self) -> JoinHandle<()> {
        let load_balancer = self.load_balancer.clone();
        let routing_table = self.routing_table.clone();
        let interval = self.config.load_check_interval;
        let shutdown = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // Update load information
                        if let Ok(groups) = routing_table.get_all_group_routes().await {
                            for (group_id, _) in groups {
                                // In a real implementation, fetch actual load metrics
                                let load_info = LoadInfo {
                                    group_id,
                                    stream_count: 0,
                                    messages_per_sec: 0.0,
                                    storage_bytes: 0,
                                    cpu_usage: 0.0,
                                    memory_usage: 0.0,
                                    load_score: 0.0,
                                };

                                if let Err(e) = load_balancer.update_load(group_id, load_info).await {
                                    error!("Failed to update load for group {:?}: {}", group_id, e);
                                }
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        debug!("Load monitor shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn metrics aggregation task
    fn spawn_metrics_aggregator(&self) -> JoinHandle<()> {
        let shutdown = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(tokio::time::Duration::from_secs(60));
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // Aggregate metrics
                        debug!("Aggregating routing metrics");
                    }
                    _ = shutdown.notified() => {
                        debug!("Metrics aggregator shutting down");
                        break;
                    }
                }
            }
        })
    }
}

impl Drop for RoutingService {
    fn drop(&mut self) {
        // Ensure shutdown on drop
        self.shutdown_signal.notify_waiters();
    }
}

#[async_trait::async_trait]
impl crate::foundation::traits::ServiceLifecycle for RoutingService {
    async fn start(&self) -> crate::error::ConsensusResult<()> {
        info!("ServiceLifecycle::start called for RoutingService");
        let result = self.start_internal().await.map_err(|e| {
            crate::error::ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to start routing service: {e}"),
            )
        });
        if result.is_ok() {
            info!("RoutingService started successfully via ServiceLifecycle");
        }
        result
    }

    async fn stop(&self) -> crate::error::ConsensusResult<()> {
        self.stop_internal().await.map_err(|e| {
            crate::error::ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to stop routing service: {e}"),
            )
        })
    }

    async fn is_running(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }

    async fn health_check(
        &self,
    ) -> crate::error::ConsensusResult<crate::foundation::traits::ServiceHealth> {
        let _is_running = self.is_running().await;
        let health = self.get_health().await.map_err(|e| {
            crate::error::ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to get routing health: {e}"),
            )
        })?;

        use crate::foundation::traits::{HealthStatus, ServiceHealth};

        let status = match health.status {
            super::types::HealthStatus::Healthy => HealthStatus::Healthy,
            super::types::HealthStatus::Degraded => HealthStatus::Degraded,
            super::types::HealthStatus::Unhealthy => HealthStatus::Unhealthy,
        };

        Ok(ServiceHealth {
            name: "RoutingService".to_string(),
            status,
            message: if health.issues.is_empty() {
                None
            } else {
                Some(health.issues.join("; "))
            },
            subsystems: vec![],
        })
    }
}

// Re-export config for convenience
pub use super::types::RoutingConfig;

/// Event handler wrapper for routing service
pub struct RoutingEventHandler {
    routing_table: Arc<RoutingTable>,
    local_node_id: NodeId,
}

impl RoutingEventHandler {
    pub fn new(routing_table: Arc<RoutingTable>, local_node_id: NodeId) -> Self {
        Self {
            routing_table,
            local_node_id,
        }
    }
}

#[async_trait::async_trait]
impl EventHandler for RoutingEventHandler {
    async fn handle(&self, envelope: EventEnvelope) -> EventResult2<EventResult> {
        // Only handle specific events
        match &envelope.event {
            Event::StreamCreated {
                name,
                group_id,
                config,
            } => {
                info!(
                    "RoutingEventHandler: Handling StreamCreated event for {} in group {:?}",
                    name, group_id
                );

                let route = StreamRoute {
                    stream_name: name.to_string(),
                    group_id: *group_id,
                    assigned_at: SystemTime::now(),
                    strategy: RoutingStrategy::Sticky,
                    is_active: true,
                    config: Some(config.clone()),
                };

                match self
                    .routing_table
                    .update_stream_route(name.to_string(), route)
                    .await
                {
                    Ok(_) => Ok(EventResult::Success),
                    Err(e) => Ok(EventResult::Failed(e.to_string())),
                }
            }
            Event::GroupCreated { group_id, members } => {
                info!(
                    "RoutingEventHandler: Handling GroupCreated for group {:?} with {} members",
                    group_id,
                    members.len()
                );

                let location = if members.contains(&self.local_node_id) {
                    GroupLocation::Local
                } else {
                    GroupLocation::Remote
                };

                let group_route = GroupRoute {
                    group_id: *group_id,
                    members: members.clone(),
                    leader: members.first().cloned(),
                    health: GroupHealth::Healthy,
                    last_updated: SystemTime::now(),
                    stream_count: 0,
                    location,
                };

                match self
                    .routing_table
                    .update_group_route(*group_id, group_route)
                    .await
                {
                    Ok(_) => {
                        info!(
                            "Successfully updated routing table for group {:?} (location: {:?})",
                            group_id, location
                        );
                        Ok(EventResult::Success)
                    }
                    Err(e) => Ok(EventResult::Failed(e.to_string())),
                }
            }
            Event::MembershipChanged {
                new_members,
                removed_members,
            } => {
                info!(
                    "RoutingEventHandler: Handling MembershipChanged - {} new, {} removed",
                    new_members.len(),
                    removed_members.len()
                );

                // Update all group routes that contain any of the affected members
                match self.routing_table.get_all_group_routes().await {
                    Ok(group_routes) => {
                        for (group_id, mut route) in group_routes {
                            let mut updated = false;

                            // Remove members that left
                            for removed in removed_members {
                                if let Some(pos) = route.members.iter().position(|m| m == removed) {
                                    route.members.remove(pos);
                                    updated = true;
                                }
                            }

                            // Add new members
                            for new_member in new_members {
                                if !route.members.contains(new_member) {
                                    route.members.push(new_member.clone());
                                    updated = true;
                                }
                            }

                            if updated {
                                // Update leader if necessary
                                if let Some(ref leader) = route.leader
                                    && removed_members.contains(leader)
                                {
                                    route.leader = route.members.first().cloned();
                                }

                                route.last_updated = SystemTime::now();
                                if let Err(e) =
                                    self.routing_table.update_group_route(group_id, route).await
                                {
                                    error!("Failed to update group route: {}", e);
                                }
                            }
                        }
                        Ok(EventResult::Success)
                    }
                    Err(e) => Ok(EventResult::Failed(e.to_string())),
                }
            }
            Event::GroupLeaderChanged {
                group_id,
                new_leader,
                ..
            } => {
                info!(
                    "RoutingEventHandler: Handling GroupLeaderChanged for group {:?}, new leader {:?}",
                    group_id, new_leader
                );

                match self.routing_table.get_group_route(*group_id).await {
                    Ok(Some(mut route)) => {
                        route.leader = Some(new_leader.clone());
                        route.last_updated = SystemTime::now();
                        match self
                            .routing_table
                            .update_group_route(*group_id, route)
                            .await
                        {
                            Ok(_) => Ok(EventResult::Success),
                            Err(e) => Ok(EventResult::Failed(e.to_string())),
                        }
                    }
                    Ok(None) => Ok(EventResult::Ignored),
                    Err(e) => Ok(EventResult::Failed(e.to_string())),
                }
            }
            Event::GlobalLeaderChanged { new_leader, .. } => {
                info!(
                    "RoutingEventHandler: Handling GlobalLeaderChanged, new leader: {}",
                    new_leader
                );
                self.routing_table
                    .update_global_leader(Some(new_leader.clone()))
                    .await;
                Ok(EventResult::Success)
            }
            _ => Ok(EventResult::Ignored),
        }
    }

    fn name(&self) -> &str {
        "RoutingEventHandler"
    }
}
