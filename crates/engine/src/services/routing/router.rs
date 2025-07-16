//! Operation router implementation

use std::sync::Arc;

use tracing::debug;

use crate::foundation::ConsensusGroupId;

use super::balancer::LoadBalancer;
use super::table::RoutingTable;
use super::types::*;

/// Router decision for operations
#[derive(Debug, Clone, PartialEq)]
pub enum RouteDecision {
    /// Route to global consensus
    RouteToGlobal,
    /// Route to specific group
    RouteToGroup(ConsensusGroupId),
    /// Process locally
    ProcessLocally,
    /// Reject the operation
    Reject(String),
}

/// Operation router
pub struct OperationRouter {
    /// Routing table
    routing_table: Arc<RoutingTable>,
    /// Load balancer
    load_balancer: Arc<LoadBalancer>,
    /// Enable sticky routing
    enable_sticky: bool,
}

impl OperationRouter {
    /// Create a new operation router
    pub fn new(
        routing_table: Arc<RoutingTable>,
        load_balancer: Arc<LoadBalancer>,
        enable_sticky: bool,
    ) -> Self {
        Self {
            routing_table,
            load_balancer,
            enable_sticky,
        }
    }

    /// Route a stream operation
    pub async fn route_stream_operation(
        &self,
        stream_name: &str,
        _operation: Vec<u8>,
    ) -> RoutingResult<RouteDecision> {
        // Check if stream already has a route
        if self.enable_sticky
            && let Ok(Some(route)) = self.routing_table.get_stream_route(stream_name).await
            && route.is_active
        {
            debug!("Using sticky route for stream {}", stream_name);
            return Ok(RouteDecision::RouteToGroup(route.group_id));
        }

        // Get available groups
        let groups = self.routing_table.get_all_group_routes().await?;
        if groups.is_empty() {
            return Err(RoutingError::NoAvailableGroups);
        }

        // Filter healthy groups
        let healthy_groups: Vec<_> = groups
            .into_iter()
            .filter(|(_, g)| g.health == GroupHealth::Healthy)
            .collect();

        if healthy_groups.is_empty() {
            return Err(RoutingError::NoAvailableGroups);
        }

        // Select group based on load
        let group_id = self
            .load_balancer
            .select_group(healthy_groups.iter().map(|(id, _)| *id).collect())
            .await?;

        debug!("Routing stream {} to group {:?}", stream_name, group_id);
        Ok(RouteDecision::RouteToGroup(group_id))
    }

    /// Route a global operation
    pub async fn route_global_operation(
        &self,
        _operation: Vec<u8>,
    ) -> RoutingResult<RouteDecision> {
        // Global operations always go to global consensus
        Ok(RouteDecision::RouteToGlobal)
    }

    /// Route based on operation type
    pub async fn route_operation(
        &self,
        operation_type: &str,
        target: Option<&str>,
    ) -> RoutingResult<RouteDecision> {
        match operation_type {
            "stream" => {
                if let Some(stream_name) = target {
                    self.route_stream_operation(stream_name, vec![]).await
                } else {
                    Err(RoutingError::RoutingFailed(
                        "Stream operation requires target".to_string(),
                    ))
                }
            }
            "global" => Ok(RouteDecision::RouteToGlobal),
            "local" => Ok(RouteDecision::ProcessLocally),
            _ => Err(RoutingError::OperationNotSupported(
                operation_type.to_string(),
            )),
        }
    }

    /// Check if a group can accept new streams
    pub async fn can_accept_stream(&self, group_id: ConsensusGroupId) -> RoutingResult<bool> {
        if let Ok(Some(group)) = self.routing_table.get_group_route(group_id).await {
            if group.health != GroupHealth::Healthy {
                return Ok(false);
            }

            // Check load
            let load_info = self.load_balancer.get_load(group_id).await?;
            Ok(load_info.map(|l| l.load_score < 0.8).unwrap_or(true))
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_global_routing() {
        let routing_table = Arc::new(RoutingTable::new(std::time::Duration::from_secs(60)));
        let load_balancer = Arc::new(LoadBalancer::new(
            RoutingStrategy::LeastLoaded,
            LoadThresholds::default(),
        ));
        let router = OperationRouter::new(routing_table, load_balancer, true);

        let decision = router.route_global_operation(vec![]).await.unwrap();
        assert_eq!(decision, RouteDecision::RouteToGlobal);
    }
}
