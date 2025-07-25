//! Main routing table implementation

use crate::foundation::ConsensusGroupId;
use dashmap::DashMap;
use proven_topology::NodeId;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::timeout;

use super::types::{GroupLocation, GroupRoute, RoutingDecision, RoutingError, StreamRoute};

/// Thread-safe routing table for managing stream and group routes
pub struct RoutingTable {
    /// Local node ID for determining local vs remote
    local_node_id: NodeId,

    /// Stream routes: stream_name -> route info
    stream_routes: Arc<DashMap<String, StreamRoute>>,

    /// Group routes: group_id -> route info
    group_routes: Arc<DashMap<ConsensusGroupId, GroupRoute>>,

    /// Global consensus leader
    global_leader: Arc<RwLock<Option<NodeId>>>,

    /// Notifier for default group availability
    default_group_notifier: Arc<Notify>,
}

impl RoutingTable {
    /// Create a new routing table
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            stream_routes: Arc::new(DashMap::new()),
            group_routes: Arc::new(DashMap::new()),
            global_leader: Arc::new(RwLock::new(None)),
            default_group_notifier: Arc::new(Notify::new()),
        }
    }

    // Stream route management

    /// Update or insert a stream route
    pub async fn update_stream_route(
        &self,
        stream_name: String,
        group_id: ConsensusGroupId,
    ) -> Result<(), RoutingError> {
        let route = StreamRoute {
            stream_name: stream_name.clone(),
            group_id,
            assigned_at: std::time::SystemTime::now(),
            is_active: true,
        };

        self.stream_routes.insert(stream_name, route);

        // Increment stream count for the group
        self.increment_group_stream_count(group_id).await?;

        Ok(())
    }

    /// Remove a stream route
    pub async fn remove_stream_route(&self, stream_name: &str) -> Result<(), RoutingError> {
        if let Some((_, route)) = self.stream_routes.remove(stream_name) {
            // Decrement stream count for the group
            self.decrement_group_stream_count(route.group_id).await?;
        }

        Ok(())
    }

    /// Get a stream route
    pub async fn get_stream_route(
        &self,
        stream_name: &str,
    ) -> Result<Option<StreamRoute>, RoutingError> {
        Ok(self
            .stream_routes
            .get(stream_name)
            .map(|entry| entry.clone()))
    }

    /// Get all stream routes
    pub async fn get_all_stream_routes(
        &self,
    ) -> Result<std::collections::HashMap<String, StreamRoute>, RoutingError> {
        let mut routes = std::collections::HashMap::new();
        for entry in self.stream_routes.iter() {
            routes.insert(entry.key().clone(), entry.value().clone());
        }
        Ok(routes)
    }

    // Group route management

    /// Update or insert a group route
    pub async fn update_group_route(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
        leader: Option<NodeId>,
    ) -> Result<(), RoutingError> {
        // Get existing stream count or default to 0
        let stream_count = self
            .group_routes
            .get(&group_id)
            .map(|r| r.stream_count)
            .unwrap_or(0);

        let route = GroupRoute {
            group_id,
            members,
            leader: leader.clone(),
            stream_count,
        };

        self.group_routes.insert(group_id, route);

        // Notify if this is the default group with a leader
        if group_id == ConsensusGroupId::new(1) && leader.is_some() {
            self.default_group_notifier.notify_waiters();
        }

        Ok(())
    }

    /// Remove a group route
    pub async fn remove_group_route(&self, group_id: ConsensusGroupId) -> Result<(), RoutingError> {
        self.group_routes.remove(&group_id);
        Ok(())
    }

    /// Get a group route
    pub async fn get_group_route(
        &self,
        group_id: ConsensusGroupId,
    ) -> Result<Option<GroupRoute>, RoutingError> {
        Ok(self.group_routes.get(&group_id).map(|entry| entry.clone()))
    }

    /// Get all group routes
    pub async fn get_all_group_routes(
        &self,
    ) -> Result<std::collections::HashMap<ConsensusGroupId, GroupRoute>, RoutingError> {
        let mut routes = std::collections::HashMap::new();
        for entry in self.group_routes.iter() {
            routes.insert(*entry.key(), entry.value().clone());
        }
        Ok(routes)
    }

    // Global leader management

    /// Update the global consensus leader
    pub async fn update_global_leader(&self, leader: Option<NodeId>) {
        let mut current_leader = self.global_leader.write().await;
        *current_leader = leader;
    }

    /// Get the current global consensus leader
    pub async fn get_global_leader(&self) -> Option<NodeId> {
        let leader = self.global_leader.read().await;
        leader.clone()
    }

    // Query methods

    /// Check if a group is local to this node
    pub async fn is_group_local(&self, group_id: ConsensusGroupId) -> Result<bool, RoutingError> {
        self.group_routes
            .get(&group_id)
            .map(|route| route.members.contains(&self.local_node_id))
            .ok_or(RoutingError::GroupNotFound(group_id))
    }

    /// Get the location of a group relative to this node
    pub async fn get_group_location(
        &self,
        group_id: ConsensusGroupId,
    ) -> Result<GroupLocation, RoutingError> {
        self.group_routes
            .get(&group_id)
            .map(|route| {
                if route.members.contains(&self.local_node_id) {
                    GroupLocation::Local
                } else {
                    GroupLocation::Remote
                }
            })
            .ok_or(RoutingError::GroupNotFound(group_id))
    }

    /// Find the least loaded group (local groups preferred)
    pub async fn find_least_loaded_group(&self) -> Option<ConsensusGroupId> {
        // First try to find a local group with capacity
        let local_group = self
            .group_routes
            .iter()
            .filter(|entry| entry.value().members.contains(&self.local_node_id))
            .min_by_key(|entry| entry.value().stream_count)
            .map(|entry| *entry.key());

        if local_group.is_some() {
            return local_group;
        }

        // Otherwise find any group with least load
        self.group_routes
            .iter()
            .min_by_key(|entry| entry.value().stream_count)
            .map(|entry| *entry.key())
    }

    /// Get all streams assigned to a group
    pub async fn get_streams_by_group(&self, group_id: ConsensusGroupId) -> Vec<String> {
        self.stream_routes
            .iter()
            .filter(|entry| entry.value().group_id == group_id)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Count streams per group
    pub async fn count_streams_per_group(
        &self,
    ) -> std::collections::HashMap<ConsensusGroupId, usize> {
        let mut counts = std::collections::HashMap::new();

        for entry in self.stream_routes.iter() {
            *counts.entry(entry.value().group_id).or_insert(0) += 1;
        }

        counts
    }

    /// Wait for the default group (group ID 1) to be available with a leader
    pub async fn wait_for_default_group(
        &self,
        wait_duration: Duration,
    ) -> Result<(), RoutingError> {
        // Check if already available
        if let Some(route) = self.group_routes.get(&ConsensusGroupId::new(1))
            && route.leader.is_some()
        {
            return Ok(());
        }

        // Wait for notification
        match timeout(wait_duration, self.default_group_notifier.notified()).await {
            Ok(_) => Ok(()),
            Err(_) => Err(RoutingError::Internal(
                "Timeout waiting for default group".to_string(),
            )),
        }
    }

    /// Get routing decision for a stream
    pub async fn get_routing_decision(
        &self,
        stream_name: &str,
    ) -> Result<RoutingDecision, RoutingError> {
        // Check if stream has an assigned route
        if let Some(route) = self.get_stream_route(stream_name).await?
            && route.is_active
        {
            return Ok(RoutingDecision::Group(route.group_id));
        }

        // Find best group for new stream
        if let Some(group_id) = self.find_least_loaded_group().await {
            Ok(RoutingDecision::Group(group_id))
        } else {
            Ok(RoutingDecision::Reject {
                reason: "No available groups".to_string(),
            })
        }
    }

    // Private helper methods

    async fn increment_group_stream_count(
        &self,
        group_id: ConsensusGroupId,
    ) -> Result<(), RoutingError> {
        if let Some(mut route) = self.group_routes.get_mut(&group_id) {
            route.stream_count += 1;
        }

        Ok(())
    }

    async fn decrement_group_stream_count(
        &self,
        group_id: ConsensusGroupId,
    ) -> Result<(), RoutingError> {
        if let Some(mut route) = self.group_routes.get_mut(&group_id)
            && route.stream_count > 0
        {
            route.stream_count -= 1;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_group_location() {
        let local_node = NodeId::from_seed(1);
        let remote_node = NodeId::from_seed(2);
        let table = RoutingTable::new(local_node.clone());

        // Add local group
        table
            .update_group_route(
                ConsensusGroupId::new(1),
                vec![local_node.clone()],
                Some(local_node.clone()),
            )
            .await
            .unwrap();

        // Add remote group
        table
            .update_group_route(
                ConsensusGroupId::new(2),
                vec![remote_node.clone()],
                Some(remote_node.clone()),
            )
            .await
            .unwrap();

        // Check locations
        assert!(
            table
                .is_group_local(ConsensusGroupId::new(1))
                .await
                .unwrap()
        );
        assert!(
            !table
                .is_group_local(ConsensusGroupId::new(2))
                .await
                .unwrap()
        );

        assert_eq!(
            table
                .get_group_location(ConsensusGroupId::new(1))
                .await
                .unwrap(),
            GroupLocation::Local
        );
        assert_eq!(
            table
                .get_group_location(ConsensusGroupId::new(2))
                .await
                .unwrap(),
            GroupLocation::Remote
        );
    }
}
