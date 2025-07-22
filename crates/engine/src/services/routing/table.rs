//! Routing table implementation

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::foundation::ConsensusGroupId;

use super::types::*;

/// Routing table entry
#[derive(Debug, Clone)]
pub struct RouteEntry {
    /// Route information
    pub route: StreamRoute,
    /// Expiration time
    pub expires_at: SystemTime,
}

/// Routing table for stream to group mappings
pub struct RoutingTable {
    /// Stream routes
    stream_routes: Arc<RwLock<HashMap<String, RouteEntry>>>,
    /// Group routes
    group_routes: Arc<RwLock<HashMap<ConsensusGroupId, GroupRoute>>>,
    /// Global consensus leader
    global_leader: Arc<RwLock<Option<proven_topology::NodeId>>>,
    /// Cache TTL
    cache_ttl: Duration,
}

impl RoutingTable {
    /// Create a new routing table
    pub fn new(cache_ttl: Duration) -> Self {
        Self {
            stream_routes: Arc::new(RwLock::new(HashMap::new())),
            group_routes: Arc::new(RwLock::new(HashMap::new())),
            global_leader: Arc::new(RwLock::new(None)),
            cache_ttl,
        }
    }

    /// Update stream route
    pub async fn update_stream_route(
        &self,
        stream_name: String,
        route: StreamRoute,
    ) -> RoutingResult<()> {
        let mut routes = self.stream_routes.write().await;
        let entry = RouteEntry {
            route,
            expires_at: SystemTime::now() + self.cache_ttl,
        };
        routes.insert(stream_name.clone(), entry);
        info!("Updated route for stream {}", stream_name);
        Ok(())
    }

    /// Remove stream route
    pub async fn remove_stream_route(&self, stream_name: &str) -> RoutingResult<()> {
        let mut routes = self.stream_routes.write().await;
        routes.remove(stream_name);
        info!("Removed route for stream {}", stream_name);
        Ok(())
    }

    /// Get stream route
    pub async fn get_stream_route(&self, stream_name: &str) -> RoutingResult<Option<StreamRoute>> {
        let routes = self.stream_routes.read().await;

        if let Some(entry) = routes.get(stream_name) {
            // Check if expired
            if SystemTime::now() > entry.expires_at {
                debug!("Route for stream {} has expired", stream_name);
                drop(routes);
                // Remove expired entry
                let mut routes = self.stream_routes.write().await;
                routes.remove(stream_name);
                return Ok(None);
            }
            Ok(Some(entry.route.clone()))
        } else {
            Ok(None)
        }
    }

    /// Update group route
    pub async fn update_group_route(
        &self,
        group_id: ConsensusGroupId,
        route: GroupRoute,
    ) -> RoutingResult<()> {
        let mut routes = self.group_routes.write().await;
        routes.insert(group_id, route);
        debug!("Updated route for group {:?}", group_id);
        Ok(())
    }

    /// Remove group route
    pub async fn remove_group_route(&self, group_id: ConsensusGroupId) -> RoutingResult<()> {
        let mut routes = self.group_routes.write().await;
        routes.remove(&group_id);

        // Also remove any stream routes to this group
        let mut stream_routes = self.stream_routes.write().await;
        stream_routes.retain(|_, entry| entry.route.group_id != group_id);

        info!("Removed route for group {:?}", group_id);
        Ok(())
    }

    /// Get group route
    pub async fn get_group_route(
        &self,
        group_id: ConsensusGroupId,
    ) -> RoutingResult<Option<GroupRoute>> {
        let routes = self.group_routes.read().await;
        Ok(routes.get(&group_id).cloned())
    }

    /// Get all stream routes
    pub async fn get_all_stream_routes(&self) -> RoutingResult<HashMap<String, StreamRoute>> {
        let routes = self.stream_routes.read().await;
        let now = SystemTime::now();

        Ok(routes
            .iter()
            .filter(|(_, entry)| entry.expires_at > now)
            .map(|(name, entry)| (name.clone(), entry.route.clone()))
            .collect())
    }

    /// Get all group routes
    pub async fn get_all_group_routes(
        &self,
    ) -> RoutingResult<HashMap<ConsensusGroupId, GroupRoute>> {
        let routes = self.group_routes.read().await;
        Ok(routes.clone())
    }

    /// Get all routes
    pub async fn get_all_routes(
        &self,
    ) -> RoutingResult<(
        HashMap<String, StreamRoute>,
        HashMap<ConsensusGroupId, GroupRoute>,
    )> {
        let stream_routes = self.get_all_stream_routes().await?;
        let group_routes = self.get_all_group_routes().await?;
        Ok((stream_routes, group_routes))
    }

    /// Clear expired entries
    pub async fn clear_expired(&self) -> RoutingResult<usize> {
        let mut routes = self.stream_routes.write().await;
        let now = SystemTime::now();
        let before = routes.len();

        routes.retain(|_, entry| entry.expires_at > now);

        let removed = before - routes.len();
        if removed > 0 {
            debug!("Cleared {} expired routing entries", removed);
        }

        Ok(removed)
    }

    /// Update global consensus leader
    pub async fn update_global_leader(&self, leader: Option<proven_topology::NodeId>) {
        let mut current_leader = self.global_leader.write().await;
        *current_leader = leader.clone();
        if let Some(ref leader_id) = leader {
            info!("Updated global consensus leader to {}", leader_id);
        } else {
            info!("Cleared global consensus leader");
        }
    }

    /// Get global consensus leader
    pub async fn get_global_leader(&self) -> Option<proven_topology::NodeId> {
        self.global_leader.read().await.clone()
    }

    /// Get streams by group
    pub async fn get_streams_by_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> RoutingResult<Vec<String>> {
        let routes = self.stream_routes.read().await;
        let now = SystemTime::now();

        Ok(routes
            .iter()
            .filter(|(_, entry)| {
                entry.route.group_id == group_id && entry.expires_at > now && entry.route.is_active
            })
            .map(|(name, _)| name.clone())
            .collect())
    }

    /// Count active streams per group
    pub async fn count_streams_per_group(&self) -> RoutingResult<HashMap<ConsensusGroupId, usize>> {
        let routes = self.stream_routes.read().await;
        let now = SystemTime::now();
        let mut counts = HashMap::new();

        for (_, entry) in routes.iter() {
            if entry.expires_at > now && entry.route.is_active {
                *counts.entry(entry.route.group_id).or_insert(0) += 1;
            }
        }

        Ok(counts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stream_route_crud() {
        let table = RoutingTable::new(Duration::from_secs(60));

        let route = StreamRoute {
            stream_name: "test-stream".to_string(),
            group_id: ConsensusGroupId::new(1),
            assigned_at: SystemTime::now(),
            strategy: RoutingStrategy::LeastLoaded,
            is_active: true,
            config: None,
        };

        // Create
        table
            .update_stream_route("test-stream".to_string(), route.clone())
            .await
            .unwrap();

        // Read
        let retrieved = table.get_stream_route("test-stream").await.unwrap();
        assert!(retrieved.is_some());

        // Delete
        table.remove_stream_route("test-stream").await.unwrap();
        let retrieved = table.get_stream_route("test-stream").await.unwrap();
        assert!(retrieved.is_none());
    }
}
