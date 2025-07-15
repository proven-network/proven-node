//! Load balancer implementation

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::foundation::ConsensusGroupId;

use super::types::*;

/// Load balancer for routing decisions
pub struct LoadBalancer {
    /// Load information by group
    load_info: Arc<RwLock<HashMap<ConsensusGroupId, LoadInfo>>>,
    /// Default strategy
    default_strategy: RoutingStrategy,
    /// Load thresholds
    thresholds: LoadThresholds,
    /// Round-robin state
    round_robin_state: Arc<RwLock<RoundRobinState>>,
}

/// Round-robin state
struct RoundRobinState {
    /// Last selected index
    last_index: usize,
}

impl LoadBalancer {
    /// Create a new load balancer
    pub fn new(default_strategy: RoutingStrategy, thresholds: LoadThresholds) -> Self {
        Self {
            load_info: Arc::new(RwLock::new(HashMap::new())),
            default_strategy,
            thresholds,
            round_robin_state: Arc::new(RwLock::new(RoundRobinState { last_index: 0 })),
        }
    }

    /// Update load information for a group
    pub async fn update_load(
        &self,
        group_id: ConsensusGroupId,
        load_info: LoadInfo,
    ) -> RoutingResult<()> {
        let mut loads = self.load_info.write().await;
        loads.insert(group_id, load_info);
        Ok(())
    }

    /// Get load information for a group
    pub async fn get_load(&self, group_id: ConsensusGroupId) -> RoutingResult<Option<LoadInfo>> {
        let loads = self.load_info.read().await;
        Ok(loads.get(&group_id).cloned())
    }

    /// Get all load information
    pub async fn get_all_load_info(&self) -> RoutingResult<Vec<LoadInfo>> {
        let loads = self.load_info.read().await;
        Ok(loads.values().cloned().collect())
    }

    /// Select a group based on the current strategy
    pub async fn select_group(
        &self,
        available_groups: Vec<ConsensusGroupId>,
    ) -> RoutingResult<ConsensusGroupId> {
        if available_groups.is_empty() {
            return Err(RoutingError::NoAvailableGroups);
        }

        if available_groups.len() == 1 {
            return Ok(available_groups[0]);
        }

        match self.default_strategy {
            RoutingStrategy::LeastLoaded => self.select_least_loaded(available_groups).await,
            RoutingStrategy::RoundRobin => self.select_round_robin(available_groups).await,
            RoutingStrategy::HashBased => self.select_hash_based(available_groups).await,
            RoutingStrategy::GeoProximity => self.select_geo_proximity(available_groups).await,
            RoutingStrategy::Sticky => {
                // Sticky routing is handled at a higher level
                self.select_least_loaded(available_groups).await
            }
        }
    }

    /// Select the least loaded group
    async fn select_least_loaded(
        &self,
        available_groups: Vec<ConsensusGroupId>,
    ) -> RoutingResult<ConsensusGroupId> {
        let loads = self.load_info.read().await;

        let mut best_group = available_groups[0];
        let mut best_score = f32::MAX;

        for group_id in &available_groups {
            let score = loads.get(group_id).map(|l| l.load_score).unwrap_or(0.0);

            if score < best_score {
                best_score = score;
                best_group = *group_id;
            }
        }

        // Check if best group is overloaded
        if best_score > self.thresholds.high_load {
            warn!(
                "All available groups are highly loaded (best score: {})",
                best_score
            );
        }

        debug!(
            "Selected least loaded group {:?} with score {}",
            best_group, best_score
        );

        Ok(best_group)
    }

    /// Select using round-robin
    async fn select_round_robin(
        &self,
        available_groups: Vec<ConsensusGroupId>,
    ) -> RoutingResult<ConsensusGroupId> {
        let mut state = self.round_robin_state.write().await;

        state.last_index = (state.last_index + 1) % available_groups.len();
        let selected = available_groups[state.last_index];

        debug!("Selected group {:?} using round-robin", selected);
        Ok(selected)
    }

    /// Select based on hash (simple implementation)
    async fn select_hash_based(
        &self,
        available_groups: Vec<ConsensusGroupId>,
    ) -> RoutingResult<ConsensusGroupId> {
        // Simple hash-based selection using current time
        let hash = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize;

        let index = hash % available_groups.len();
        let selected = available_groups[index];

        debug!("Selected group {:?} using hash-based routing", selected);
        Ok(selected)
    }

    /// Select based on geographic proximity (placeholder)
    async fn select_geo_proximity(
        &self,
        available_groups: Vec<ConsensusGroupId>,
    ) -> RoutingResult<ConsensusGroupId> {
        // Placeholder - would need actual geo information
        warn!("Geo-proximity routing not implemented, falling back to least loaded");
        self.select_least_loaded(available_groups).await
    }

    /// Check if rebalancing is needed
    pub async fn needs_rebalancing(&self) -> RoutingResult<bool> {
        let loads = self.load_info.read().await;

        if loads.len() < 2 {
            return Ok(false);
        }

        let load_scores: Vec<f32> = loads.values().map(|l| l.load_score).collect();
        let avg_load = load_scores.iter().sum::<f32>() / load_scores.len() as f32;

        // Check for significant imbalance
        for score in &load_scores {
            let deviation = (score - avg_load).abs() / avg_load;
            if deviation > self.thresholds.rebalance_threshold {
                debug!(
                    "Rebalancing needed: load deviation {} exceeds threshold {}",
                    deviation, self.thresholds.rebalance_threshold
                );
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get groups that should be preferred for new streams
    pub async fn get_preferred_groups(&self) -> RoutingResult<Vec<ConsensusGroupId>> {
        let loads = self.load_info.read().await;

        Ok(loads
            .iter()
            .filter(|(_, load)| load.load_score < self.thresholds.medium_load)
            .map(|(id, _)| *id)
            .collect())
    }

    /// Get overloaded groups
    pub async fn get_overloaded_groups(&self) -> RoutingResult<Vec<ConsensusGroupId>> {
        let loads = self.load_info.read().await;

        Ok(loads
            .iter()
            .filter(|(_, load)| load.load_score > self.thresholds.high_load)
            .map(|(id, _)| *id)
            .collect())
    }
}

/// Load strategy for decision making
#[derive(Debug, Clone, Copy)]
pub enum LoadStrategy {
    /// Optimize for even distribution
    EvenDistribution,
    /// Optimize for resource utilization
    ResourceUtilization,
    /// Optimize for latency
    MinimizeLatency,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_least_loaded_selection() {
        let balancer = LoadBalancer::new(RoutingStrategy::LeastLoaded, LoadThresholds::default());

        // Add load info
        balancer
            .update_load(
                ConsensusGroupId::new(1),
                LoadInfo {
                    group_id: ConsensusGroupId::new(1),
                    stream_count: 10,
                    messages_per_sec: 100.0,
                    storage_bytes: 1000000,
                    cpu_usage: 0.5,
                    memory_usage: 0.4,
                    load_score: 0.5,
                },
            )
            .await
            .unwrap();

        balancer
            .update_load(
                ConsensusGroupId::new(2),
                LoadInfo {
                    group_id: ConsensusGroupId::new(2),
                    stream_count: 5,
                    messages_per_sec: 50.0,
                    storage_bytes: 500000,
                    cpu_usage: 0.3,
                    memory_usage: 0.2,
                    load_score: 0.3,
                },
            )
            .await
            .unwrap();

        let available = vec![ConsensusGroupId::new(1), ConsensusGroupId::new(2)];
        let selected = balancer.select_group(available).await.unwrap();

        assert_eq!(selected, ConsensusGroupId::new(2)); // Should select less loaded
    }
}
