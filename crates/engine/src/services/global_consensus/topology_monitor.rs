//! Topology monitoring for global consensus membership updates

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use openraft::ChangeMembers;
use proven_topology::{NodeId, TopologyAdaptor, TopologyManager, TopologySubscription};

use crate::{
    consensus::global::GlobalConsensusLayer,
    error::{ConsensusResult, Error, ErrorKind},
};
use proven_storage::ConsensusStorage;

/// Type alias for the consensus layer
type ConsensusLayerRef<S> = Arc<RwLock<Option<Arc<GlobalConsensusLayer<ConsensusStorage<S>>>>>>;

/// Type alias for the last known members
type LastKnownMembersRef = Arc<RwLock<BTreeSet<NodeId>>>;

/// Monitors topology changes and proposes Raft membership updates
pub struct TopologyMonitor<G, S>
where
    G: TopologyAdaptor,
    S: proven_storage::StorageAdaptor,
{
    node_id: NodeId,
    topology_manager: Arc<TopologyManager<G>>,
    consensus_layer: ConsensusLayerRef<S>,
    last_known_members: LastKnownMembersRef,
}

impl<G, S> TopologyMonitor<G, S>
where
    G: TopologyAdaptor + 'static,
    S: proven_storage::StorageAdaptor + 'static,
{
    /// Create a new topology monitor
    pub fn new(
        node_id: NodeId,
        topology_manager: Arc<TopologyManager<G>>,
        consensus_layer: ConsensusLayerRef<S>,
    ) -> Self {
        Self {
            node_id,
            topology_manager,
            consensus_layer,
            last_known_members: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }

    /// Start monitoring topology changes
    pub fn start_monitoring(
        self: Arc<Self>,
        task_tracker: &TaskTracker,
        cancellation_token: &CancellationToken,
    ) {
        let token = cancellation_token.clone();

        task_tracker.spawn(async move {
            info!("Starting topology monitor for global consensus");

            // Subscribe to topology changes
            let mut subscription = self.topology_manager.subscribe();

            // Initial delay to let system stabilize
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Do an initial check
            if let Err(e) = self.check_topology_changes().await {
                error!("Error during initial topology check: {}", e);
            }

            loop {
                tokio::select! {
                    Ok(_) = subscription.changed() => {
                        info!("Topology change detected by subscription");
                        if let Err(e) = self.check_topology_changes().await {
                            error!("Error checking topology changes: {}", e);
                        }
                    }
                    _ = token.cancelled() => {
                        info!("Topology monitor shutting down");
                        return;
                    }
                }
            }
        });
    }

    /// Check for topology changes and propose membership updates
    async fn check_topology_changes(&self) -> ConsensusResult<()> {
        // Get consensus layer
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = match consensus_guard.as_ref() {
            Some(c) => c,
            None => {
                return Ok(());
            }
        };

        // Only the leader should propose membership changes
        let is_leader = consensus.is_leader().await;
        if !is_leader {
            debug!("Not the leader, skipping topology check");
            return Ok(());
        }

        // Get current topology from manager
        let topology_nodes = self
            .topology_manager
            .get_cached_nodes()
            .await
            .map_err(|e| Error::network(format!("Failed to get cached nodes: {e}")))?;
        let topology_members: BTreeSet<NodeId> =
            topology_nodes.iter().map(|n| n.node_id.clone()).collect();

        // Get current Raft membership
        let raft_metrics = consensus.metrics();
        let current_membership = {
            let metrics = raft_metrics.borrow();
            // Extract membership from metrics - this is a simplified approach
            // In practice, we'd need to query the actual membership config
            metrics.membership_config.membership().clone()
        };

        // Get current voters and learners
        let current_voters: BTreeSet<NodeId> = current_membership.voter_ids().collect();

        let current_learners: BTreeSet<NodeId> = current_membership.learner_ids().collect();

        let all_current: BTreeSet<NodeId> =
            current_voters.union(&current_learners).cloned().collect();

        // Check if membership has changed
        let last_known = self.last_known_members.read().await;
        if topology_members == *last_known && topology_members == all_current {
            debug!("No topology changes detected");
            return Ok(());
        }
        drop(last_known);

        info!(
            "Topology change detected. Current: {:?}, New: {:?}",
            all_current, topology_members
        );

        // Determine nodes to add and remove
        let nodes_to_add: Vec<NodeId> =
            topology_members.difference(&all_current).cloned().collect();

        let nodes_to_remove: Vec<NodeId> =
            all_current.difference(&topology_members).cloned().collect();

        // First, add new nodes as learners
        for node_id in &nodes_to_add {
            info!("Adding node {} as learner", node_id);

            // Find the node info from topology
            let node_info = topology_nodes
                .iter()
                .find(|n| &n.node_id == node_id)
                .ok_or_else(|| {
                    Error::with_context(
                        ErrorKind::NotFound,
                        format!("Node {node_id} not found in topology"),
                    )
                })?;

            // Add as learner
            match consensus
                .add_learner(node_id.clone(), node_info.clone())
                .await
            {
                Ok(_) => info!("Successfully added {} as learner", node_id),
                Err(e) => {
                    warn!("Failed to add {} as learner: {}", node_id, e);
                    // Continue with other nodes
                }
            }
        }

        // Give learners time to catch up
        if !nodes_to_add.is_empty() {
            info!("Waiting for learners to catch up...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        // Now update membership to include new nodes as voters
        let mut new_voters = current_voters.clone();
        for node_id in nodes_to_add {
            new_voters.insert(node_id);
        }

        // Remove nodes that are no longer in topology
        for node_id in nodes_to_remove {
            new_voters.remove(&node_id);
        }

        // Propose membership change
        if new_voters != current_voters {
            info!("Proposing membership change: {:?}", new_voters);

            match consensus.change_membership(new_voters.clone(), false).await {
                Ok(_) => {
                    info!("Successfully updated membership");

                    // Update last known members
                    let mut last_known = self.last_known_members.write().await;
                    *last_known = new_voters;
                }
                Err(e) => {
                    error!("Failed to update membership: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
