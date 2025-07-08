//! Group discovery and formation for local consensus groups
//!
//! This module handles discovering which local consensus groups a node belongs to
//! by querying the global consensus state, and coordinating group formation.

use super::LocalConsensusManager;
use crate::allocation::ConsensusGroupId;
use crate::error::ConsensusResult;
use crate::global::{GlobalManager, GlobalOperation};
use crate::node_id::NodeId;
use proven_attestation::Attestor;
use proven_governance::Governance;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Service that discovers and manages local group membership
pub struct GroupDiscoveryService<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Node ID
    node_id: NodeId,
    /// Reference to the global consensus manager
    global_manager: Arc<GlobalManager<G, A>>,
    /// Reference to the local consensus manager
    local_manager: Arc<LocalConsensusManager<G, A>>,
    /// Interval for checking group membership
    check_interval: Duration,
}

impl<G, A> GroupDiscoveryService<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Create a new group discovery service
    pub fn new(
        node_id: NodeId,
        global_manager: Arc<GlobalManager<G, A>>,
        local_manager: Arc<LocalConsensusManager<G, A>>,
        check_interval: Duration,
    ) -> Self {
        Self {
            node_id,
            global_manager,
            local_manager,
            check_interval,
        }
    }

    /// Start the group discovery service
    pub async fn start(self: Arc<Self>) -> ConsensusResult<()> {
        info!("Starting group discovery service for node {}", self.node_id);

        // Do initial discovery
        self.discover_and_join_groups().await?;

        // Start background task for periodic checks
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(service.check_interval);
            loop {
                interval.tick().await;
                if let Err(e) = service.discover_and_join_groups().await {
                    error!("Error during group discovery: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Discover which groups this node belongs to and join them
    async fn discover_and_join_groups(&self) -> ConsensusResult<()> {
        debug!("Discovering group membership for node {}", self.node_id);

        // Query global consensus for groups this node belongs to
        let groups = match self.query_node_groups().await {
            Ok(groups) => groups,
            Err(e) => {
                warn!("Failed to query group membership: {}", e);
                return Ok(()); // Don't fail, will retry on next interval
            }
        };

        info!("Node {} belongs to {} groups", self.node_id, groups.len());

        // Get currently managed groups
        let current_groups = self.local_manager.get_managed_groups().await;

        // Join new groups
        for group_info in &groups {
            if !current_groups.contains(&group_info.id) {
                info!("Joining new group {:?}", group_info.id);
                if let Err(e) = self.join_group(group_info.id, &group_info.members).await {
                    error!("Failed to join group {:?}: {}", group_info.id, e);
                }
            }
        }

        // Leave groups we're no longer part of
        let active_group_ids: Vec<_> = groups.iter().map(|g| g.id).collect();
        for group_id in current_groups {
            if !active_group_ids.contains(&group_id) {
                info!("Leaving group {:?}", group_id);
                if let Err(e) = self.local_manager.remove_group(group_id).await {
                    error!("Failed to leave group {:?}: {}", group_id, e);
                }
            }
        }

        Ok(())
    }

    /// Query global consensus for groups this node belongs to
    async fn query_node_groups(
        &self,
    ) -> ConsensusResult<Vec<crate::global::state_machine::ConsensusGroupInfo>> {
        // Query the global manager for groups this node belongs to
        Ok(self.global_manager.query_node_groups(&self.node_id).await)
    }

    /// Join a local consensus group
    async fn join_group(
        &self,
        group_id: ConsensusGroupId,
        members: &[crate::NodeId],
    ) -> ConsensusResult<()> {
        // Members are already NodeIds, no conversion needed
        let member_nodes = members.to_vec();

        // Create the group (storage is now created internally by the factory)
        self.local_manager
            .create_group(group_id, member_nodes)
            .await?;

        // If we're the first member, initialize the group
        // In production, we'd coordinate with other members
        if members.first() == Some(&self.node_id) {
            // Create node info
            // In production, this would come from topology
            let governance_node = proven_governance::GovernanceNode {
                public_key: *self.node_id.verifying_key(), // Get VerifyingKey from NodeId
                origin: "http://localhost:8080".to_string(), // Placeholder
                region: "us-east-1".to_string(),           // Placeholder
                availability_zone: "us-east-1a".to_string(), // Placeholder
                specializations: std::collections::HashSet::new(), // No specializations for now
            };
            let node_info = crate::node::Node::from(governance_node);

            self.local_manager
                .initialize_single_node_group(group_id, node_info)
                .await?;
        }

        Ok(())
    }
}

/// Create a global consensus operation to form a new local group
pub fn create_add_group_operation(
    group_id: ConsensusGroupId,
    members: Vec<NodeId>,
) -> GlobalOperation {
    GlobalOperation::AddConsensusGroup { group_id, members }
}
