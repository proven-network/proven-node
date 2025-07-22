use crate::consensus::global::raft::GlobalRaftMessageHandler;
use crate::consensus::global::types::GroupInfo;
use crate::consensus::global::{GlobalConsensusLayer, GlobalRequest};
use crate::foundation::types::ConsensusGroupId;
use crate::services::event::{EventHandler, EventPriority};
use crate::services::membership::MembershipEvent;
use proven_logger::{debug, error, info, warn};
use proven_storage::{StorageAdaptor, StorageManager};
use proven_topology::{NodeId, TopologyAdaptor, TopologyManager};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type alias for the consensus layer
type ConsensusLayer<S> =
    Arc<RwLock<Option<Arc<GlobalConsensusLayer<proven_storage::ConsensusStorage<S>>>>>>;

/// Subscriber for membership events in global consensus
pub struct MembershipEventSubscriber<G, S>
where
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Reference to global consensus layer
    consensus_layer: ConsensusLayer<S>,
    /// Topology manager to get node information
    topology_manager: Option<Arc<TopologyManager<G>>>,
}

impl<G, S> MembershipEventSubscriber<G, S>
where
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Create a new membership event subscriber
    pub fn new(
        node_id: NodeId,
        consensus_layer: ConsensusLayer<S>,
        _storage_manager: Arc<StorageManager<S>>,
        topology_manager: Option<Arc<TopologyManager<G>>>,
    ) -> Self {
        Self {
            node_id,
            consensus_layer,
            topology_manager,
        }
    }
}

#[async_trait::async_trait]
impl<G, S> EventHandler<MembershipEvent> for MembershipEventSubscriber<G, S>
where
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    fn priority(&self) -> EventPriority {
        // Handle membership events synchronously to ensure initialization happens before other services
        EventPriority::Critical
    }

    async fn handle(&self, event: MembershipEvent) {
        match event {
            MembershipEvent::ClusterFormed {
                members,
                coordinator,
            } => {
                info!(
                    "MembershipEventSubscriber: Cluster formed with {} members, coordinator: {}",
                    members.len(),
                    coordinator
                );

                // Check if we have a consensus layer
                let consensus_guard = self.consensus_layer.read().await;
                if let Some(consensus) = consensus_guard.as_ref() {
                    // Check if Raft is already initialized
                    if consensus.is_initialized().await {
                        info!("Raft is already initialized, skipping initialization");
                    } else {
                        // Not initialized - initialize if we're the coordinator
                        if self.node_id == coordinator {
                            info!(
                                "We are the coordinator and Raft is not initialized, initializing Raft cluster"
                            );

                            // Get actual node information from topology manager
                            let mut raft_members = std::collections::BTreeMap::new();

                            if let Some(topology_mgr) = &self.topology_manager {
                                // Get node information for each member
                                for member_id in &members {
                                    if let Some(node_info) = topology_mgr.get_node(member_id).await
                                    {
                                        raft_members.insert(member_id.clone(), node_info);
                                    } else {
                                        error!("Could not find node info for member {member_id}");
                                        return;
                                    }
                                }
                            } else {
                                error!("No topology manager available to get node information");
                                return;
                            }

                            // Initialize cluster
                            let handler: &dyn GlobalRaftMessageHandler = consensus.as_ref();
                            if let Err(e) = handler.initialize_cluster(raft_members).await {
                                error!("Failed to initialize Raft cluster: {e}");
                                return;
                            }

                            info!("Successfully initialized Raft cluster");

                            // For single-node clusters, wait for the node to become leader
                            if members.len() == 1 {
                                let start = std::time::Instant::now();
                                let timeout = std::time::Duration::from_secs(5);

                                loop {
                                    if consensus.is_leader().await {
                                        info!("Single node became leader after initialization");
                                        break;
                                    }

                                    if start.elapsed() > timeout {
                                        error!("Timeout waiting for single node to become leader");
                                        return;
                                    }

                                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                }
                            }
                        } else {
                            info!("Not the coordinator, waiting for Raft sync");
                        }
                    }

                    // Now handle default group creation
                    // Check if default group already exists
                    let state = consensus.state();
                    let default_group_id = ConsensusGroupId::new(1);

                    if state.get_group(&default_group_id).await.is_some() {
                        debug!("Default group already exists");
                        return;
                    }

                    // Check if we're the leader (may have changed after initialization)
                    if !consensus.is_leader().await {
                        debug!("Not the leader, skipping default group creation");
                        return;
                    }

                    info!("Creating default group as the global consensus leader");

                    // Create the default group with all cluster members
                    let create_group_request = GlobalRequest::CreateGroup {
                        info: GroupInfo {
                            id: default_group_id,
                            members: members.clone(),
                            created_at: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            metadata: Default::default(),
                        },
                    };

                    match consensus.submit_request(create_group_request).await {
                        Ok(_) => {
                            info!("Successfully created default group through consensus");
                        }
                        Err(e) => {
                            error!("Failed to create default group: {e}");
                        }
                    }
                } else {
                    warn!("Global consensus layer not initialized when handling ClusterFormed");
                }
            }
            _ => {
                // Ignore other membership events
            }
        }
    }
}
