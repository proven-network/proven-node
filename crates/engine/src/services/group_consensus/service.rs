//! Group consensus service

use std::num::NonZero;
use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::{
    ConsensusStorage, LogStorage, StorageAdaptor, StorageManager, StorageNamespace,
};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::{callbacks, config::GroupConsensusConfig};
use crate::{
    consensus::group::GroupConsensusLayer,
    error::{ConsensusResult, Error, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::event::bus::EventBus,
    services::group_consensus::GroupConsensusEvent,
    services::stream::StreamService,
};
use async_trait::async_trait;

/// Type alias for the consensus layers
type ConsensusLayers<S> = Arc<
    RwLock<
        std::collections::HashMap<ConsensusGroupId, Arc<GroupConsensusLayer<ConsensusStorage<S>>>>,
    >,
>;

/// Group consensus service
pub struct GroupConsensusService<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Configuration
    config: GroupConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
    /// Group consensus layers
    groups: ConsensusLayers<S>,
    /// Event bus
    event_bus: Arc<EventBus>,
    /// Topology manager
    topology_manager: Option<Arc<proven_topology::TopologyManager<G>>>,
    /// Stream service for message persistence
    stream_service: Arc<RwLock<Option<Arc<StreamService<S>>>>>,
}

impl<T, G, S> GroupConsensusService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create new group consensus service
    pub fn new(
        config: GroupConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage_manager: Arc<StorageManager<S>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage_manager,
            groups: Arc::new(RwLock::new(std::collections::HashMap::new())),
            event_bus,
            topology_manager: None,
            stream_service: Arc::new(RwLock::new(None)),
        }
    }

    /// Set topology manager
    pub fn with_topology(mut self, topology: Arc<proven_topology::TopologyManager<G>>) -> Self {
        self.topology_manager = Some(topology);
        self
    }

    /// Set stream service
    pub async fn set_stream_service(&self, stream_service: Arc<StreamService<S>>) {
        *self.stream_service.write().await = Some(stream_service);
    }

    /// Start the service
    pub async fn start(&self) -> ConsensusResult<()> {
        // Register handlers
        self.register_message_handlers().await?;

        // Event handling is done via handlers registered on the event bus

        // Register event subscribers
        let subscriber =
            crate::services::group_consensus::subscribers::GlobalConsensusSubscriber::new(
                Arc::new(self.clone()),
                self.node_id.clone(),
            );

        self.event_bus.subscribe(subscriber).await;

        tracing::info!("GroupConsensusService: Registered subscriber for GlobalConsensusEvents");

        // Restore any existing groups from storage
        self.restore_persisted_groups().await?;

        Ok(())
    }

    /// Restore persisted groups from storage
    async fn restore_persisted_groups(&self) -> ConsensusResult<()> {
        // For now, we don't have a way to enumerate all persisted groups
        // This would require storage to keep track of which groups exist
        // The groups will be restored when the global state sync callback tries to create them
        tracing::debug!("Checking for persisted groups to restore");
        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
        // Shutdown all Raft instances in groups
        let mut groups = self.groups.write().await;
        for (group_id, layer) in groups.iter() {
            // Shutdown the Raft instance to release all resources with timeout
            match tokio::time::timeout(std::time::Duration::from_secs(5), layer.shutdown()).await {
                Ok(Ok(())) => {
                    tracing::debug!(
                        "Raft instance for group {:?} shut down successfully",
                        group_id
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!(
                        "Failed to shutdown Raft instance for group {:?}: {}",
                        group_id,
                        e
                    );
                }
                Err(_) => {
                    tracing::error!(
                        "Timeout while shutting down Raft instance for group {:?}",
                        group_id
                    );
                }
            }
        }

        // Clear all groups to release storage references
        groups.clear();
        drop(groups);

        use super::messages::GroupConsensusMessage;
        if let Err(e) = self
            .network_manager
            .unregister_service::<GroupConsensusMessage>()
            .await
        {
            tracing::warn!("Failed to unregister group consensus service: {}", e);
        } else {
            tracing::debug!("Group consensus service handler unregistered");
        }

        Ok(())
    }

    /// Create a new group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        // Check if this node is a member of the group
        if !members.contains(&self.node_id) {
            tracing::debug!(
                "Node {} is not a member of group {:?}, skipping creation",
                self.node_id,
                group_id
            );
            return Ok(());
        }

        // Check if group already exists
        {
            let groups = self.groups.read().await;
            if groups.contains_key(&group_id) {
                tracing::debug!("Group {:?} already exists, skipping creation", group_id);
                return Ok(());
            }
        }

        // Create the group consensus layer
        use super::adaptor::GroupNetworkFactory;
        use crate::consensus::group::GroupConsensusLayer;
        use openraft::Config;

        let raft_config = Config {
            cluster_name: format!("group-{:?}-{}", group_id, self.node_id),
            election_timeout_min: self.config.election_timeout_min.as_millis() as u64,
            election_timeout_max: self.config.election_timeout_max.as_millis() as u64,
            heartbeat_interval: self.config.heartbeat_interval.as_millis() as u64,
            max_payload_entries: self.config.max_entries_per_append.get(),
            // TODO: Think about snapshotting
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        };

        let network_factory = GroupNetworkFactory::new(self.network_manager.clone(), group_id);

        // Create callbacks for this group
        let callbacks = Arc::new(callbacks::GroupConsensusCallbacksImpl::<S>::new(
            group_id,
            self.node_id.clone(),
            Some(self.event_bus.clone()),
        ));

        let layer = Arc::new(
            GroupConsensusLayer::new(
                self.node_id.clone(),
                group_id,
                raft_config,
                network_factory,
                self.storage_manager.consensus_storage(),
                callbacks,
            )
            .await?,
        );

        // Store the layer
        self.groups.write().await.insert(group_id, layer.clone());

        // Start event monitoring for this group
        Self::start_group_event_monitoring(
            layer.clone(),
            self.event_bus.clone(),
            self.node_id.clone(),
            group_id,
        );

        // Initialize the group (we already checked that we're a member)
        // Check if we have persisted state for this group
        let has_persisted_state = self.has_persisted_state(group_id).await;

        tracing::info!(
            "Group {:?} initialization check - has_persisted_state: {}",
            group_id,
            has_persisted_state
        );

        if !has_persisted_state {
            // Fresh start - only ONE node should initialize to avoid election storms
            tracing::info!(
                "No persisted state found for group {}, checking if we should initialize",
                group_id
            );

            // Sort members by ID to ensure deterministic selection
            let mut sorted_members = members.clone();
            sorted_members.sort();

            // Only the first member (lowest ID) initializes the cluster
            if sorted_members.first() == Some(&self.node_id) {
                tracing::info!(
                    "This node ({}) has the lowest ID in group {}, initializing cluster",
                    self.node_id,
                    group_id
                );

                // Get member nodes from topology
                let topology = match &self.topology_manager {
                    Some(tm) => tm,
                    None => {
                        tracing::error!("No topology manager available for group {}", group_id);
                        return Err(Error::with_context(
                            ErrorKind::Configuration,
                            format!("No topology manager available for group {group_id}"),
                        ));
                    }
                };

                let all_nodes = match topology.provider().get_topology().await {
                    Ok(nodes) => nodes,
                    Err(e) => {
                        tracing::error!("Failed to get topology for group {}: {}", group_id, e);
                        return Err(Error::with_context(
                            ErrorKind::Network,
                            format!("Failed to get topology: {e}"),
                        ));
                    }
                };

                let mut raft_members = std::collections::BTreeMap::new();
                for member_id in &members {
                    if let Some(node) = all_nodes.iter().find(|n| n.node_id == *member_id) {
                        raft_members.insert(member_id.clone(), node.clone());
                    } else {
                        tracing::error!(
                            "Member {} not found in topology for group {}",
                            member_id,
                            group_id
                        );
                        return Err(Error::with_context(
                            ErrorKind::Configuration,
                            format!(
                                "Member {member_id} not found in topology for group {group_id}"
                            ),
                        ));
                    }
                }

                tracing::info!(
                    "Initializing group {} Raft cluster with {} members",
                    group_id,
                    raft_members.len()
                );

                // Initialize the Raft cluster
                if let Err(e) = layer.raft().initialize(raft_members).await {
                    tracing::error!(
                        "Failed to initialize group {} Raft cluster: {}",
                        group_id,
                        e
                    );
                    return Err(Error::with_context(
                        ErrorKind::Consensus,
                        format!("Failed to initialize group Raft: {e}"),
                    ));
                }

                tracing::info!("Successfully initialized group {} Raft cluster", group_id);
            } else {
                tracing::info!(
                    "This node ({}) is not the initializer for group {}, waiting for leader contact",
                    self.node_id,
                    group_id
                );
                // Don't initialize - just start up and wait for the leader to contact us
            }
        } else {
            // Resuming from persisted state
            tracing::info!(
                "Group {} resuming from persisted state - skipping initialization",
                group_id
            );
        }

        Ok(())
    }

    /// Check if we have persisted consensus state for a group
    async fn has_persisted_state(&self, group_id: ConsensusGroupId) -> bool {
        // Check if we have a vote or committed index in storage
        let consensus_storage = self.storage_manager.consensus_storage();
        let namespace = StorageNamespace::new(format!("group_{}_logs", group_id.value()));

        // Check for vote
        if let Ok(Some(_)) = consensus_storage.get_metadata(&namespace, "vote").await {
            return true;
        }

        // Check for committed
        if let Ok(Some(_)) = consensus_storage
            .get_metadata(&namespace, "committed")
            .await
        {
            return true;
        }

        // Check for log entries
        if let Ok(Some(_)) = consensus_storage.bounds(&namespace).await {
            return true;
        }

        false
    }

    /// Register message handlers
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        use super::messages::{GroupConsensusMessage, GroupConsensusServiceResponse};
        use crate::consensus::group::raft::GroupRaftMessageHandler;

        let groups = self.groups.clone();

        // Register the service handler
        self.network_manager
            .register_service::<GroupConsensusMessage, _>(move |_sender, message| {
                let groups = groups.clone();
                Box::pin(async move {
                    match message {
                        GroupConsensusMessage::Vote { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                            let resp = handler
                                .handle_vote(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::Vote {
                                group_id,
                                response: resp,
                            })
                        }
                        GroupConsensusMessage::AppendEntries { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                            let resp = handler
                                .handle_append_entries(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::AppendEntries {
                                group_id,
                                response: resp,
                            })
                        }
                        GroupConsensusMessage::InstallSnapshot { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                            let resp = handler
                                .handle_install_snapshot(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::InstallSnapshot {
                                group_id,
                                response: resp,
                            })
                        }
                        GroupConsensusMessage::Consensus { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let resp = layer
                                .submit_request(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::Consensus {
                                group_id,
                                response: resp,
                            })
                        }
                    }
                })
            })
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;

        Ok(())
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        true
    }

    /// Submit a request to a specific group
    pub async fn submit_to_group(
        &self,
        group_id: ConsensusGroupId,
        request: crate::consensus::group::GroupRequest,
    ) -> ConsensusResult<crate::consensus::group::GroupResponse> {
        let groups = self.groups.read().await;
        let layer = groups.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        let response = layer.submit_request(request.clone()).await?;

        // Handle response for event publishing
        // Event publishing is now handled by callbacks

        Ok(response)
    }

    /// Get stream state from a specific group
    pub async fn get_stream_state(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::consensus::group::state::StreamState>> {
        let groups = self.groups.read().await;
        let layer = groups.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        let state = layer.state();
        let stream_name = crate::services::stream::StreamName::new(stream_name);
        Ok(state.get_stream(&stream_name).await)
    }

    /// Get all group IDs this node is a member of
    pub async fn get_node_groups(&self) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let groups = self.groups.read().await;
        // For now, return all groups that this node has joined
        // TODO: Properly track membership through Raft state
        Ok(groups.keys().cloned().collect())
    }

    /// Get group state information
    pub async fn get_group_state_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<super::types::GroupStateInfo> {
        use std::time::SystemTime;

        let groups = self.groups.read().await;
        let layer = groups.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        // Get state from the layer
        let state = layer.state();

        // Get Raft metrics to find leader and term
        let metrics_rx = layer.metrics();
        let (current_leader, current_term) = {
            let raft_metrics = metrics_rx.borrow();
            (
                raft_metrics.current_leader.clone(),
                raft_metrics.current_term,
            )
        };
        // raft_metrics is now dropped, so we can safely await

        // Collect stream information
        let stream_names = state.list_streams().await;
        let mut stream_infos = Vec::new();
        let mut total_messages = 0u64;
        let mut total_bytes = 0u64;

        for stream_name in stream_names {
            if let Some(stream_state) = state.get_stream(&stream_name).await {
                let info = super::types::StreamInfo {
                    name: stream_name.to_string(),
                    message_count: stream_state.stats.message_count,
                    next_sequence: stream_state.next_sequence,
                    first_sequence: stream_state.first_sequence,
                    total_bytes: stream_state.stats.total_bytes,
                    last_update: stream_state.stats.last_update,
                };
                total_messages += stream_state.stats.message_count;
                total_bytes += stream_state.stats.total_bytes;
                stream_infos.push(info);
            }
        }

        // TODO: Get actual members from Raft membership config
        // For now, assume all nodes with this group are members
        let members = vec![self.node_id.clone()];
        let is_member = true;

        Ok(super::types::GroupStateInfo {
            group_id,
            members,
            leader: current_leader,
            term: current_term,
            is_member,
            streams: stream_infos,
            total_messages,
            total_bytes,
            created_at: SystemTime::now(), // TODO: Track actual creation time
            last_updated: SystemTime::now(),
        })
    }

    /// Start monitoring a group consensus layer for events
    fn start_group_event_monitoring<L>(
        layer: Arc<GroupConsensusLayer<L>>,
        event_bus: Arc<EventBus>,
        _node_id: NodeId,
        group_id: ConsensusGroupId,
    ) where
        L: LogStorage + 'static,
    {
        // Monitor for leader changes
        let mut metrics_rx = layer.metrics();
        let event_bus_clone = event_bus.clone();
        let group_id_clone = group_id;

        tokio::spawn(async move {
            let mut current_leader: Option<NodeId> = None;
            let mut current_term: u64 = 0;

            loop {
                tokio::select! {
                    Ok(_) = metrics_rx.changed() => {
                        let metrics = metrics_rx.borrow().clone();

                        // Check for leader change
                        if metrics.current_leader != current_leader || metrics.current_term != current_term {
                            let old_leader = current_leader.clone();
                            current_leader = metrics.current_leader.clone();
                            current_term = metrics.current_term;

                            if let Some(new_leader) = &current_leader {
                                tracing::info!(
                                    "Group {} consensus leader changed: {:?} -> {} (term {})",
                                    group_id_clone, old_leader, new_leader, current_term
                                );

                                // Publish event
                                let event = GroupConsensusEvent::LeaderChanged {
                                    group_id: group_id_clone,
                                    old_leader,
                                    new_leader: Some(new_leader.clone()),
                                };

                                event_bus_clone.publish(event).await;
                            }
                        }
                    }
                    else => break,
                }
            }

            tracing::debug!("Group {} leader monitoring task stopped", group_id_clone);
        });
    }
}

// Implement Clone for the service
impl<T, G, S> Clone for GroupConsensusService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id.clone(),
            network_manager: self.network_manager.clone(),
            storage_manager: self.storage_manager.clone(),
            groups: self.groups.clone(),
            event_bus: self.event_bus.clone(),
            topology_manager: self.topology_manager.clone(),
            stream_service: self.stream_service.clone(),
        }
    }
}
