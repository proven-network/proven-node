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
use tracing::info;

use super::{callbacks, config::GroupConsensusConfig};
use crate::foundation::{
    GroupState, GroupStateRead, GroupStateReader, GroupStateWriter, StreamState,
    create_group_state_access,
};
use crate::{
    consensus::group::GroupConsensusLayer,
    error::{ConsensusResult, Error, ErrorKind},
    foundation::events::EventBus,
    foundation::types::ConsensusGroupId,
    services::group_consensus::events::GroupConsensusEvent,
    services::stream::StreamService,
};
use async_trait::async_trait;

/// Type alias for the consensus layers
type ConsensusLayers<S> = Arc<
    RwLock<
        std::collections::HashMap<ConsensusGroupId, Arc<GroupConsensusLayer<ConsensusStorage<S>>>>,
    >,
>;

type States = Arc<RwLock<std::collections::HashMap<ConsensusGroupId, GroupStateReader>>>;

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
    /// Group states (read-only access)
    group_states: States,
    /// Group consensus layers
    consensus_layers: ConsensusLayers<S>,
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
            group_states: Arc::new(RwLock::new(std::collections::HashMap::new())),
            consensus_layers: Arc::new(RwLock::new(std::collections::HashMap::new())),
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

        // Register command handlers
        use crate::services::group_consensus::command_handlers::global_consensus::*;
        use crate::services::group_consensus::commands::*;

        let service_arc = Arc::new(self.clone());

        // Register CreateGroup handler
        let create_handler = CreateGroupHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<CreateGroup, _>(create_handler)
            .expect("Failed to register CreateGroup handler");

        // Register DissolveGroup handler
        let dissolve_handler = DissolveGroupHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<DissolveGroup, _>(dissolve_handler)
            .expect("Failed to register DissolveGroup handler");

        // Register InitializeStreamInGroup handler
        let init_stream_handler = InitializeStreamInGroupHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<InitializeStreamInGroup, _>(init_stream_handler)
            .expect("Failed to register InitializeStreamInGroup handler");

        // Register SubmitToGroup handler
        let submit_handler = SubmitToGroupHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<SubmitToGroup, _>(submit_handler)
            .expect("Failed to register SubmitToGroup handler");

        // Register GetNodeGroups handler
        let get_groups_handler = GetNodeGroupsHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<GetNodeGroups, _>(get_groups_handler)
            .expect("Failed to register GetNodeGroups handler");

        // Register GetGroupInfo handler
        let get_info_handler = GetGroupInfoHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<GetGroupInfo, _>(get_info_handler)
            .expect("Failed to register GetGroupInfo handler");

        // Register GetStreamState handler
        let get_stream_state_handler = GetStreamStateHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<GetStreamState, _>(get_stream_state_handler)
            .expect("Failed to register GetStreamState handler");

        tracing::info!("GroupConsensusService: Registered command handlers");

        // Register event handlers
        use crate::services::global_consensus::events::GlobalConsensusEvent;
        use crate::services::group_consensus::subscribers::global::GlobalConsensusSubscriber;

        // Subscribe to global consensus events
        let global_subscriber = GlobalConsensusSubscriber::new(service_arc, self.node_id.clone());
        self.event_bus
            .subscribe::<GlobalConsensusEvent, _>(global_subscriber);

        tracing::info!("GroupConsensusService: Registered event handlers");

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
        let mut groups = self.consensus_layers.write().await;
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
            let groups = self.consensus_layers.read().await;
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
            max_payload_entries: self.config.max_entries_per_append as u64,
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

        let (state_reader, state_writer) = create_group_state_access();
        self.group_states
            .write()
            .await
            .insert(group_id, state_reader.clone());

        let layer = Arc::new(
            GroupConsensusLayer::new(
                self.node_id.clone(),
                group_id,
                raft_config,
                network_factory,
                self.storage_manager.consensus_storage(),
                callbacks,
                state_writer,
            )
            .await?,
        );

        // Store the layer
        self.consensus_layers
            .write()
            .await
            .insert(group_id, layer.clone());

        // Start event monitoring for this group
        Self::start_group_event_monitoring(
            layer.clone(),
            self.event_bus.clone(),
            self.node_id.clone(),
            group_id,
        );

        // Initialize the group (we already checked that we're a member)
        // Check if Raft is already initialized for this group
        let is_initialized = layer.is_initialized().await;

        info!("Group {group_id:?} initialization check - is_initialized: {is_initialized}");

        if !is_initialized {
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
            // Already initialized
            info!("Group {group_id} is already initialized - skipping initialization");
        }

        Ok(())
    }

    /// Register message handlers
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        use super::messages::{GroupConsensusMessage, GroupConsensusServiceResponse};
        use crate::consensus::group::raft::GroupRaftMessageHandler;

        let groups = self.consensus_layers.clone();

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
        let groups = self.consensus_layers.read().await;
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
    ) -> ConsensusResult<Option<StreamState>> {
        let groups = self.consensus_layers.read().await;
        let _layer = groups.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        // Get the state from our own states map
        let states = self.group_states.read().await;
        let state = states.get(&group_id).ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("State for group {group_id} not found"),
            )
        })?;

        let stream_name = crate::services::stream::StreamName::new(stream_name);
        Ok(state.get_stream(&stream_name).await)
    }

    /// Get all group IDs this node is a member of
    pub async fn get_node_groups(&self) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let groups = self.consensus_layers.read().await;
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

        let groups = self.consensus_layers.read().await;
        let layer = groups.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        // Get state from the layer
        let state = {
            let states_guard = self.group_states.read().await;
            states_guard.get(&group_id).cloned().ok_or_else(|| {
                Error::with_context(
                    ErrorKind::NotFound,
                    format!("State for group {group_id} not found"),
                )
            })?
        };

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

                                event_bus_clone.emit(event);
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
            group_states: self.group_states.clone(),
            consensus_layers: self.consensus_layers.clone(),
            event_bus: self.event_bus.clone(),
            topology_manager: self.topology_manager.clone(),
            stream_service: self.stream_service.clone(),
        }
    }
}
