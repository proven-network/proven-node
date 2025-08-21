//! Group consensus service

use proven_attestation::Attestor;
use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::{LogStorage, StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::config::GroupConsensusConfig;
use super::types::{ConsensusLayers, States};
use crate::foundation::GroupStateRead;
use crate::{
    consensus::group::GroupConsensusLayer,
    error::{ConsensusResult, Error, ErrorKind},
    foundation::events::EventBus,
    foundation::routing::RoutingTable,
    foundation::types::ConsensusGroupId,
    services::group_consensus::events::GroupConsensusEvent,
};

/// Group consensus service
pub struct GroupConsensusService<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
    A: Attestor,
{
    /// Configuration
    config: GroupConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G, A>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
    /// Group states (read-only access)
    group_states: States,
    /// Group consensus layers
    consensus_layers: ConsensusLayers<S>,
    /// Event bus
    event_bus: Arc<EventBus>,
    /// Topology manager
    topology_manager: Arc<proven_topology::TopologyManager<G>>,
    /// Routing table
    routing_table: Arc<RoutingTable>,
}

impl<T, G, A, S> GroupConsensusService<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Create new group consensus service
    pub fn new(
        config: GroupConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G, A>>,
        storage_manager: Arc<StorageManager<S>>,
        event_bus: Arc<EventBus>,
        routing_table: Arc<RoutingTable>,
        topology_manager: Arc<proven_topology::TopologyManager<G>>,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage_manager,
            group_states: Arc::new(RwLock::new(std::collections::HashMap::new())),
            consensus_layers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            event_bus,
            topology_manager,
            routing_table,
        }
    }

    /// Start the service
    pub async fn start(&self) -> ConsensusResult<()> {
        // Register handlers
        self.register_message_handlers().await?;

        // Register command handlers
        use crate::services::group_consensus::command_handlers::*;
        use crate::services::group_consensus::commands::*;

        let service_arc = Arc::new(self.clone());

        // Register EnsureGroupConsensusInitialized handler
        let ensure_group_handler = EnsureGroupConsensusInitializedHandler::new(
            self.node_id,
            self.consensus_layers.clone(),
            self.group_states.clone(),
            self.network_manager.clone(),
            self.storage_manager.clone(),
            self.topology_manager.clone(),
            self.event_bus.clone(),
            self.routing_table.clone(),
        );
        self.event_bus
            .handle_requests::<EnsureGroupConsensusInitialized, _>(ensure_group_handler)
            .expect("Failed to register EnsureGroupConsensusInitialized handler");

        // Register DissolveGroup handler
        let dissolve_handler = DissolveGroupHandler::new(self.consensus_layers.clone());
        self.event_bus
            .handle_requests::<DissolveGroup, _>(dissolve_handler)
            .expect("Failed to register DissolveGroup handler");

        // Register EnsureStreamInitializedInGroup handler
        let ensure_stream_handler = EnsureStreamInitializedInGroupHandler::new(
            self.node_id,
            self.consensus_layers.clone(),
            self.event_bus.clone(),
        );
        self.event_bus
            .handle_requests::<EnsureStreamInitializedInGroup, _>(ensure_stream_handler)
            .expect("Failed to register EnsureStreamInitializedInGroup handler");

        // Register SubmitToGroup handler
        let submit_handler = SubmitToGroupHandler::new(
            self.node_id,
            self.consensus_layers.clone(),
            self.routing_table.clone(),
            self.network_manager.clone(),
        );
        self.event_bus
            .handle_requests::<SubmitToGroup, _>(submit_handler)
            .expect("Failed to register SubmitToGroup handler");

        // Register GetNodeGroups handler
        let get_groups_handler = GetNodeGroupsHandler::new(self.consensus_layers.clone());
        self.event_bus
            .handle_requests::<GetNodeGroups, _>(get_groups_handler)
            .expect("Failed to register GetNodeGroups handler");

        // Register GetGroupInfo handler
        let get_info_handler =
            GetGroupInfoHandler::new(self.consensus_layers.clone(), self.group_states.clone());
        self.event_bus
            .handle_requests::<GetGroupInfo, _>(get_info_handler)
            .expect("Failed to register GetGroupInfo handler");

        // Register GetStreamState handler
        let get_stream_state_handler = GetStreamStateHandler::new(
            self.node_id,
            self.consensus_layers.clone(),
            self.group_states.clone(),
            self.routing_table.clone(),
            self.network_manager.clone(),
        );
        self.event_bus
            .handle_requests::<GetStreamState, _>(get_stream_state_handler)
            .expect("Failed to register GetStreamState handler");

        // Register GetGroupState handler
        let get_group_state_handler = GetGroupStateHandler::new(service_arc.clone());
        self.event_bus
            .handle_requests::<GetGroupState, _>(get_group_state_handler)
            .expect("Failed to register GetGroupState handler");

        tracing::info!("GroupConsensusService: Registered command handlers");

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

        // Unregister from network service
        let _ = self
            .network_manager
            .unregister_service("group_consensus")
            .await;

        // Unregister all event handlers to allow re-registration on restart
        use crate::services::group_consensus::commands::*;
        let _ = self
            .event_bus
            .unregister_request_handler::<EnsureGroupConsensusInitialized>();
        let _ = self.event_bus.unregister_request_handler::<DissolveGroup>();
        let _ = self
            .event_bus
            .unregister_request_handler::<EnsureStreamInitializedInGroup>();
        let _ = self.event_bus.unregister_request_handler::<SubmitToGroup>();
        let _ = self.event_bus.unregister_request_handler::<GetNodeGroups>();
        let _ = self.event_bus.unregister_request_handler::<GetGroupInfo>();
        let _ = self
            .event_bus
            .unregister_request_handler::<GetStreamState>();
        let _ = self.event_bus.unregister_request_handler::<GetGroupState>();

        Ok(())
    }

    /// Register message handlers
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        use super::handler::GroupConsensusHandler;

        // Create and register the service handler
        let handler =
            GroupConsensusHandler::new(self.consensus_layers.clone(), self.group_states.clone());
        self.network_manager
            .register_service(handler)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;

        Ok(())
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        true
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

        // Get Raft info to find leader and term
        let current_leader = layer.get_leader();
        let current_term = layer.get_current_term();

        // Collect stream information
        let stream_names = state.list_streams().await;
        let mut stream_names_vec = Vec::new();
        let mut total_messages = 0u64;
        let mut total_bytes = 0u64;

        for stream_name in stream_names {
            if let Some(stream_state) = state.get_stream(&stream_name).await {
                total_messages += stream_state.stats.message_count;
                total_bytes += stream_state.stats.total_bytes;
                stream_names_vec.push(stream_name);
            }
        }

        // TODO: Get actual members from Raft membership config
        // For now, assume all nodes with this group are members
        let members = vec![self.node_id];
        let is_member = true;

        Ok(super::types::GroupStateInfo {
            group_id,
            members,
            leader: current_leader,
            term: current_term,
            is_member,
            stream_names: stream_names_vec,
            total_messages,
            total_bytes,
            created_at: SystemTime::now(), // TODO: Track actual creation time
            last_updated: SystemTime::now(),
        })
    }

    /// Start monitoring a group consensus layer for events
    pub(crate) fn start_group_event_monitoring<L>(
        layer: Arc<GroupConsensusLayer<L>>,
        event_bus: Arc<EventBus>,
        _node_id: NodeId,
        group_id: ConsensusGroupId,
        _routing_table: Arc<RoutingTable>,
    ) where
        L: LogStorage + 'static,
    {
        // Start leader monitoring with callback
        layer.start_leader_monitoring(group_id, move |group_id, old_leader, new_leader, _term| {
            if let Some(new_leader) = new_leader {
                // Publish event
                let event = GroupConsensusEvent::LeaderChanged {
                    group_id,
                    old_leader,
                    new_leader: Some(new_leader),
                };

                event_bus.emit(event);
            }
        });
    }
}

// Implement Clone for the service
impl<T, G, A, S> Clone for GroupConsensusService<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
    A: Attestor + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id,
            network_manager: self.network_manager.clone(),
            storage_manager: self.storage_manager.clone(),
            group_states: self.group_states.clone(),
            consensus_layers: self.consensus_layers.clone(),
            event_bus: self.event_bus.clone(),
            topology_manager: self.topology_manager.clone(),
            routing_table: self.routing_table.clone(),
        }
    }
}
