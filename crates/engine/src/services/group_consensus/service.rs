//! Group consensus service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::LogStorage;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::config::GroupConsensusConfig;
use crate::{
    consensus::group::GroupConsensusLayer,
    error::{ConsensusError, ConsensusResult, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::event::{Event, EventPublisher},
};

/// Group consensus service
pub struct GroupConsensusService<T, G, L>
where
    T: Transport,
    G: TopologyAdaptor,
    L: LogStorage,
{
    /// Configuration
    config: GroupConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Storage
    storage: L,
    /// Group consensus layers
    groups: Arc<RwLock<std::collections::HashMap<ConsensusGroupId, Arc<GroupConsensusLayer<L>>>>>,
    /// Event publisher
    event_publisher: Option<EventPublisher>,
}

impl<T, G, L> GroupConsensusService<T, G, L>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    L: LogStorage + 'static,
{
    /// Create new group consensus service
    pub fn new(
        config: GroupConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage: L,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage,
            groups: Arc::new(RwLock::new(std::collections::HashMap::new())),
            event_publisher: None,
        }
    }

    /// Set event publisher
    pub fn with_event_publisher(mut self, publisher: EventPublisher) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    /// Start the service
    pub async fn start(&self) -> ConsensusResult<()> {
        // Register handlers
        self.register_message_handlers().await?;
        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    /// Create a new group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        // TODO: Implement group creation

        // Emit event
        if let Some(ref publisher) = self.event_publisher {
            let _ = publisher
                .publish(
                    Event::GroupConsensusInitialized {
                        group_id,
                        node_id: self.node_id.clone(),
                        members,
                    },
                    "group_consensus".to_string(),
                )
                .await;
        }

        Ok(())
    }

    /// Register message handlers
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        // TODO: Register group consensus message handlers
        Ok(())
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        true
    }
}
