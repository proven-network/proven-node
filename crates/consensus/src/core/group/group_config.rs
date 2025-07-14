//! Configuration types for consensus groups
//!
//! This module provides configuration structures for creating and managing
//! consensus groups with proper encapsulation of settings.

use openraft::Config as RaftConfig;
use proven_governance::Governance;
use std::sync::Arc;

use crate::{
    core::group::{GroupStorageFactory, UnifiedGroupStorage},
    core::state_machine::group::GroupStateMachine,
    core::stream::StreamStorageBackend,
};
use proven_network::NetworkManager;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Transport;

/// Configuration for creating a new consensus group
#[derive(Clone)]
pub struct GroupConfig {
    /// The node ID that owns this group
    pub node_id: NodeId,
    /// Base Raft configuration for groups
    pub raft_config: Arc<RaftConfig>,
    /// Storage backend for streams
    pub stream_storage_backend: StreamStorageBackend,
}

/// Dependencies required for group operations
pub struct GroupDependencies<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Storage factory for creating group storage
    pub storage_factory: Box<
        dyn GroupStorageFactory<
                Storage = UnifiedGroupStorage,
                LogStorage = UnifiedGroupStorage,
                StateMachine = GroupStateMachine,
            >,
    >,
    /// Network manager for Raft communication
    pub network_manager: Arc<NetworkManager<T, G>>,
    /// Topology manager for node information
    pub topology_manager: Arc<TopologyManager<G>>,
}

/// Builder for creating group configurations
pub struct GroupConfigBuilder {
    node_id: Option<NodeId>,
    raft_config: Option<Arc<RaftConfig>>,
    stream_storage_backend: Option<StreamStorageBackend>,
}

impl GroupConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            node_id: None,
            raft_config: None,
            stream_storage_backend: None,
        }
    }

    /// Set the node ID
    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Set the Raft configuration
    pub fn raft_config(mut self, config: Arc<RaftConfig>) -> Self {
        self.raft_config = Some(config);
        self
    }

    /// Set the stream storage backend
    pub fn stream_storage_backend(mut self, backend: StreamStorageBackend) -> Self {
        self.stream_storage_backend = Some(backend);
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<GroupConfig, &'static str> {
        Ok(GroupConfig {
            node_id: self.node_id.ok_or("node_id is required")?,
            raft_config: self.raft_config.ok_or("raft_config is required")?,
            stream_storage_backend: self
                .stream_storage_backend
                .ok_or("stream_storage_backend is required")?,
        })
    }
}

impl Default for GroupConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
