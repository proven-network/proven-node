//! Common type aliases for client handlers

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::services::{
    global_consensus::GlobalConsensusService, group_consensus::GroupConsensusService,
    stream::StreamService,
};

/// Type alias for service references
pub type ServiceRef<T> = Arc<RwLock<Option<Arc<T>>>>;

/// Type alias for global consensus service reference
pub type GlobalConsensusRef<T, G, S> = ServiceRef<GlobalConsensusService<T, G, S>>;

/// Type alias for group consensus service reference
pub type GroupConsensusRef<T, G, S> = ServiceRef<GroupConsensusService<T, G, S>>;

/// Type alias for stream service reference
pub type StreamServiceRef<S> = ServiceRef<StreamService<S>>;

/// Type alias for network manager reference
pub type NetworkManagerRef<T, G> = ServiceRef<NetworkManager<T, G>>;
