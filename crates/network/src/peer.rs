//! Peer abstraction for network communication

use crate::error::NetworkResult;
use crate::manager::NetworkManager;
use crate::message::{HandledMessage, NetworkMessage};
use proven_governance::Governance;
use proven_topology::{Node, NodeId};
use proven_transport::Transport;
use std::sync::Arc;
use std::time::Duration;

/// A peer in the network - wrapper around Node with network operations
#[derive(Clone)]
pub struct Peer<T, G>
where
    T: Transport,
    G: Governance,
{
    /// The underlying node information
    node: Node,
    /// Reference to the network manager for sending messages
    manager: Arc<NetworkManager<T, G>>,
}

impl<T, G> Peer<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new peer
    pub(crate) fn new(node: Node, manager: Arc<NetworkManager<T, G>>) -> Self {
        Self { node, manager }
    }

    /// Get the peer's node ID
    pub fn node_id(&self) -> &NodeId {
        self.node.node_id()
    }

    /// Get the peer's origin URL
    pub fn origin(&self) -> &str {
        self.node.origin()
    }

    /// Get the underlying node
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Send a typed message to this peer
    pub async fn send<M>(&self, message: M) -> NetworkResult<()>
    where
        M: NetworkMessage,
    {
        self.manager
            .send(self.node.node_id().clone(), message)
            .await
    }

    /// Send a typed request and wait for typed response
    pub async fn request<M>(&self, message: M, timeout: Duration) -> NetworkResult<M::Response>
    where
        M: HandledMessage,
        M::Response: NetworkMessage + serde::de::DeserializeOwned,
    {
        self.manager
            .request(self.node.node_id().clone(), message, timeout)
            .await
    }
}
