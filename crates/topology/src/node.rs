//! Node ID type for consensus system

use std::{collections::HashSet, net::SocketAddr};

use ed25519_dalek::VerifyingKey;
use proven_governance::{GovernanceNode, NodeSpecialization};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::NodeId;

/// Node ID type for consensus system
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Node {
    inner: GovernanceNode,
    node_id: NodeId,
}

impl Node {
    /// Create a new node from a GovernanceNode
    pub fn new(node: GovernanceNode) -> Self {
        let node_id = NodeId::new(node.public_key);
        Node {
            inner: node,
            node_id,
        }
    }

    /// Get the public key of this node
    pub fn public_key(&self) -> VerifyingKey {
        self.inner.public_key
    }

    /// Get the node ID of this node
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the origin URL of this node
    pub fn origin(&self) -> &str {
        &self.inner.origin
    }

    /// Get the availability zone of this node
    pub fn availability_zone(&self) -> &str {
        &self.inner.availability_zone
    }

    /// Get the region of this node
    pub fn region(&self) -> &str {
        &self.inner.region
    }

    /// Get the specializations of this node
    pub fn specializations(&self) -> HashSet<NodeSpecialization> {
        self.inner.specializations.clone()
    }

    /// Get a reference to the underlying GovernanceNode
    pub fn as_governance_node(&self) -> &GovernanceNode {
        &self.inner
    }

    /// Create a TCP socket address from this node's origin URL
    ///
    /// Parses the origin URL, extracts host and port, resolves DNS, and returns
    /// the first resolved socket address suitable for TCP connections.
    pub async fn tcp_socket_addr(&self) -> Result<SocketAddr, String> {
        // Parse the origin URL to extract host and port
        let url = Url::parse(&self.inner.origin)
            .map_err(|e| format!("Invalid origin URL '{}': {}", self.inner.origin, e))?;

        let host = url
            .host_str()
            .ok_or_else(|| format!("No host in origin URL '{}'", self.inner.origin))?;

        let port = url.port().unwrap_or_else(|| {
            match url.scheme() {
                "https" => 443,
                "http" => 80,
                _ => 80, // Default fallback
            }
        });

        // Resolve DNS to get socket address
        let socket_addrs = tokio::net::lookup_host(format!("{host}:{port}"))
            .await
            .map_err(|e| {
                format!(
                    "Failed to resolve host '{}:{}' from origin '{}': {}",
                    host, port, self.inner.origin, e
                )
            })?;

        // Return the first resolved address
        socket_addrs.into_iter().next().ok_or_else(|| {
            format!(
                "No addresses resolved for host '{}:{}' from origin '{}'",
                host, port, self.inner.origin
            )
        })
    }

    /// Create a WebSocket URL from this node's origin URL
    ///
    /// Converts HTTP/HTTPS schemes to WS/WSS, adds the consensus WebSocket endpoint,
    /// and returns the complete WebSocket URL.
    pub fn websocket_url(&self) -> Result<String, String> {
        // Parse the origin URL and convert http/https to ws/wss
        let mut url = Url::parse(&self.inner.origin)
            .map_err(|e| format!("Invalid origin URL '{}': {}", self.inner.origin, e))?;

        // Convert HTTP schemes to WebSocket schemes
        let current_scheme = url.scheme().to_string();
        let ws_scheme = match current_scheme.as_str() {
            "http" => "ws",
            "https" => "wss",
            "ws" | "wss" => &current_scheme, // Already a WebSocket URL
            scheme => {
                return Err(format!(
                    "Unsupported scheme '{}' in origin '{}'. Expected http, https, ws, or wss",
                    scheme, self.inner.origin
                ));
            }
        };

        url.set_scheme(ws_scheme).map_err(|_| {
            format!(
                "Failed to set WebSocket scheme for origin '{}'",
                self.inner.origin
            )
        })?;

        // Add the WebSocket endpoint path
        // Ensure no double slashes by trimming trailing slash from URL
        let base_url = url.as_str().trim_end_matches('/');
        Ok(format!("{base_url}/consensus/ws"))
    }
}

impl From<GovernanceNode> for Node {
    fn from(node: GovernanceNode) -> Self {
        Node::new(node)
    }
}

impl From<Node> for GovernanceNode {
    fn from(node: Node) -> Self {
        node.inner
    }
}
