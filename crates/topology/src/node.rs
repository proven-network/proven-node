//! Node type for topology management

use std::{collections::HashSet, net::SocketAddr};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{NodeId, NodeSpecialization};

/// A node in the network topology
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Node {
    /// The availability zone of the node.
    pub availability_zone: String,

    /// The origin of the node.
    pub origin: String,

    /// The public key of the node.
    pub node_id: NodeId,

    /// The region of the node.
    pub region: String,

    /// Any specializations of the node.
    pub specializations: HashSet<NodeSpecialization>,
}

impl Node {
    /// Create a new node
    pub fn new(
        availability_zone: String,
        origin: String,
        node_id: NodeId,
        region: String,
        specializations: HashSet<NodeSpecialization>,
    ) -> Self {
        Node {
            availability_zone,
            origin,
            node_id,
            region,
            specializations,
        }
    }

    /// Get the node ID of this node
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the origin URL of this node
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Get the availability zone of this node
    pub fn availability_zone(&self) -> &str {
        &self.availability_zone
    }

    /// Get the region of this node
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Get the specializations of this node
    pub fn specializations(&self) -> HashSet<NodeSpecialization> {
        self.specializations.clone()
    }

    /// Create a TCP socket address from this node's origin URL
    ///
    /// Parses the origin URL, extracts host and port, resolves DNS, and returns
    /// the first resolved socket address suitable for TCP connections.
    pub async fn tcp_socket_addr(&self) -> Result<SocketAddr, String> {
        // Parse the origin URL to extract host and port
        let url = Url::parse(&self.origin)
            .map_err(|e| format!("Invalid origin URL '{}': {}", self.origin, e))?;

        let host = url
            .host_str()
            .ok_or_else(|| format!("No host in origin URL '{}'", self.origin))?;

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
                    host, port, self.origin, e
                )
            })?;

        // Return the first resolved address
        socket_addrs.into_iter().next().ok_or_else(|| {
            format!(
                "No addresses resolved for host '{}:{}' from origin '{}'",
                host, port, self.origin
            )
        })
    }

    /// Create a WebSocket URL from this node's origin URL
    ///
    /// Converts HTTP/HTTPS schemes to WS/WSS, adds the consensus WebSocket endpoint,
    /// and returns the complete WebSocket URL.
    pub fn websocket_url(&self) -> Result<String, String> {
        // Parse the origin URL and convert http/https to ws/wss
        let mut url = Url::parse(&self.origin)
            .map_err(|e| format!("Invalid origin URL '{}': {}", self.origin, e))?;

        // Convert HTTP schemes to WebSocket schemes
        let current_scheme = url.scheme().to_string();
        let ws_scheme = match current_scheme.as_str() {
            "http" => "ws",
            "https" => "wss",
            "ws" | "wss" => &current_scheme, // Already a WebSocket URL
            scheme => {
                return Err(format!(
                    "Unsupported scheme '{}' in origin '{}'. Expected http, https, ws, or wss",
                    scheme, self.origin
                ));
            }
        };

        url.set_scheme(ws_scheme).map_err(|_| {
            format!(
                "Failed to set WebSocket scheme for origin '{}'",
                self.origin
            )
        })?;

        // Add the WebSocket endpoint path
        // Ensure no double slashes by trimming trailing slash from URL
        let base_url = url.as_str().trim_end_matches('/');
        Ok(format!("{base_url}/consensus/ws"))
    }
}
