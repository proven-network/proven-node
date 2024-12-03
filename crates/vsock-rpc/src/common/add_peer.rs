use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

/// A request to add a new peer to the server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddPeerRequest {
    /// IP address of the peer.
    pub peer_ip: Ipv4Addr,

    /// NATS port of the peer.
    pub peer_port: u16,
}

/// The response to an add peer request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddPeerResponse {
    /// Whether the peer was successfully added.
    pub success: bool,
}
