use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddPeerRequest {
    pub peer_ip: Ipv4Addr,
    pub peer_port: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddPeerResponse {
    pub success: bool,
}
