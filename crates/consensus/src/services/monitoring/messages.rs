//! Health and monitoring network messages

use proven_network::message::HandledMessage;
use proven_network::namespace::MessageType;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};

/// Health check request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckRequest;

impl MessageType for HealthCheckRequest {
    fn message_type(&self) -> &'static str {
        "consensus.health_check_request"
    }
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Whether the node is healthy
    pub healthy: bool,
    /// Current leader if known
    pub leader: Option<NodeId>,
}

impl MessageType for HealthResponse {
    fn message_type(&self) -> &'static str {
        "consensus.health_response"
    }
}

impl HandledMessage for HealthCheckRequest {
    type Response = HealthResponse;
}
