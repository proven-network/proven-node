//! Health and monitoring network messages

use proven_network::{NetworkMessage, ServiceMessage};
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};

/// Monitoring service message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum MonitoringServiceMessage {
    /// Health check request
    HealthCheckRequest,
}

/// Monitoring service response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum MonitoringServiceResponse {
    /// Health check response
    HealthResponse {
        /// Whether the node is healthy
        healthy: bool,
        /// Current leader if known
        leader: Option<NodeId>,
    },
}

impl NetworkMessage for MonitoringServiceMessage {
    fn message_type() -> &'static str {
        "monitoring_message"
    }
}

impl NetworkMessage for MonitoringServiceResponse {
    fn message_type() -> &'static str {
        "monitoring_response"
    }
}

impl ServiceMessage for MonitoringServiceMessage {
    type Response = MonitoringServiceResponse;

    fn service_id() -> &'static str {
        "monitoring"
    }
}
