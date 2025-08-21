//! Messages for host-enclave time communication.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use proven_vsock_rpc::RpcMessage;
use serde::{Deserialize, Serialize};

/// Message ID for time RPC.
pub const TIME_RPC_MESSAGE_ID: &str = "hlc.time.request";

/// Time request from enclave to host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRequest;

/// Time response from host to enclave.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeResponse {
    /// Stratum level
    pub stratum: u8,
    /// Current time from host
    pub timestamp: DateTime<Utc>,
    /// Uncertainty in microseconds
    pub uncertainty_us: f64,
}

// Implement RpcMessage for TimeRequest
impl RpcMessage for TimeRequest {
    type Response = TimeResponse;

    fn message_id(&self) -> &'static str {
        TIME_RPC_MESSAGE_ID
    }
}

impl TryInto<Bytes> for TimeRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for TimeRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for TimeResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for TimeResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}
