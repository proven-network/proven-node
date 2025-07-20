//! WAL command definitions for S3 storage

use bytes::Bytes;
use proven_vsock_rpc::RpcMessage;
use serde::{Deserialize, Serialize};

/// Request to append log entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendLogsRequest {
    pub namespace: String,
    pub entries: Vec<(u64, Vec<u8>)>,
    pub batch_id: String,
}

/// Response to append logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendLogsResponse {
    pub success: bool,
    pub error: Option<String>,
}

impl RpcMessage for AppendLogsRequest {
    type Response = AppendLogsResponse;

    fn message_id(&self) -> &'static str {
        "wal.append_logs"
    }
}

impl TryInto<Bytes> for AppendLogsRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for AppendLogsRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for AppendLogsResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for AppendLogsResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

/// Request to confirm batch upload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmBatchRequest {
    pub batch_id: String,
}

/// Response to confirm batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmBatchResponse {
    pub success: bool,
    pub error: Option<String>,
}

impl RpcMessage for ConfirmBatchRequest {
    type Response = ConfirmBatchResponse;

    fn message_id(&self) -> &'static str {
        "wal.confirm_batch"
    }
}

impl TryInto<Bytes> for ConfirmBatchRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for ConfirmBatchRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for ConfirmBatchResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for ConfirmBatchResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

/// Request to sync WAL
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    pub force: bool,
}

/// Response to sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    pub success: bool,
    pub error: Option<String>,
}

impl RpcMessage for SyncRequest {
    type Response = SyncResponse;

    fn message_id(&self) -> &'static str {
        "wal.sync"
    }
}

impl TryInto<Bytes> for SyncRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for SyncRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for SyncResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for SyncResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

/// Request to get pending batches
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GetPendingBatchesRequest {
    pub namespace_filter: Option<String>,
}

/// A pending batch in WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingBatch {
    pub batch_id: String,
    pub namespace: String,
    pub entries: Vec<(u64, Vec<u8>)>,
    pub created_at: u64, // Unix timestamp
}

/// Response with pending batches
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPendingBatchesResponse {
    pub batches: Vec<PendingBatch>,
    pub error: Option<String>,
}

impl RpcMessage for GetPendingBatchesRequest {
    type Response = GetPendingBatchesResponse;

    fn message_id(&self) -> &'static str {
        "wal.get_pending_batches"
    }
}

impl TryInto<Bytes> for GetPendingBatchesRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for GetPendingBatchesRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for GetPendingBatchesResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for GetPendingBatchesResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

/// Request to set metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetMetadataRequest {
    pub namespace: String,
    pub key: String,
    pub value: Vec<u8>,
}

/// Response to set metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetMetadataResponse {
    pub success: bool,
    pub error: Option<String>,
}

impl RpcMessage for SetMetadataRequest {
    type Response = SetMetadataResponse;

    fn message_id(&self) -> &'static str {
        "wal.set_metadata"
    }
}

impl TryInto<Bytes> for SetMetadataRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for SetMetadataRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for SetMetadataResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for SetMetadataResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}
