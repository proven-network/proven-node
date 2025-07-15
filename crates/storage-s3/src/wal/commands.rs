//! WAL command definitions for S3 storage

use proven_vsock_rpc::RpcMessage;
use serde::{Deserialize, Serialize};

/// WAL command envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalCommand {
    /// Append log entries to WAL
    AppendLogs(AppendLogsRequest),
    /// Confirm batch was uploaded to S3
    ConfirmBatch(ConfirmBatchRequest),
    /// Sync WAL to disk
    Sync(SyncRequest),
    /// Get pending batches for recovery
    GetPendingBatches(GetPendingBatchesRequest),
}

/// WAL response envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalResponse {
    /// Response to append logs
    AppendLogs(AppendLogsResponse),
    /// Response to confirm batch
    ConfirmBatch(ConfirmBatchResponse),
    /// Response to sync
    Sync(SyncResponse),
    /// Response with pending batches
    GetPendingBatches(GetPendingBatchesResponse),
}

impl RpcMessage for WalCommand {
    type Response = WalResponse;

    fn message_id(&self) -> &'static str {
        match self {
            Self::AppendLogs(_) => "wal.append_logs",
            Self::ConfirmBatch(_) => "wal.confirm_batch",
            Self::Sync(_) => "wal.sync",
            Self::GetPendingBatches(_) => "wal.get_pending_batches",
        }
    }
}

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
