//! VSOCK WAL client for durability

use crate::storage_backends::types::{StorageError, StorageResult};
use bytes::Bytes;
use proven_vsock_rpc::{RpcClient, RpcMessage, VsockAddr};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error};
use uuid::Uuid;

use super::{config::WalConfig, encryption::EncryptionManager};

/// WAL entry identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalEntryId {
    /// Unique ID for the WAL entry
    pub id: Uuid,
    /// Offset within the WAL
    pub offset: u64,
}

/// WAL entry with data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    /// Entry ID
    pub id: WalEntryId,
    /// Entry data
    pub data: Bytes,
    /// Data checksum
    pub checksum: u32,
}

/// Pending batch recovered from WAL
#[derive(Clone, Debug)]
pub struct PendingBatch {
    pub entries: Vec<WalEntry>,
    pub created_at: u64,
}

/// WAL append request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalAppendRequest {
    pub id: Uuid,
    pub encrypted_data: Vec<u8>,
    pub checksum: u32,
}

/// WAL append response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalAppendResponse {
    pub id: Uuid,
    pub offset: u64,
}

impl RpcMessage for WalAppendRequest {
    type Response = WalAppendResponse;

    fn message_id(&self) -> &'static str {
        "wal.append"
    }
}

/// WAL read request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalReadRequest {
    pub id: Uuid,
}

/// WAL read response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalReadResponse {
    pub id: Uuid,
    pub encrypted_data: Vec<u8>,
    pub checksum: u32,
}

impl RpcMessage for WalReadRequest {
    type Response = WalReadResponse;

    fn message_id(&self) -> &'static str {
        "wal.read"
    }
}

/// WAL sync request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSyncRequest {
    pub up_to_id: Uuid,
}

/// WAL sync response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSyncResponse {
    pub synced: bool,
}

impl RpcMessage for WalSyncRequest {
    type Response = WalSyncResponse;

    fn message_id(&self) -> &'static str {
        "wal.sync"
    }
}

/// WAL mark uploaded request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalMarkUploadedRequest {
    pub ids: Vec<Uuid>,
}

/// WAL mark uploaded response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalMarkUploadedResponse {
    pub marked_count: u32,
}

impl RpcMessage for WalMarkUploadedRequest {
    type Response = WalMarkUploadedResponse;

    fn message_id(&self) -> &'static str {
        "wal.mark_uploaded"
    }
}

/// WAL list pending request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalListPendingRequest {
    pub after: Option<Uuid>,
    pub limit: usize,
}

/// WAL list pending response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalListPendingResponse {
    pub entries: Vec<WalEntry>,
}

impl RpcMessage for WalListPendingRequest {
    type Response = WalListPendingResponse;

    fn message_id(&self) -> &'static str {
        "wal.list_pending"
    }
}

/// WAL cleanup request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalCleanupRequest {
    pub before_id: Uuid,
}

/// WAL cleanup response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalCleanupResponse {
    pub cleaned_count: u32,
}

impl RpcMessage for WalCleanupRequest {
    type Response = WalCleanupResponse;

    fn message_id(&self) -> &'static str {
        "wal.cleanup"
    }
}

/// WAL client for communicating with host-side WAL server
pub struct WalClient {
    #[allow(dead_code)]
    config: WalConfig,
    encryptor: Arc<EncryptionManager>,
    rpc_client: RpcClient,
}

impl WalClient {
    /// Create a new WAL client
    pub async fn new(config: &WalConfig, encryptor: Arc<EncryptionManager>) -> StorageResult<Self> {
        debug!(
            "Initializing WAL client on VSOCK port {}",
            config.vsock_port
        );

        // Create VSOCK address (CID 2 is the host)
        let vsock_addr = VsockAddr::new(2, config.vsock_port);

        // Build RPC client
        let rpc_client = RpcClient::builder()
            .vsock_addr(vsock_addr)
            .default_timeout(config.request_timeout)
            .build()
            .map_err(|e| StorageError::Backend(format!("Failed to create RPC client: {e}")))?;

        Ok(Self {
            config: config.clone(),
            encryptor,
            rpc_client,
        })
    }

    /// Append data to WAL
    pub async fn append(&self, data: &[u8]) -> StorageResult<WalEntryId> {
        let id = Uuid::new_v4();

        // Encrypt data before sending
        let encrypted = self.encryptor.encrypt_for_wal(data).await?;
        let checksum = crc32fast::hash(&encrypted);

        let request = WalAppendRequest {
            id,
            encrypted_data: encrypted.to_vec(),
            checksum,
        };

        debug!("Appending {} bytes to WAL with id {}", data.len(), id);

        // Send request via vsock-rpc
        let response = self.rpc_client.request(request).await.map_err(|e| {
            error!("WAL append failed: {}", e);
            StorageError::Backend(format!("WAL append failed: {e}"))
        })?;

        Ok(WalEntryId {
            id: response.id,
            offset: response.offset,
        })
    }

    /// Read entry from WAL
    pub async fn read(&self, id: WalEntryId) -> StorageResult<Vec<u8>> {
        let request = WalReadRequest { id: id.id };

        debug!("Reading WAL entry {}", id.id);

        // Send request via vsock-rpc
        let response = self.rpc_client.request(request).await.map_err(|e| {
            error!("WAL read failed: {}", e);
            StorageError::KeyNotFound(format!("WAL entry {} not found: {}", id.id, e))
        })?;

        // Verify checksum
        let actual_checksum = crc32fast::hash(&response.encrypted_data);
        if actual_checksum != response.checksum {
            return Err(StorageError::InvalidValue(format!(
                "WAL entry {} checksum mismatch: expected {}, got {}",
                id.id, response.checksum, actual_checksum
            )));
        }

        // Decrypt data
        let decrypted = self
            .encryptor
            .decrypt_from_wal(&response.encrypted_data)
            .await?;

        Ok(decrypted.to_vec())
    }

    /// Sync WAL up to a specific entry
    pub async fn sync(&self, up_to: WalEntryId) -> StorageResult<()> {
        let request = WalSyncRequest { up_to_id: up_to.id };

        debug!("Syncing WAL up to {}", up_to.id);

        // Send request via vsock-rpc
        let response = self.rpc_client.request(request).await.map_err(|e| {
            error!("WAL sync failed: {}", e);
            StorageError::Backend(format!("WAL sync failed: {e}"))
        })?;

        if !response.synced {
            return Err(StorageError::Backend("WAL sync failed".to_string()));
        }

        Ok(())
    }

    /// Mark entries as uploaded to S3
    pub async fn mark_uploaded(&self, ids: &[WalEntryId]) -> StorageResult<()> {
        let request = WalMarkUploadedRequest {
            ids: ids.iter().map(|e| e.id).collect(),
        };

        debug!("Marking {} entries as uploaded", ids.len());

        // Send request via vsock-rpc
        let response = self.rpc_client.request(request).await.map_err(|e| {
            error!("WAL mark uploaded failed: {}", e);
            StorageError::Backend(format!("WAL mark uploaded failed: {e}"))
        })?;

        debug!(
            "Successfully marked {} entries as uploaded",
            response.marked_count
        );

        Ok(())
    }

    /// List pending entries that haven't been uploaded
    pub async fn list_pending(&self, after: Option<WalEntryId>) -> StorageResult<Vec<WalEntry>> {
        let request = WalListPendingRequest {
            after: after.map(|e| e.id),
            limit: 1000,
        };

        debug!("Listing pending WAL entries");

        // Send request via vsock-rpc
        let response = self.rpc_client.request(request).await.map_err(|e| {
            error!("WAL list pending failed: {}", e);
            StorageError::Backend(format!("WAL list pending failed: {e}"))
        })?;

        debug!("Found {} pending WAL entries", response.entries.len());

        Ok(response.entries)
    }

    /// Recover pending batches from WAL
    pub async fn recover_pending(&self) -> StorageResult<Vec<PendingBatch>> {
        let pending_entries = self.list_pending(None).await?;

        if pending_entries.is_empty() {
            return Ok(Vec::new());
        }

        // Group entries into batches based on timestamp proximity
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let batch_start_time = 0u64;

        for entry in pending_entries {
            // TODO: Group by actual timestamp from WAL metadata
            current_batch.push(entry);
        }

        if !current_batch.is_empty() {
            batches.push(PendingBatch {
                entries: current_batch,
                created_at: batch_start_time,
            });
        }

        Ok(batches)
    }

    /// Cleanup old WAL entries
    pub async fn cleanup(&self, before: WalEntryId) -> StorageResult<u32> {
        let request = WalCleanupRequest {
            before_id: before.id,
        };

        debug!("Cleaning up WAL entries before {}", before.id);

        // Send request via vsock-rpc
        let response = self.rpc_client.request(request).await.map_err(|e| {
            error!("WAL cleanup failed: {}", e);
            StorageError::Backend(format!("WAL cleanup failed: {e}"))
        })?;

        debug!("Cleaned up {} WAL entries", response.cleaned_count);

        Ok(response.cleaned_count)
    }
}
