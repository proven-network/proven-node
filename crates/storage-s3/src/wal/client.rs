//! WAL client implementation for S3 storage

use bytes::Bytes;
use proven_storage::{StorageError, StorageNamespace, StorageResult};
use proven_vsock_rpc::{RpcClient, VsockAddr};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::commands::*;
use crate::config::WalConfig;

/// WAL client for durability
pub struct WalClient {
    /// Configuration
    config: WalConfig,
    /// RPC client
    client: RpcClient,
    /// Pending data size tracking
    pending_size: Arc<RwLock<usize>>,
}

impl WalClient {
    /// Create a new WAL client
    pub async fn new(config: WalConfig) -> StorageResult<Self> {
        info!("Connecting to WAL service on port {}", config.vsock_port);

        let vsock_addr = VsockAddr::new(2, config.vsock_port); // Host CID = 2

        let client = RpcClient::builder()
            .vsock_addr(vsock_addr)
            .build()
            .map_err(|e| StorageError::Backend(format!("Failed to create WAL client: {e}")))?;

        Ok(Self {
            config,
            client,
            pending_size: Arc::new(RwLock::new(0)),
        })
    }

    /// Append entries to WAL
    pub async fn append_logs(
        &self,
        namespace: &StorageNamespace,
        entries: &[(u64, Bytes)],
        batch_id: String,
    ) -> StorageResult<()> {
        // Check pending size limit
        let entry_size: usize = entries.iter().map(|(_, data)| data.len() + 8).sum();

        {
            let mut pending = self.pending_size.write().await;
            let new_size = *pending + entry_size;

            if new_size > self.config.max_pending_mb * 1024 * 1024 {
                return Err(StorageError::Backend(
                    "WAL pending data limit exceeded".to_string(),
                ));
            }

            *pending = new_size;
        }

        // Convert entries for serialization
        let wal_entries: Vec<(u64, Vec<u8>)> = entries
            .iter()
            .map(|(idx, data)| (*idx, data.to_vec()))
            .collect();

        let request = AppendLogsRequest {
            namespace: namespace.as_str().to_string(),
            entries: wal_entries,
            batch_id: batch_id.clone(),
        };

        let command = WalCommand::AppendLogs(request);
        let response = self
            .client
            .request(command)
            .await
            .map_err(|e| StorageError::Backend(format!("WAL request failed: {e}")))?;

        match response {
            WalResponse::AppendLogs(resp) => {
                if resp.success {
                    debug!("WAL append successful for batch {}", batch_id);
                    Ok(())
                } else {
                    // Restore pending size on error
                    *self.pending_size.write().await -= entry_size;
                    Err(StorageError::Backend(
                        resp.error
                            .unwrap_or_else(|| "WAL append failed".to_string()),
                    ))
                }
            }
            _ => {
                *self.pending_size.write().await -= entry_size;
                Err(StorageError::Backend(
                    "Unexpected WAL response type".to_string(),
                ))
            }
        }
    }

    /// Confirm a batch was successfully uploaded to S3
    pub async fn confirm_batch(&self, batch_id: String) -> StorageResult<()> {
        let request = ConfirmBatchRequest { batch_id };
        let command = WalCommand::ConfirmBatch(request);

        let response = self
            .client
            .request(command)
            .await
            .map_err(|e| StorageError::Backend(format!("WAL request failed: {e}")))?;

        match response {
            WalResponse::ConfirmBatch(resp) => {
                if resp.success {
                    Ok(())
                } else {
                    Err(StorageError::Backend(
                        resp.error
                            .unwrap_or_else(|| "WAL confirm failed".to_string()),
                    ))
                }
            }
            _ => Err(StorageError::Backend(
                "Unexpected WAL response type".to_string(),
            )),
        }
    }

    /// Sync WAL to disk
    pub async fn sync(&self) -> StorageResult<()> {
        let command = WalCommand::Sync(SyncRequest::default());

        let response = self
            .client
            .request(command)
            .await
            .map_err(|e| StorageError::Backend(format!("WAL request failed: {e}")))?;

        match response {
            WalResponse::Sync(resp) => {
                if resp.success {
                    Ok(())
                } else {
                    Err(StorageError::Backend(
                        resp.error.unwrap_or_else(|| "WAL sync failed".to_string()),
                    ))
                }
            }
            _ => Err(StorageError::Backend(
                "Unexpected WAL response type".to_string(),
            )),
        }
    }

    /// Get pending batches for recovery
    pub async fn get_pending_batches(&self) -> StorageResult<Vec<PendingBatch>> {
        let command = WalCommand::GetPendingBatches(GetPendingBatchesRequest::default());

        let response = self
            .client
            .request(command)
            .await
            .map_err(|e| StorageError::Backend(format!("WAL request failed: {e}")))?;

        match response {
            WalResponse::GetPendingBatches(resp) => {
                if let Some(error) = resp.error {
                    Err(StorageError::Backend(format!(
                        "WAL recovery failed: {error}"
                    )))
                } else {
                    // Update pending size based on recovered batches
                    let total_size: usize = resp
                        .batches
                        .iter()
                        .flat_map(|b| &b.entries)
                        .map(|(_, data)| data.len() + 8)
                        .sum();

                    *self.pending_size.write().await = total_size;

                    Ok(resp.batches)
                }
            }
            _ => Err(StorageError::Backend(
                "Unexpected WAL response type".to_string(),
            )),
        }
    }

    /// Update pending size after batch confirmation
    pub async fn update_pending_size(&self, batch_size: usize) {
        let mut pending = self.pending_size.write().await;
        *pending = pending.saturating_sub(batch_size);
    }
}

/// WAL recovery manager
pub struct WalRecovery {
    /// WAL client
    client: Arc<WalClient>,
}

impl WalRecovery {
    /// Create a new recovery manager
    pub fn new(client: Arc<WalClient>) -> Self {
        Self { client }
    }

    /// Recover pending batches from WAL
    pub async fn recover(&self) -> StorageResult<HashMap<StorageNamespace, Vec<(u64, Bytes)>>> {
        info!("Starting WAL recovery");

        let pending_batches = self.client.get_pending_batches().await?;

        if pending_batches.is_empty() {
            info!("No pending batches to recover");
            return Ok(HashMap::new());
        }

        info!("Found {} pending batches to recover", pending_batches.len());

        let mut recovered = HashMap::new();

        for batch in pending_batches {
            let namespace = StorageNamespace::new(batch.namespace);

            // Convert entries back to Bytes
            let entries: Vec<(u64, Bytes)> = batch
                .entries
                .into_iter()
                .map(|(idx, data)| (idx, Bytes::from(data)))
                .collect();

            recovered
                .entry(namespace)
                .or_insert_with(Vec::new)
                .extend(entries);
        }

        info!(
            "WAL recovery complete, recovered {} namespaces",
            recovered.len()
        );

        Ok(recovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    #[ignore] // Requires VSOCK setup
    async fn test_wal_client_basic() {
        let config = WalConfig::default();
        let client = WalClient::new(config).await.unwrap();

        let namespace = StorageNamespace::new("test");
        let entries = vec![(1, Bytes::from("data1")), (2, Bytes::from("data2"))];

        let batch_id = Uuid::new_v4().to_string();

        // Append to WAL
        client
            .append_logs(&namespace, &entries, batch_id.clone())
            .await
            .unwrap();

        // Sync
        client.sync().await.unwrap();

        // Confirm batch
        client.confirm_batch(batch_id).await.unwrap();
    }
}
