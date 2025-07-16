//! Storage abstraction layer for VSOCK-FUSE
//!
//! This module provides the blob storage interface that separates
//! the encrypted filesystem logic from the actual storage backend.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    BlobId, StorageTier, TierHint,
    error::{Result, StorageError},
};

/// Blob storage trait for the host side
#[async_trait]
pub trait BlobStorage: Send + Sync {
    /// Store a blob
    async fn store_blob(&self, blob_id: BlobId, data: Vec<u8>, tier_hint: TierHint) -> Result<()>;

    /// Retrieve a blob
    async fn get_blob(&self, blob_id: BlobId) -> Result<BlobData>;

    /// Delete a blob
    async fn delete_blob(&self, blob_id: BlobId) -> Result<()>;

    /// List blobs with optional prefix
    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<BlobInfo>>;

    /// Get storage statistics
    async fn get_stats(&self) -> Result<StorageStats>;
}

/// Blob data with tier information
#[derive(Debug, Clone)]
pub struct BlobData {
    /// The actual blob data
    pub data: Vec<u8>,
    /// Which tier the blob was retrieved from
    pub tier: StorageTier,
}

/// Information about a stored blob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobInfo {
    /// Blob identifier
    pub blob_id: BlobId,
    /// Size in bytes
    pub size: u64,
    /// Storage tier
    pub tier: StorageTier,
    /// Last access time
    pub last_accessed: std::time::SystemTime,
    /// Creation time
    pub created_at: std::time::SystemTime,
}

/// Storage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Hot tier statistics
    pub hot_tier: TierStats,
    /// Cold tier statistics
    pub cold_tier: TierStats,
    /// Number of blobs pending migration
    pub migration_queue_size: usize,
}

/// Statistics for a storage tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierStats {
    /// Total capacity in bytes
    pub total_bytes: u64,
    /// Used capacity in bytes
    pub used_bytes: u64,
    /// Number of files in this tier
    pub file_count: u64,
    /// Read operations per second
    pub read_ops_per_sec: f64,
    /// Write operations per second
    pub write_ops_per_sec: f64,
}

/// RPC messages for blob storage operations
pub mod messages {
    use super::*;
    use proven_vsock_rpc::RpcMessage;
    use serde::{Deserialize, Serialize};

    /// Error type for message conversions
    #[derive(Debug)]
    pub struct MessageError(String);

    impl std::fmt::Display for MessageError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for MessageError {}

    /// Macro to implement TryFrom/TryInto for message types
    macro_rules! impl_message_conversions {
        ($msg_type:ty) => {
            impl TryFrom<bytes::Bytes> for $msg_type {
                type Error = MessageError;

                fn try_from(bytes: bytes::Bytes) -> std::result::Result<Self, Self::Error> {
                    bincode::deserialize(&bytes).map_err(|e| MessageError(e.to_string()))
                }
            }

            impl TryInto<bytes::Bytes> for $msg_type {
                type Error = MessageError;

                fn try_into(self) -> std::result::Result<bytes::Bytes, Self::Error> {
                    let encoded =
                        bincode::serialize(&self).map_err(|e| MessageError(e.to_string()))?;
                    Ok(bytes::Bytes::from(encoded))
                }
            }
        };
    }

    /// Store blob request
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct StoreBlobRequest {
        pub blob_id: BlobId,
        pub data: Vec<u8>,
        pub tier_hint: TierHint,
    }

    /// Store blob response
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct StoreBlobResponse {
        pub stored: bool,
        pub tier: StorageTier,
    }

    impl_message_conversions!(StoreBlobRequest);
    impl_message_conversions!(StoreBlobResponse);

    impl RpcMessage for StoreBlobRequest {
        type Response = StoreBlobResponse;

        fn message_id(&self) -> &'static str {
            "blob.store"
        }
    }

    /// Get blob request
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GetBlobRequest {
        pub blob_id: BlobId,
    }

    /// Get blob response
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GetBlobResponse {
        pub data: Vec<u8>,
        pub tier: StorageTier,
    }

    impl_message_conversions!(GetBlobRequest);
    impl_message_conversions!(GetBlobResponse);

    impl RpcMessage for GetBlobRequest {
        type Response = GetBlobResponse;

        fn message_id(&self) -> &'static str {
            "blob.get"
        }
    }

    /// Delete blob request
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DeleteBlobRequest {
        pub blob_id: BlobId,
        pub permanent: bool,
    }

    /// Delete blob response
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DeleteBlobResponse {
        pub deleted: bool,
    }

    impl_message_conversions!(DeleteBlobRequest);
    impl_message_conversions!(DeleteBlobResponse);

    impl RpcMessage for DeleteBlobRequest {
        type Response = DeleteBlobResponse;

        fn message_id(&self) -> &'static str {
            "blob.delete"
        }
    }

    /// List blobs request
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ListBlobsRequest {
        pub prefix: Option<Vec<u8>>,
        pub continuation_token: Option<String>,
        pub max_results: Option<u32>,
    }

    /// List blobs response
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ListBlobsResponse {
        pub blobs: Vec<BlobInfo>,
        pub continuation_token: Option<String>,
    }

    impl_message_conversions!(ListBlobsRequest);
    impl_message_conversions!(ListBlobsResponse);

    impl RpcMessage for ListBlobsRequest {
        type Response = ListBlobsResponse;

        fn message_id(&self) -> &'static str {
            "blob.list"
        }
    }

    /// Get storage stats request
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GetStorageStatsRequest {}

    /// Get storage stats response
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GetStorageStatsResponse {
        pub stats: StorageStats,
    }

    impl_message_conversions!(GetStorageStatsRequest);
    impl_message_conversions!(GetStorageStatsResponse);

    impl RpcMessage for GetStorageStatsRequest {
        type Response = GetStorageStatsResponse;

        fn message_id(&self) -> &'static str {
            "storage.stats"
        }
    }

    /// Streaming read request for large blobs
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct StreamingReadRequest {
        pub blob_id: BlobId,
        pub offset: u64,
        pub chunk_size: u32,
    }

    /// Streaming read chunk
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct StreamingReadChunk {
        pub offset: u64,
        pub data: Vec<u8>,
        pub is_last: bool,
    }

    impl_message_conversions!(StreamingReadRequest);
    impl_message_conversions!(StreamingReadChunk);

    impl RpcMessage for StreamingReadRequest {
        type Response = StreamingReadChunk;

        fn message_id(&self) -> &'static str {
            "blob.stream_read"
        }
    }
}

/// Client-side blob storage implementation that uses RPC
pub struct RpcBlobStorage {
    client: Arc<proven_vsock_rpc::RpcClient>,
}

impl RpcBlobStorage {
    /// Create a new RPC blob storage client
    pub fn new(client: Arc<proven_vsock_rpc::RpcClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl BlobStorage for RpcBlobStorage {
    async fn store_blob(&self, blob_id: BlobId, data: Vec<u8>, tier_hint: TierHint) -> Result<()> {
        let request = messages::StoreBlobRequest {
            blob_id,
            data,
            tier_hint,
        };

        let _response: messages::StoreBlobResponse =
            self.client
                .request(request)
                .await
                .map_err(|e| StorageError::S3Error {
                    message: format!("RPC error: {e}"),
                })?;

        Ok(())
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<BlobData> {
        let request = messages::GetBlobRequest { blob_id };

        let response: messages::GetBlobResponse = self
            .client
            .request(request)
            .await
            .map_err(|_e| StorageError::BlobNotFound { blob_id })?;

        Ok(BlobData {
            data: response.data,
            tier: response.tier,
        })
    }

    async fn delete_blob(&self, blob_id: BlobId) -> Result<()> {
        let request = messages::DeleteBlobRequest {
            blob_id,
            permanent: true,
        };

        let _response: messages::DeleteBlobResponse =
            self.client
                .request(request)
                .await
                .map_err(|e| StorageError::S3Error {
                    message: format!("RPC error: {e}"),
                })?;

        Ok(())
    }

    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<BlobInfo>> {
        let request = messages::ListBlobsRequest {
            prefix: prefix.map(|p| p.to_vec()),
            continuation_token: None,
            max_results: Some(1000),
        };

        let response: messages::ListBlobsResponse =
            self.client
                .request(request)
                .await
                .map_err(|e| StorageError::S3Error {
                    message: format!("RPC error: {e}"),
                })?;

        Ok(response.blobs)
    }

    async fn get_stats(&self) -> Result<StorageStats> {
        let request = messages::GetStorageStatsRequest {};

        let response: messages::GetStorageStatsResponse = self
            .client
            .request(request)
            .await
            .map_err(|e| StorageError::S3Error {
                message: format!("RPC error: {e}"),
            })?;

        Ok(response.stats)
    }
}

/// Mock storage implementation for testing
#[cfg(test)]
pub mod mock {
    use super::*;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    #[derive(Debug, Default, Clone)]
    pub struct MockBlobStorage {
        data: Arc<RwLock<HashMap<BlobId, Vec<u8>>>>,
    }

    impl MockBlobStorage {
        pub fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl BlobStorage for MockBlobStorage {
        async fn store_blob(
            &self,
            blob_id: BlobId,
            data: Vec<u8>,
            _tier_hint: TierHint,
        ) -> Result<()> {
            self.data.write().insert(blob_id, data);
            Ok(())
        }

        async fn get_blob(&self, blob_id: BlobId) -> Result<BlobData> {
            self.data
                .read()
                .get(&blob_id)
                .map(|data| BlobData {
                    data: data.clone(),
                    tier: StorageTier::Hot,
                })
                .ok_or_else(|| StorageError::BlobNotFound { blob_id }.into())
        }

        async fn delete_blob(&self, blob_id: BlobId) -> Result<()> {
            self.data.write().remove(&blob_id);
            Ok(())
        }

        async fn list_blobs(&self, _prefix: Option<&[u8]>) -> Result<Vec<BlobInfo>> {
            let now = std::time::SystemTime::now();
            Ok(self
                .data
                .read()
                .iter()
                .map(|(id, data)| BlobInfo {
                    blob_id: *id,
                    size: data.len() as u64,
                    tier: StorageTier::Hot,
                    last_accessed: now,
                    created_at: now,
                })
                .collect())
        }

        async fn get_stats(&self) -> Result<StorageStats> {
            let data = self.data.read();
            let total_size: usize = data.values().map(|v| v.len()).sum();

            Ok(StorageStats {
                hot_tier: TierStats {
                    total_bytes: 1_000_000_000, // 1GB mock capacity
                    used_bytes: total_size as u64,
                    file_count: data.len() as u64,
                    read_ops_per_sec: 0.0,
                    write_ops_per_sec: 0.0,
                },
                cold_tier: TierStats {
                    total_bytes: 0,
                    used_bytes: 0,
                    file_count: 0,
                    read_ops_per_sec: 0.0,
                    write_ops_per_sec: 0.0,
                },
                migration_queue_size: 0,
            })
        }
    }
}
