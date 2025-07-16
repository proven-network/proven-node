//! RPC client and server implementations for VSOCK-FUSE
//!
//! This module provides the communication layer between enclave and host.

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use crate::{
    BlobId, StorageTier, TierHint,
    error::{Result, VsockFuseError},
    storage::{BlobData, BlobInfo, BlobStorage, StorageStats, StreamingBlobStorage, messages},
};

/// RPC client configuration
#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    /// VSOCK CID of the host
    pub host_cid: u32,
    /// VSOCK port to connect to
    pub port: u32,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Maximum request size
    pub max_request_size: usize,
    /// Maximum response size
    pub max_response_size: usize,
    /// Retry configuration
    pub retry_config: RetryConfig,
}

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Exponential backoff multiplier
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
        }
    }
}

/// RPC client for enclave-side communication
pub struct VsockRpcClient {
    inner: Arc<proven_vsock_rpc::RpcClient>,
    config: RpcClientConfig,
}

impl VsockRpcClient {
    /// Create a new RPC client
    pub async fn new(config: RpcClientConfig) -> Result<Self> {
        #[cfg(target_os = "linux")]
        let vsock_addr = proven_vsock_rpc::VsockAddr::new(config.host_cid, config.port);
        #[cfg(not(target_os = "linux"))]
        let vsock_addr = std::net::SocketAddr::from(([127, 0, 0, 1], config.port as u16));

        let client = proven_vsock_rpc::RpcClient::builder()
            .vsock_addr(vsock_addr)
            .default_timeout(config.request_timeout)
            .build()
            .map_err(|e| VsockFuseError::Internal(format!("Failed to create RPC client: {e}")))?;

        Ok(Self {
            inner: Arc::new(client),
            config,
        })
    }

    /// Execute a request with retry logic
    async fn request_with_retry<Req>(&self, request: Req) -> Result<Req::Response>
    where
        Req: proven_vsock_rpc::RpcMessage + Clone + TryInto<bytes::Bytes>,
        Req::Error: std::error::Error + Send + Sync + 'static,
        Req::Response: TryFrom<bytes::Bytes>,
        <Req::Response as TryFrom<bytes::Bytes>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut attempt = 0;
        let mut delay = self.config.retry_config.initial_delay;

        loop {
            match timeout(
                self.config.request_timeout,
                self.inner.request(request.clone()),
            )
            .await
            {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(e)) => {
                    attempt += 1;
                    if attempt >= self.config.retry_config.max_attempts {
                        return Err(VsockFuseError::Internal(format!("RPC error: {e}")));
                    }

                    // Check if error is retryable
                    if !self.is_retryable_error(&e) {
                        return Err(VsockFuseError::Internal(format!("RPC error: {e}")));
                    }
                }
                Err(_) => {
                    attempt += 1;
                    if attempt >= self.config.retry_config.max_attempts {
                        return Err(VsockFuseError::Io {
                            message: "Request timeout".to_string(),
                            source: None,
                        });
                    }
                }
            }

            // Wait before retry
            tokio::time::sleep(delay).await;

            // Update delay with exponential backoff
            delay = Duration::from_secs_f64(
                (delay.as_secs_f64() * self.config.retry_config.multiplier)
                    .min(self.config.retry_config.max_delay.as_secs_f64()),
            );
        }
    }

    /// Check if an error is retryable
    fn is_retryable_error(&self, _error: &proven_vsock_rpc::Error) -> bool {
        // For now, retry on all errors except those that seem permanent
        // TODO: Implement more sophisticated retry logic based on error type
        true
    }
}

#[async_trait]
impl BlobStorage for VsockRpcClient {
    async fn store_blob(&self, blob_id: BlobId, data: Vec<u8>, tier_hint: TierHint) -> Result<()> {
        // Check size limit
        if data.len() > self.config.max_request_size {
            return Err(VsockFuseError::InvalidArgument {
                message: format!(
                    "Blob size {} exceeds max request size {}",
                    data.len(),
                    self.config.max_request_size
                ),
            });
        }

        let request = messages::StoreBlobRequest {
            blob_id,
            data,
            tier_hint,
        };

        let _response: messages::StoreBlobResponse = self.request_with_retry(request).await?;

        Ok(())
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<BlobData> {
        let request = messages::GetBlobRequest { blob_id };

        let response: messages::GetBlobResponse = self.request_with_retry(request).await?;

        // Check response size
        if response.data.len() > self.config.max_response_size {
            return Err(VsockFuseError::InvalidArgument {
                message: format!(
                    "Response size {} exceeds max response size {}",
                    response.data.len(),
                    self.config.max_response_size
                ),
            });
        }

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

        let _response: messages::DeleteBlobResponse = self.request_with_retry(request).await?;

        Ok(())
    }

    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<BlobInfo>> {
        let mut all_blobs = Vec::new();
        let mut continuation_token = None;

        loop {
            let request = messages::ListBlobsRequest {
                prefix: prefix.map(|p| p.to_vec()),
                continuation_token: continuation_token.clone(),
                max_results: Some(1000),
            };

            let response: messages::ListBlobsResponse = self.request_with_retry(request).await?;

            all_blobs.extend(response.blobs);

            if response.continuation_token.is_none() {
                break;
            }
            continuation_token = response.continuation_token;
        }

        Ok(all_blobs)
    }

    async fn get_stats(&self) -> Result<StorageStats> {
        let request = messages::GetStorageStatsRequest {};

        let response: messages::GetStorageStatsResponse = self.request_with_retry(request).await?;

        Ok(response.stats)
    }
}

#[async_trait]
impl StreamingBlobStorage for VsockRpcClient {
    async fn store_blob_stream(
        &self,
        blob_id: BlobId,
        data_stream: impl Stream<Item = Result<bytes::Bytes>> + Send + 'static,
        tier_hint: TierHint,
    ) -> Result<()> {
        // Box the stream to make it Unpin
        let data_stream = Box::pin(data_stream);
        self.store_blob_stream_with_size(blob_id, data_stream, tier_hint, None)
            .await
    }

    async fn get_blob_stream(
        &self,
        blob_id: BlobId,
    ) -> Result<impl Stream<Item = Result<bytes::Bytes>> + Send> {
        // Use the existing get_blob_stream_with_range method
        self.get_blob_stream_with_range(blob_id, 0, None).await
    }
}

/// Streaming support for large blobs
impl VsockRpcClient {
    /// Stream store a blob in chunks
    pub async fn store_blob_stream_with_size(
        &self,
        blob_id: BlobId,
        mut data_stream: std::pin::Pin<Box<dyn Stream<Item = Result<bytes::Bytes>> + Send>>,
        tier_hint: TierHint,
        expected_size: Option<u64>,
    ) -> Result<()> {
        // Initiate streaming upload
        let _init_request = messages::StoreBlobStreamRequest {
            blob_id,
            tier_hint,
            expected_size,
        };

        // For now, collect all chunks and send as a single blob
        // TODO: Implement actual streaming when server supports it
        let mut all_data = Vec::new();
        while let Some(chunk_result) = data_stream.next().await {
            let chunk = chunk_result?;
            all_data.extend_from_slice(&chunk);
        }

        // Fall back to regular store_blob
        self.store_blob(blob_id, all_data, tier_hint).await
    }

    /// Stream read a large blob in chunks
    pub async fn get_blob_stream_with_range(
        &self,
        blob_id: BlobId,
        offset: u64,
        length: Option<u64>,
    ) -> Result<impl Stream<Item = Result<bytes::Bytes>> + Send> {
        let _request = messages::GetBlobStreamRequest {
            blob_id,
            offset,
            length,
        };

        // For now, use regular get_blob and convert to stream
        // TODO: Use vsock-rpc's request_stream when server supports it
        let response = self.get_blob(blob_id).await?;
        let data = response.data;

        // Apply offset and length
        let start = offset as usize;
        let end = if let Some(len) = length {
            (start + len as usize).min(data.len())
        } else {
            data.len()
        };

        let chunk_data = if start < data.len() {
            data[start..end].to_vec()
        } else {
            Vec::new()
        };

        // Create chunks of 64KB
        let chunk_size = 64 * 1024;
        let chunks: Vec<bytes::Bytes> = chunk_data
            .chunks(chunk_size)
            .map(bytes::Bytes::copy_from_slice)
            .collect();

        Ok(futures::stream::iter(chunks.into_iter().map(Ok)))
    }
}

/// RPC server handler for host-side
pub struct VsockRpcHandler {
    storage: Arc<dyn BlobStorage>,
}

impl VsockRpcHandler {
    /// Create a new RPC handler
    pub fn new(storage: Arc<dyn BlobStorage>) -> Self {
        Self { storage }
    }

    /// Handle store blob request
    pub async fn handle_store_blob(
        &self,
        request: messages::StoreBlobRequest,
    ) -> Result<messages::StoreBlobResponse> {
        self.storage
            .store_blob(request.blob_id, request.data, request.tier_hint)
            .await?;

        Ok(messages::StoreBlobResponse {
            stored: true,
            tier: StorageTier::Hot, // TODO: Get actual tier from storage
        })
    }

    /// Handle get blob request
    pub async fn handle_get_blob(
        &self,
        request: messages::GetBlobRequest,
    ) -> Result<messages::GetBlobResponse> {
        let blob_data = self.storage.get_blob(request.blob_id).await?;

        Ok(messages::GetBlobResponse {
            data: blob_data.data,
            tier: blob_data.tier,
        })
    }

    /// Handle delete blob request
    pub async fn handle_delete_blob(
        &self,
        request: messages::DeleteBlobRequest,
    ) -> Result<messages::DeleteBlobResponse> {
        self.storage.delete_blob(request.blob_id).await?;

        Ok(messages::DeleteBlobResponse { deleted: true })
    }

    /// Handle list blobs request
    pub async fn handle_list_blobs(
        &self,
        request: messages::ListBlobsRequest,
    ) -> Result<messages::ListBlobsResponse> {
        let blobs = self.storage.list_blobs(request.prefix.as_deref()).await?;

        // TODO: Implement pagination
        Ok(messages::ListBlobsResponse {
            blobs,
            continuation_token: None,
        })
    }

    /// Handle get storage stats request
    pub async fn handle_get_storage_stats(
        &self,
        _request: messages::GetStorageStatsRequest,
    ) -> Result<messages::GetStorageStatsResponse> {
        let stats = self.storage.get_stats().await?;

        Ok(messages::GetStorageStatsResponse { stats })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mock::MockBlobStorage;

    #[tokio::test]
    async fn test_rpc_handler_store_and_get() {
        let storage = Arc::new(MockBlobStorage::new());
        let handler = VsockRpcHandler::new(storage);

        let blob_id = BlobId::new();
        let data = vec![1, 2, 3, 4, 5];

        // Store blob
        let store_req = messages::StoreBlobRequest {
            blob_id,
            data: data.clone(),
            tier_hint: TierHint::PreferHot,
        };
        let store_resp = handler.handle_store_blob(store_req).await.unwrap();
        assert!(store_resp.stored);

        // Get blob
        let get_req = messages::GetBlobRequest { blob_id };
        let get_resp = handler.handle_get_blob(get_req).await.unwrap();
        assert_eq!(get_resp.data, data);
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
    }
}
