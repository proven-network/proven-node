//! Host-side RPC server for storage operations
//!
//! This module implements the RPC server that handles storage requests
//! from the enclave over VSOCK.

use async_trait::async_trait;
use bytes::Bytes;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, RpcHandler, RpcServer as VsockRpcServer, ServerConfig,
    error::HandlerError,
};
use std::sync::Arc;
use tracing::{debug, error};

use crate::{
    error::{Result, VsockFuseError as Error},
    storage::{
        BlobStorage,
        messages::{
            DeleteBlobRequest, DeleteBlobResponse, GetBlobRequest, GetBlobResponse,
            GetStorageStatsRequest, GetStorageStatsResponse, ListBlobsRequest, ListBlobsResponse,
            StoreBlobRequest, StoreBlobResponse,
        },
    },
};

/// Storage RPC handler
struct StorageHandler {
    storage: Arc<dyn BlobStorage>,
}

#[async_trait]
impl RpcHandler for StorageHandler {
    async fn handle_message(
        &self,
        message_id: &str,
        message: Bytes,
        _pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        debug!(
            "Received message '{}' with {} bytes",
            message_id,
            message.len()
        );

        // Route based on message_id
        match message_id {
            "blob.store" => {
                let request = StoreBlobRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode StoreBlobRequest: {e}"
                    )))
                })?;
                debug!("Received StoreBlobRequest for blob {:?}", request.blob_id);
                let response = self.handle_store_blob(request).await.map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(e.to_string()))
                })?;
                let response_bytes: Bytes = response.try_into().map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to encode response: {e}"
                    )))
                })?;
                debug!("Sending response with {} bytes", response_bytes.len());
                Ok(HandlerResponse::Single(response_bytes))
            }
            "blob.get" => {
                let request = GetBlobRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode GetBlobRequest: {e}"
                    )))
                })?;
                debug!("Received GetBlobRequest for blob {:?}", request.blob_id);
                let response = self.handle_get_blob(request).await.map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(e.to_string()))
                })?;
                let response_bytes: Bytes = response.try_into().map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to encode response: {e}"
                    )))
                })?;
                Ok(HandlerResponse::Single(response_bytes))
            }
            "blob.delete" => {
                let request = DeleteBlobRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode DeleteBlobRequest: {e}"
                    )))
                })?;
                debug!("Received DeleteBlobRequest for blob {:?}", request.blob_id);
                let response = self.handle_delete_blob(request).await.map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(e.to_string()))
                })?;
                debug!("Delete response: {:?}", response);
                let response_bytes: Bytes = response.try_into().map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to encode response: {e}"
                    )))
                })?;
                debug!("Encoded delete response to {} bytes", response_bytes.len());
                Ok(HandlerResponse::Single(response_bytes))
            }
            "blob.list" => {
                let request = ListBlobsRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode ListBlobsRequest: {e}"
                    )))
                })?;
                debug!("Received ListBlobsRequest with prefix {:?}", request.prefix);
                let response = self.handle_list_blobs(request).await.map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(e.to_string()))
                })?;
                let response_bytes: Bytes = response.try_into().map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to encode response: {e}"
                    )))
                })?;
                Ok(HandlerResponse::Single(response_bytes))
            }
            "storage.stats" => {
                let request = GetStorageStatsRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode GetStorageStatsRequest: {e}"
                    )))
                })?;
                debug!("Received GetStorageStatsRequest");
                let response = self.handle_get_stats(request).await.map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(e.to_string()))
                })?;
                debug!("Generated stats response: {:?}", response);
                let response_bytes: Bytes = response.try_into().map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to encode response: {e}"
                    )))
                })?;
                debug!("Encoded stats response to {} bytes", response_bytes.len());
                Ok(HandlerResponse::Single(response_bytes))
            }
            _ => {
                error!("Unknown message type: {}", message_id);
                Err(proven_vsock_rpc::Error::Handler(HandlerError::NotFound(
                    format!("Unknown message type: {message_id}"),
                )))
            }
        }
    }

    async fn on_connect(
        &self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> proven_vsock_rpc::Result<()> {
        debug!("Client connected from: {:?}", addr);
        Ok(())
    }

    async fn on_disconnect(
        &self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) {
        debug!("Client disconnected: {:?}", addr);
    }
}

/// Host-side storage server
pub struct HostStorageServer {
    /// Storage backend
    storage: Arc<dyn BlobStorage>,
    /// Server port
    port: u32,
}

impl HostStorageServer {
    /// Create a new storage server
    pub fn new(storage: Arc<dyn BlobStorage>, port: u32) -> Self {
        Self { storage, port }
    }

    /// Start the server and run until shutdown
    pub async fn serve(self) -> Result<()> {
        debug!("Starting host storage server on port {}", self.port);

        // Create the address based on platform
        #[cfg(target_os = "linux")]
        let addr = tokio_vsock::VsockAddr::new(3, self.port); // CID 3 for host

        #[cfg(not(target_os = "linux"))]
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], self.port as u16));

        // Create handler
        let handler = StorageHandler {
            storage: Arc::clone(&self.storage),
        };

        // Create server config
        let config = ServerConfig {
            max_connections: 100,
            request_timeout: std::time::Duration::from_secs(300), // 5 minutes for large blobs
            max_frame_size: 100 * 1024 * 1024,                    // 100MB for large blobs
            decompress_requests: false,
        };

        // Create the server
        let server = VsockRpcServer::new(addr, handler, config);

        // Start serving
        server.serve().await.map_err(|e| Error::Io {
            message: format!("RPC server error: {e}"),
            source: None,
        })?;

        Ok(())
    }
}

impl StorageHandler {
    /// Handle a store blob request
    async fn handle_store_blob(&self, request: StoreBlobRequest) -> Result<StoreBlobResponse> {
        debug!(
            "Storing blob {:?} with {} bytes",
            request.blob_id,
            request.data.len()
        );

        self.storage
            .store_blob(request.blob_id, request.data, request.tier_hint)
            .await?;

        debug!("Blob stored successfully");

        Ok(StoreBlobResponse {
            stored: true,
            tier: crate::StorageTier::Hot, // TODO: Return actual tier
        })
    }

    /// Handle a get blob request
    async fn handle_get_blob(&self, request: GetBlobRequest) -> Result<GetBlobResponse> {
        let blob = self.storage.get_blob(request.blob_id).await?;

        Ok(GetBlobResponse {
            data: blob.data,
            tier: blob.tier,
        })
    }

    /// Handle a delete blob request
    async fn handle_delete_blob(&self, request: DeleteBlobRequest) -> Result<DeleteBlobResponse> {
        self.storage.delete_blob(request.blob_id).await?;
        Ok(DeleteBlobResponse { deleted: true })
    }

    /// Handle a list blobs request
    async fn handle_list_blobs(&self, request: ListBlobsRequest) -> Result<ListBlobsResponse> {
        let blobs = self.storage.list_blobs(request.prefix.as_deref()).await?;
        Ok(ListBlobsResponse {
            blobs,
            continuation_token: None,
        })
    }

    /// Handle a get stats request
    async fn handle_get_stats(
        &self,
        _request: GetStorageStatsRequest,
    ) -> Result<GetStorageStatsResponse> {
        let stats = self.storage.get_stats().await?;
        Ok(GetStorageStatsResponse { stats })
    }
}
