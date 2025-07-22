//! Enclave-side storage client
//!
//! This module provides the client interface for the enclave to communicate
//! with the host-side storage service over VSOCK.

use async_trait::async_trait;
use proven_logger::warn;
use std::sync::Arc;

use crate::{
    BlobId, TierHint,
    error::Result,
    storage::{
        BlobData, BlobInfo, BlobStorage, StorageStats,
        messages::{
            DeleteBlobRequest, DeleteBlobResponse, GetBlobRequest, GetBlobResponse,
            GetStorageStatsRequest, GetStorageStatsResponse, ListBlobsRequest, ListBlobsResponse,
            StoreBlobRequest, StoreBlobResponse,
        },
    },
};

/// Enclave-side storage client that communicates with the host
pub struct EnclaveStorageClient {
    /// RPC client
    client: Arc<proven_vsock_rpc::RpcClient>,
}

impl EnclaveStorageClient {
    /// Connect to the host storage service.
    /// On Linux, this uses VSOCK. On other platforms, it uses TCP on localhost.
    pub async fn connect(host_cid: u32, host_port: u32) -> Result<Self> {
        #[cfg(target_os = "linux")]
        let addr = tokio_vsock::VsockAddr::new(host_cid, host_port);

        #[cfg(not(target_os = "linux"))]
        let addr = {
            if host_cid != 2 && host_cid != 3 {
                warn!("CID {host_cid} ignored on non-Linux platform, using localhost:{host_port}");
            }
            std::net::SocketAddr::from(([127, 0, 0, 1], host_port as u16))
        };

        let client = proven_vsock_rpc::RpcClient::builder()
            .vsock_addr(addr)
            .build()?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Create from existing RPC client
    pub fn from_client(client: Arc<proven_vsock_rpc::RpcClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl BlobStorage for EnclaveStorageClient {
    async fn store_blob(&self, id: BlobId, data: Vec<u8>, hint: TierHint) -> Result<()> {
        let request = StoreBlobRequest {
            blob_id: id,
            data,
            tier_hint: hint,
        };

        let _response: StoreBlobResponse = self.client.request(request).await?;

        Ok(())
    }

    async fn get_blob(&self, id: BlobId) -> Result<BlobData> {
        let request = GetBlobRequest { blob_id: id };

        let response: GetBlobResponse = self.client.request(request).await?;

        Ok(BlobData {
            data: response.data,
            tier: response.tier,
        })
    }

    async fn delete_blob(&self, id: BlobId) -> Result<()> {
        let request = DeleteBlobRequest {
            blob_id: id,
            permanent: true,
        };

        let _response: DeleteBlobResponse = self.client.request(request).await?;

        Ok(())
    }

    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<BlobInfo>> {
        let request = ListBlobsRequest {
            prefix: prefix.map(|p| p.to_vec()),
            continuation_token: None,
            max_results: Some(1000),
        };

        let response: ListBlobsResponse = self.client.request(request).await?;

        Ok(response.blobs)
    }

    async fn get_stats(&self) -> Result<StorageStats> {
        let request = GetStorageStatsRequest {};

        let response: GetStorageStatsResponse = self.client.request(request).await?;

        Ok(response.stats)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
