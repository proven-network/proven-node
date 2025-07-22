//! WAL server implementation for S3 storage

use async_trait::async_trait;
use bytes::Bytes;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, RpcHandler, RpcServer, ServerConfig, error::HandlerError,
};
use serde_json;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use proven_logger::{debug, error, info};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
};

use super::commands::*;

/// Type alias for metadata storage to reduce type complexity
type MetadataStorage = Arc<RwLock<HashMap<String, HashMap<String, Vec<u8>>>>>;

/// Trait for implementing WAL command handlers
#[async_trait]
pub trait WalCommandHandler: Send + Sync + 'static {
    /// Handle append logs request
    async fn handle_append_logs(
        &self,
        request: AppendLogsRequest,
    ) -> Result<AppendLogsResponse, Box<dyn std::error::Error>>;

    /// Handle confirm batch request
    async fn handle_confirm_batch(
        &self,
        request: ConfirmBatchRequest,
    ) -> Result<ConfirmBatchResponse, Box<dyn std::error::Error>>;

    /// Handle sync request
    async fn handle_sync(
        &self,
        request: SyncRequest,
    ) -> Result<SyncResponse, Box<dyn std::error::Error>>;

    /// Handle get pending batches request
    async fn handle_get_pending_batches(
        &self,
        request: GetPendingBatchesRequest,
    ) -> Result<GetPendingBatchesResponse, Box<dyn std::error::Error>>;

    /// Handle set metadata request
    async fn handle_set_metadata(
        &self,
        request: SetMetadataRequest,
    ) -> Result<SetMetadataResponse, Box<dyn std::error::Error>>;
}

/// WAL handler that wraps the command handler
pub struct WalHandler<H> {
    inner: Arc<H>,
}

impl<H: WalCommandHandler> WalHandler<H> {
    /// Create a new WAL handler
    pub fn new(inner: H) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<H: WalCommandHandler> RpcHandler for WalHandler<H> {
    // #[instrument(skip(self, message))] // TODO: Add this back in if we support instrument
    async fn handle_message(
        &self,
        message_id: &str,
        message: Bytes,
        _pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        debug!("Received WAL command: {}", message_id);

        // Route based on message_id
        match message_id {
            "wal.append_logs" => {
                // Decode AppendLogsRequest
                let request = AppendLogsRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode AppendLogsRequest: {e}"
                    )))
                })?;

                // Handle the request
                match self.inner.handle_append_logs(request).await {
                    Ok(resp) => {
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                    Err(e) => {
                        error!("Append logs failed: {e}");
                        let resp = AppendLogsResponse {
                            success: false,
                            error: Some(e.to_string()),
                        };
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                }
            }
            "wal.confirm_batch" => {
                // Decode ConfirmBatchRequest
                let request = ConfirmBatchRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode ConfirmBatchRequest: {e}"
                    )))
                })?;

                // Handle the request
                match self.inner.handle_confirm_batch(request).await {
                    Ok(resp) => {
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                    Err(e) => {
                        error!("Confirm batch failed: {e}");
                        let resp = ConfirmBatchResponse {
                            success: false,
                            error: Some(e.to_string()),
                        };
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                }
            }
            "wal.sync" => {
                // Decode SyncRequest
                let request = SyncRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode SyncRequest: {e}"
                    )))
                })?;

                // Handle the request
                match self.inner.handle_sync(request).await {
                    Ok(resp) => {
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                    Err(e) => {
                        error!("Sync failed: {e}");
                        let resp = SyncResponse {
                            success: false,
                            error: Some(e.to_string()),
                        };
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                }
            }
            "wal.get_pending_batches" => {
                // Decode GetPendingBatchesRequest
                let request = GetPendingBatchesRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode GetPendingBatchesRequest: {e}"
                    )))
                })?;

                // Handle the request
                match self.inner.handle_get_pending_batches(request).await {
                    Ok(resp) => {
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                    Err(e) => {
                        error!("Get pending batches failed: {e}");
                        let resp = GetPendingBatchesResponse {
                            batches: Vec::new(),
                            error: Some(e.to_string()),
                        };
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                }
            }
            "wal.set_metadata" => {
                // Decode SetMetadataRequest
                let request = SetMetadataRequest::try_from(message).map_err(|e| {
                    proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                        "Failed to decode SetMetadataRequest: {e}"
                    )))
                })?;

                // Handle the request
                match self.inner.handle_set_metadata(request).await {
                    Ok(resp) => {
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                    Err(e) => {
                        error!("Set metadata failed: {e}");
                        let resp = SetMetadataResponse {
                            success: false,
                            error: Some(e.to_string()),
                        };
                        let response_bytes: Bytes =
                            resp.try_into().map_err(|e: bincode::Error| {
                                proven_vsock_rpc::Error::Handler(HandlerError::Internal(format!(
                                    "Failed to encode response: {e}"
                                )))
                            })?;
                        Ok(HandlerResponse::Single(response_bytes))
                    }
                }
            }
            _ => Err(proven_vsock_rpc::Error::Handler(HandlerError::NotFound(
                format!("Unknown message_id: {message_id}"),
            ))),
        }
    }

    async fn on_connect(
        &self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) -> proven_vsock_rpc::Result<()> {
        info!("WAL client connected from {:?}", addr);
        Ok(())
    }

    async fn on_disconnect(
        &self,
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
    ) {
        info!("WAL client disconnected from {addr:?}");
    }
}

/// WAL server that listens for commands
pub struct WalServer<H: WalCommandHandler> {
    inner: RpcServer<WalHandler<H>>,
}

impl<H: WalCommandHandler> WalServer<H> {
    /// Create a new WAL server
    pub fn new(
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
        handler: H,
    ) -> Self {
        let wal_handler = WalHandler::new(handler);
        let config = ServerConfig::default();
        let inner = RpcServer::new(addr, wal_handler, config);

        Self { inner }
    }

    /// Create a new WAL server with custom configuration
    pub fn with_config(
        #[cfg(target_os = "linux")] addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] addr: std::net::SocketAddr,
        handler: H,
        config: ServerConfig,
    ) -> Self {
        let wal_handler = WalHandler::new(handler);
        let inner = RpcServer::new(addr, wal_handler, config);

        Self { inner }
    }

    /// Start serving WAL commands
    pub async fn serve(self) -> proven_vsock_rpc::Result<()> {
        info!("Starting WAL server");
        self.inner.serve().await
    }
}

/// Default WAL command handler implementation
pub struct DefaultWalHandler {
    /// WAL directory
    wal_dir: PathBuf,
    /// Active WAL files by namespace
    active_files: Arc<RwLock<HashMap<String, Arc<Mutex<File>>>>>,
    /// Pending batches
    pending_batches: Arc<RwLock<HashMap<String, PendingBatch>>>,
    /// Metadata storage by namespace
    metadata: MetadataStorage,
}

impl DefaultWalHandler {
    /// Create a new default WAL handler
    pub async fn new(wal_dir: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        fs::create_dir_all(&wal_dir).await?;

        Ok(Self {
            wal_dir,
            active_files: Arc::new(RwLock::new(HashMap::new())),
            pending_batches: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get or create WAL file for namespace
    async fn get_wal_file(
        &self,
        namespace: &str,
    ) -> Result<Arc<Mutex<File>>, Box<dyn std::error::Error>> {
        let mut files = self.active_files.write().await;

        if let Some(file) = files.get(namespace) {
            return Ok(Arc::clone(file));
        }

        let file_path = self.wal_dir.join(format!("{namespace}.wal"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?;

        let file = Arc::new(Mutex::new(file));
        files.insert(namespace.to_string(), Arc::clone(&file));

        Ok(file)
    }
}

#[async_trait]
impl WalCommandHandler for DefaultWalHandler {
    async fn handle_append_logs(
        &self,
        request: AppendLogsRequest,
    ) -> Result<AppendLogsResponse, Box<dyn std::error::Error>> {
        // Get WAL file for namespace
        let file = self.get_wal_file(&request.namespace).await?;

        // Serialize entries
        let entry_data = serde_json::to_vec(&request.entries)?;

        // Write to WAL
        {
            let mut file = file.lock().await;
            file.write_all(&(entry_data.len() as u32).to_le_bytes())
                .await?;
            file.write_all(&entry_data).await?;
            file.sync_all().await?;
        }

        // Store pending batch
        let batch = PendingBatch {
            batch_id: request.batch_id.clone(),
            namespace: request.namespace.clone(),
            entries: request.entries,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        self.pending_batches
            .write()
            .await
            .insert(request.batch_id, batch);

        Ok(AppendLogsResponse {
            success: true,
            error: None,
        })
    }

    async fn handle_confirm_batch(
        &self,
        request: ConfirmBatchRequest,
    ) -> Result<ConfirmBatchResponse, Box<dyn std::error::Error>> {
        // Remove from pending batches
        let removed = self.pending_batches.write().await.remove(&request.batch_id);

        Ok(ConfirmBatchResponse {
            success: removed.is_some(),
            error: if removed.is_none() {
                Some(format!("Batch {} not found", request.batch_id))
            } else {
                None
            },
        })
    }

    async fn handle_sync(
        &self,
        _request: SyncRequest,
    ) -> Result<SyncResponse, Box<dyn std::error::Error>> {
        // Sync all open files
        let files = self.active_files.read().await;
        for (_, file) in files.iter() {
            let file = file.lock().await;
            file.sync_all().await?;
        }

        Ok(SyncResponse {
            success: true,
            error: None,
        })
    }

    async fn handle_get_pending_batches(
        &self,
        request: GetPendingBatchesRequest,
    ) -> Result<GetPendingBatchesResponse, Box<dyn std::error::Error>> {
        let pending = self.pending_batches.read().await;

        let batches: Vec<PendingBatch> = if let Some(filter) = request.namespace_filter {
            pending
                .values()
                .filter(|b| b.namespace == filter)
                .cloned()
                .collect()
        } else {
            pending.values().cloned().collect()
        };

        Ok(GetPendingBatchesResponse {
            batches,
            error: None,
        })
    }

    async fn handle_set_metadata(
        &self,
        request: SetMetadataRequest,
    ) -> Result<SetMetadataResponse, Box<dyn std::error::Error>> {
        // Store metadata in memory and persist to disk
        {
            let mut metadata = self.metadata.write().await;
            metadata
                .entry(request.namespace.clone())
                .or_insert_with(HashMap::new)
                .insert(request.key.clone(), request.value.clone());
        }

        // Persist metadata to disk
        let metadata_file_path = self.wal_dir.join(format!("{}.metadata", request.namespace));
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&metadata_file_path)
            .await?;

        // Read all metadata for this namespace and write it
        let metadata = self.metadata.read().await;
        if let Some(namespace_metadata) = metadata.get(&request.namespace) {
            let serialized = serde_json::to_vec(namespace_metadata)?;
            file.write_all(&serialized).await?;
            file.sync_all().await?;
        }

        Ok(SetMetadataResponse {
            success: true,
            error: None,
        })
    }
}
