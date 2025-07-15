//! WAL server implementation for S3 storage

use async_trait::async_trait;
use bytes::Bytes;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, RpcHandler, RpcMessage, RpcServer, ServerConfig, VsockAddr,
    protocol::codec,
};
use serde_json;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
};
use tracing::{debug, error, info, instrument};

use super::commands::*;

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
    #[instrument(skip(self, message))]
    async fn handle_message(
        &self,
        message: Bytes,
        _pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        // Decode the command
        let command: WalCommand = codec::decode(&message)?;

        debug!("Received WAL command: {:?}", command.message_id());

        // Handle the command
        let response = match command {
            WalCommand::AppendLogs(request) => match self.inner.handle_append_logs(request).await {
                Ok(resp) => WalResponse::AppendLogs(resp),
                Err(e) => {
                    error!("Append logs failed: {}", e);
                    WalResponse::AppendLogs(AppendLogsResponse {
                        success: false,
                        error: Some(e.to_string()),
                    })
                }
            },
            WalCommand::ConfirmBatch(request) => {
                match self.inner.handle_confirm_batch(request).await {
                    Ok(resp) => WalResponse::ConfirmBatch(resp),
                    Err(e) => {
                        error!("Confirm batch failed: {}", e);
                        WalResponse::ConfirmBatch(ConfirmBatchResponse {
                            success: false,
                            error: Some(e.to_string()),
                        })
                    }
                }
            }
            WalCommand::Sync(request) => match self.inner.handle_sync(request).await {
                Ok(resp) => WalResponse::Sync(resp),
                Err(e) => {
                    error!("Sync failed: {}", e);
                    WalResponse::Sync(SyncResponse {
                        success: false,
                        error: Some(e.to_string()),
                    })
                }
            },
            WalCommand::GetPendingBatches(request) => {
                match self.inner.handle_get_pending_batches(request).await {
                    Ok(resp) => WalResponse::GetPendingBatches(resp),
                    Err(e) => {
                        error!("Get pending batches failed: {}", e);
                        WalResponse::GetPendingBatches(GetPendingBatchesResponse {
                            batches: Vec::new(),
                            error: Some(e.to_string()),
                        })
                    }
                }
            }
        };

        // Encode the response
        let response_bytes = codec::encode(&response)?;

        Ok(HandlerResponse::Single(response_bytes))
    }

    async fn on_connect(&self, addr: VsockAddr) -> proven_vsock_rpc::Result<()> {
        info!("WAL client connected from {:?}", addr);
        Ok(())
    }

    async fn on_disconnect(&self, addr: VsockAddr) {
        info!("WAL client disconnected from {:?}", addr);
    }
}

/// WAL server that listens for commands
pub struct WalServer<H: WalCommandHandler> {
    inner: RpcServer<WalHandler<H>>,
}

impl<H: WalCommandHandler> WalServer<H> {
    /// Create a new WAL server
    pub fn new(addr: VsockAddr, handler: H) -> Self {
        let wal_handler = WalHandler::new(handler);
        let config = ServerConfig::default();
        let inner = RpcServer::new(addr, wal_handler, config);

        Self { inner }
    }

    /// Create a new WAL server with custom configuration
    pub fn with_config(addr: VsockAddr, handler: H, config: ServerConfig) -> Self {
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

/// File-based WAL implementation
pub struct FileBasedWalHandler {
    /// Base directory for WAL files
    base_dir: PathBuf,
    /// Pending batches: batch_id -> (namespace, entries, created_at)
    pending_batches: Arc<RwLock<HashMap<String, PendingBatch>>>,
    /// WAL file handle
    wal_file: Arc<Mutex<File>>,
}

impl FileBasedWalHandler {
    /// Create a new file-based WAL handler
    pub async fn new(base_dir: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        fs::create_dir_all(&base_dir).await?;

        // Open WAL file
        let wal_path = base_dir.join("wal.log");
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await?;

        // Load existing batches from WAL
        let pending_batches = Self::load_pending_batches(&base_dir).await?;

        Ok(Self {
            base_dir,
            pending_batches: Arc::new(RwLock::new(pending_batches)),
            wal_file: Arc::new(Mutex::new(wal_file)),
        })
    }

    /// Load pending batches from disk
    async fn load_pending_batches(
        base_dir: &Path,
    ) -> Result<HashMap<String, PendingBatch>, Box<dyn std::error::Error>> {
        let mut batches = HashMap::new();
        let batches_dir = base_dir.join("batches");

        if !batches_dir.exists() {
            return Ok(batches);
        }

        let mut entries = fs::read_dir(&batches_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Some(file_name) = entry.file_name().to_str()
                && file_name.ends_with(".batch")
            {
                let batch_id = file_name.trim_end_matches(".batch");
                let content = fs::read(entry.path()).await?;

                if let Ok(batch) = serde_json::from_slice::<PendingBatch>(&content) {
                    batches.insert(batch_id.to_string(), batch);
                }
            }
        }

        info!("Loaded {} pending batches from disk", batches.len());
        Ok(batches)
    }

    /// Write batch to disk
    async fn write_batch(&self, batch: &PendingBatch) -> Result<(), Box<dyn std::error::Error>> {
        let batches_dir = self.base_dir.join("batches");
        fs::create_dir_all(&batches_dir).await?;

        let batch_path = batches_dir.join(format!("{}.batch", batch.batch_id));
        let content = serde_json::to_vec(&batch)?;
        fs::write(&batch_path, content).await?;

        Ok(())
    }

    /// Delete batch from disk
    async fn delete_batch(&self, batch_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let batch_path = self
            .base_dir
            .join("batches")
            .join(format!("{batch_id}.batch"));
        if batch_path.exists() {
            fs::remove_file(&batch_path).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl WalCommandHandler for FileBasedWalHandler {
    async fn handle_append_logs(
        &self,
        request: AppendLogsRequest,
    ) -> Result<AppendLogsResponse, Box<dyn std::error::Error>> {
        let batch = PendingBatch {
            batch_id: request.batch_id.clone(),
            namespace: request.namespace,
            entries: request.entries,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        };

        // Write to disk
        self.write_batch(&batch).await?;

        // Add to in-memory map
        self.pending_batches
            .write()
            .await
            .insert(batch.batch_id.clone(), batch);

        // Log to WAL file
        let log_entry = format!("APPEND {} {}\n", request.batch_id, chrono::Utc::now());
        self.wal_file
            .lock()
            .await
            .write_all(log_entry.as_bytes())
            .await?;

        Ok(AppendLogsResponse {
            success: true,
            error: None,
        })
    }

    async fn handle_confirm_batch(
        &self,
        request: ConfirmBatchRequest,
    ) -> Result<ConfirmBatchResponse, Box<dyn std::error::Error>> {
        // Remove from in-memory map
        let removed = self.pending_batches.write().await.remove(&request.batch_id);

        if removed.is_some() {
            // Delete from disk
            self.delete_batch(&request.batch_id).await?;

            // Log to WAL file
            let log_entry = format!("CONFIRM {} {}\n", request.batch_id, chrono::Utc::now());
            self.wal_file
                .lock()
                .await
                .write_all(log_entry.as_bytes())
                .await?;

            Ok(ConfirmBatchResponse {
                success: true,
                error: None,
            })
        } else {
            Ok(ConfirmBatchResponse {
                success: false,
                error: Some(format!("Batch {} not found", request.batch_id)),
            })
        }
    }

    async fn handle_sync(
        &self,
        request: SyncRequest,
    ) -> Result<SyncResponse, Box<dyn std::error::Error>> {
        // Sync WAL file to disk
        let file = self.wal_file.lock().await;
        file.sync_all().await?;

        if request.force {
            // Also sync data directory
            let _ = std::fs::File::open(&self.base_dir)?.sync_all();
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
        let batches = self.pending_batches.read().await;

        let filtered_batches: Vec<PendingBatch> = if let Some(filter) = request.namespace_filter {
            batches
                .values()
                .filter(|b| b.namespace == filter)
                .cloned()
                .collect()
        } else {
            batches.values().cloned().collect()
        };

        Ok(GetPendingBatchesResponse {
            batches: filtered_batches,
            error: None,
        })
    }
}
