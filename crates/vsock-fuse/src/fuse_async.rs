//! Async operations handler for FUSE filesystem
//!
//! This module handles all async operations requested by the FUSE filesystem
//! through a channel-based architecture to avoid block_on calls.

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, instrument};

use crate::{
    BlobId, DirectoryId, FileId, FileMetadata, TierHint,
    encryption::EncryptionLayer,
    error::{Result, VsockFuseError},
    metadata::{DirectoryEntry, LocalEncryptedMetadataStore},
    storage::BlobStorage,
};

/// Operations that can be sent to the async handler
#[derive(Debug)]
pub enum FuseOperation {
    /// Lookup a file or directory
    Lookup {
        parent_id: DirectoryId,
        name: Vec<u8>,
        reply: oneshot::Sender<Result<Option<DirectoryEntry>>>,
    },
    /// Get file metadata
    GetMetadata {
        file_id: FileId,
        reply: oneshot::Sender<Result<Option<FileMetadata>>>,
    },
    /// Create a file
    CreateFile {
        parent_id: DirectoryId,
        name: Vec<u8>,
        metadata: FileMetadata,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Create a directory
    CreateDirectory {
        parent_id: DirectoryId,
        name: Vec<u8>,
        metadata: FileMetadata,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Read file data
    ReadFile {
        file_id: FileId,
        offset: u64,
        size: u32,
        reply: oneshot::Sender<Result<Vec<u8>>>,
    },
    /// Write file data
    WriteFile {
        file_id: FileId,
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<usize>>,
    },
    /// Delete a file
    DeleteFile {
        parent_id: DirectoryId,
        name: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Delete a directory
    DeleteDirectory {
        parent_id: DirectoryId,
        name: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// List directory contents
    ListDirectory {
        dir_id: DirectoryId,
        reply: oneshot::Sender<Result<Vec<DirectoryEntry>>>,
    },
    /// Rename an entry
    Rename {
        old_parent: DirectoryId,
        old_name: Vec<u8>,
        new_parent: DirectoryId,
        new_name: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Get storage statistics
    GetStats {
        reply: oneshot::Sender<Result<crate::storage::StorageStats>>,
    },
    /// Update metadata
    UpdateMetadata {
        file_id: FileId,
        metadata: FileMetadata,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Flush file data
    FlushFile {
        file_id: FileId,
        reply: oneshot::Sender<Result<()>>,
    },
}

/// Async operations handler
pub struct FuseAsyncHandler {
    /// Metadata store
    metadata: Arc<LocalEncryptedMetadataStore>,
    /// Blob storage
    storage: Arc<dyn BlobStorage>,
    /// Encryption layer
    crypto: Arc<EncryptionLayer>,
    /// Block size for file operations
    block_size: usize,
}

impl FuseAsyncHandler {
    /// Create a new async handler
    pub fn new(
        metadata: Arc<LocalEncryptedMetadataStore>,
        storage: Arc<dyn BlobStorage>,
        crypto: Arc<EncryptionLayer>,
        block_size: usize,
    ) -> Self {
        Self {
            metadata,
            storage,
            crypto,
            block_size,
        }
    }

    /// Run the async handler
    pub async fn run(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<FuseOperation>) {
        info!("FUSE async handler started");

        while let Some(op) = rx.recv().await {
            info!("Received operation: {:?}", std::mem::discriminant(&op));

            // Spawn a new task for each operation to enable concurrent processing
            let handler = self.clone();
            let task = tokio::spawn(async move {
                info!("Starting to process operation in spawned task");
                match op {
                    FuseOperation::Lookup {
                        parent_id,
                        name,
                        reply,
                    } => {
                        let result = handler.handle_lookup(parent_id, &name).await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::GetMetadata { file_id, reply } => {
                        let result = handler.handle_get_metadata(file_id).await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::CreateFile {
                        parent_id,
                        name,
                        metadata,
                        reply,
                    } => {
                        info!(
                            "Handling CreateFile operation: parent_id={:?}, name={:?}",
                            parent_id,
                            String::from_utf8_lossy(&name)
                        );
                        let result = handler.handle_create_file(parent_id, &name, metadata).await;
                        info!("CreateFile result: {:?}", result.is_ok());
                        let _ = reply.send(result);
                    }
                    FuseOperation::CreateDirectory {
                        parent_id,
                        name,
                        metadata,
                        reply,
                    } => {
                        let result = handler
                            .handle_create_directory(parent_id, &name, metadata)
                            .await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::ReadFile {
                        file_id,
                        offset,
                        size,
                        reply,
                    } => {
                        info!(
                            "Handling ReadFile operation: file_id={:?}, offset={}, size={}",
                            file_id, offset, size
                        );
                        let result = handler.handle_read_file(file_id, offset, size).await;
                        info!(
                            "ReadFile result: {} bytes",
                            result.as_ref().map(|v| v.len()).unwrap_or(0)
                        );
                        let _ = reply.send(result);
                    }
                    FuseOperation::WriteFile {
                        file_id,
                        offset,
                        data,
                        reply,
                    } => {
                        let result = handler.handle_write_file(file_id, offset, data).await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::DeleteFile {
                        parent_id,
                        name,
                        reply,
                    } => {
                        let result = handler.handle_delete_file(parent_id, &name).await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::DeleteDirectory {
                        parent_id,
                        name,
                        reply,
                    } => {
                        let result = handler.handle_delete_directory(parent_id, &name).await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::ListDirectory { dir_id, reply } => {
                        info!("Handling ListDirectory operation for {:?}", dir_id);
                        let result = handler.handle_list_directory(dir_id).await;
                        info!(
                            "ListDirectory result: {} entries",
                            result.as_ref().map(|v| v.len()).unwrap_or(0)
                        );
                        let _ = reply.send(result);
                    }
                    FuseOperation::Rename {
                        old_parent,
                        old_name,
                        new_parent,
                        new_name,
                        reply,
                    } => {
                        let result = handler
                            .handle_rename(old_parent, &old_name, new_parent, &new_name)
                            .await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::GetStats { reply } => {
                        info!("Handling GetStats operation");
                        let result = handler.handle_get_stats().await;
                        info!("GetStats result: {:?}", result.is_ok());
                        let _ = reply.send(result);
                    }
                    FuseOperation::UpdateMetadata {
                        file_id,
                        metadata,
                        reply,
                    } => {
                        let result = handler.handle_update_metadata(file_id, metadata).await;
                        let _ = reply.send(result);
                    }
                    FuseOperation::FlushFile { file_id, reply } => {
                        let result = handler.handle_flush_file(file_id).await;
                        let _ = reply.send(result);
                    }
                }
            });
        }

        info!("FUSE async handler stopped");
    }

    #[instrument(skip(self))]
    async fn handle_lookup(
        &self,
        parent_id: DirectoryId,
        name: &[u8],
    ) -> Result<Option<DirectoryEntry>> {
        info!(
            "Looking up {:?} in parent {:?}",
            String::from_utf8_lossy(name),
            parent_id
        );
        let result = self.metadata.lookup_entry(&parent_id, name).await;
        info!("Lookup result: {:?}", result.is_ok());
        result
    }

    #[instrument(skip(self))]
    async fn handle_get_metadata(&self, file_id: FileId) -> Result<Option<FileMetadata>> {
        self.metadata.get_metadata(&file_id).await
    }

    #[instrument(skip(self, metadata))]
    async fn handle_create_file(
        &self,
        parent_id: DirectoryId,
        name: &[u8],
        metadata: FileMetadata,
    ) -> Result<()> {
        self.metadata.create_file(&parent_id, name, metadata).await
    }

    #[instrument(skip(self, metadata))]
    async fn handle_create_directory(
        &self,
        parent_id: DirectoryId,
        name: &[u8],
        metadata: FileMetadata,
    ) -> Result<()> {
        self.metadata
            .create_directory(&parent_id, name, metadata)
            .await
    }

    #[instrument(skip(self))]
    async fn handle_read_file(&self, file_id: FileId, offset: u64, size: u32) -> Result<Vec<u8>> {
        // Load metadata to get block information
        let metadata = self.metadata.get_metadata(&file_id).await?.ok_or_else(|| {
            VsockFuseError::NotFound {
                path: std::path::PathBuf::from(format!("file_id: {file_id:?}")),
            }
        })?;

        // Calculate block range
        let start_block = offset / self.block_size as u64;
        let end_block = (offset + size as u64 - 1) / self.block_size as u64;

        let mut result = Vec::with_capacity(size as usize);

        for block_num in start_block..=end_block {
            if let Some(block_loc) = metadata.blocks.iter().find(|b| b.block_num == block_num) {
                // Read the block
                let blob_data = self.storage.get_blob(block_loc.blob_id).await?;

                // Decrypt the block
                let decrypted = self.crypto.decrypt_block(
                    &file_id,
                    block_num,
                    &blob_data.data[block_loc.offset as usize
                        ..block_loc.offset as usize + block_loc.encrypted_size],
                )?;

                // Calculate offsets within the block
                let block_start = block_num * self.block_size as u64;
                let data_start = if block_num == start_block {
                    (offset - block_start) as usize
                } else {
                    0
                };
                let data_end = if block_num == end_block {
                    ((offset + size as u64) - block_start) as usize
                } else {
                    decrypted.len()
                };

                result.extend_from_slice(&decrypted[data_start..data_end.min(decrypted.len())]);
            }
        }

        Ok(result)
    }

    #[instrument(skip(self, data))]
    async fn handle_write_file(
        &self,
        file_id: FileId,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<usize> {
        // Load metadata
        let mut metadata = self.metadata.get_metadata(&file_id).await?.ok_or_else(|| {
            VsockFuseError::NotFound {
                path: std::path::PathBuf::from(format!("file_id: {file_id:?}")),
            }
        })?;

        // Calculate affected blocks
        let start_block = offset / self.block_size as u64;
        let end_block = (offset + data.len() as u64 - 1) / self.block_size as u64;

        let mut written = 0;

        for block_num in start_block..=end_block {
            let block_start = block_num * self.block_size as u64;
            let block_end = block_start + self.block_size as u64;

            // Determine data range for this block
            let data_start = if block_num == start_block {
                0
            } else {
                ((block_start - offset) as usize).min(data.len())
            };
            let data_end = if block_num == end_block {
                data.len()
            } else {
                ((block_end - offset) as usize).min(data.len())
            };

            // Handle partial block updates
            let block_data = if (block_num == start_block && offset > block_start)
                || (block_num == end_block && (offset + data.len() as u64) < block_end)
            {
                // Need to read existing block for partial update
                let mut existing_data = vec![0u8; self.block_size];

                if let Some(block_loc) = metadata.blocks.iter().find(|b| b.block_num == block_num) {
                    let blob_data = self.storage.get_blob(block_loc.blob_id).await?;
                    let decrypted = self.crypto.decrypt_block(
                        &file_id,
                        block_num,
                        &blob_data.data[block_loc.offset as usize
                            ..block_loc.offset as usize + block_loc.encrypted_size],
                    )?;
                    existing_data[..decrypted.len()].copy_from_slice(&decrypted);
                }

                // Update with new data
                let block_offset = if block_num == start_block {
                    (offset - block_start) as usize
                } else {
                    0
                };
                existing_data[block_offset..block_offset + (data_end - data_start)]
                    .copy_from_slice(&data[data_start..data_end]);

                existing_data
            } else {
                data[data_start..data_end].to_vec()
            };

            // Encrypt the block
            let encrypted = self
                .crypto
                .encrypt_block(&file_id, block_num, &block_data)?;

            // Create a new blob for this block
            let blob_id = BlobId::new();
            self.storage
                .store_blob(blob_id, encrypted.clone(), TierHint::PreferHot)
                .await?;

            // Update block location
            if let Some(block_loc) = metadata
                .blocks
                .iter_mut()
                .find(|b| b.block_num == block_num)
            {
                block_loc.blob_id = blob_id;
                block_loc.offset = 0;
                block_loc.encrypted_size = encrypted.len();
            } else {
                metadata.blocks.push(crate::BlockLocation {
                    block_num,
                    blob_id,
                    offset: 0,
                    encrypted_size: encrypted.len(),
                });
            }

            written += data_end - data_start;
        }

        // Update file size and modification time
        metadata.size = metadata.size.max(offset + data.len() as u64);
        metadata.modified_at = std::time::SystemTime::now();

        // Save updated metadata
        self.metadata
            .update_metadata_direct(&file_id, metadata)
            .await?;

        Ok(written)
    }

    #[instrument(skip(self))]
    async fn handle_delete_file(&self, parent_id: DirectoryId, name: &[u8]) -> Result<()> {
        self.metadata.delete_file(&parent_id, name).await
    }

    #[instrument(skip(self))]
    async fn handle_delete_directory(&self, parent_id: DirectoryId, name: &[u8]) -> Result<()> {
        self.metadata.delete_directory(&parent_id, name).await
    }

    #[instrument(skip(self))]
    async fn handle_list_directory(&self, dir_id: DirectoryId) -> Result<Vec<DirectoryEntry>> {
        self.metadata.list_directory(&dir_id).await
    }

    #[instrument(skip(self))]
    async fn handle_rename(
        &self,
        old_parent: DirectoryId,
        old_name: &[u8],
        new_parent: DirectoryId,
        new_name: &[u8],
    ) -> Result<()> {
        self.metadata
            .rename_entry(&old_parent, old_name, &new_parent, new_name)
            .await
    }

    #[instrument(skip(self))]
    async fn handle_get_stats(&self) -> Result<crate::storage::StorageStats> {
        // Get local metadata statistics
        let metadata_stats = self.metadata.get_stats().await;

        // Convert to storage stats format
        // For now, use reasonable defaults for a local filesystem
        // In production, these would be calculated based on actual enclave resources
        let total_bytes = 10 * 1024 * 1024 * 1024; // 10GB total
        let used_bytes = metadata_stats.total_size;

        Ok(crate::storage::StorageStats {
            hot_tier: crate::storage::TierStats {
                total_bytes,
                used_bytes,
                file_count: metadata_stats.total_files as u64,
                read_ops_per_sec: 0.0,
                write_ops_per_sec: 0.0,
            },
            cold_tier: crate::storage::TierStats {
                total_bytes: 0,
                used_bytes: 0,
                file_count: 0,
                read_ops_per_sec: 0.0,
                write_ops_per_sec: 0.0,
            },
            migration_queue_size: 0,
        })
    }

    #[instrument(skip(self, metadata))]
    async fn handle_update_metadata(&self, file_id: FileId, metadata: FileMetadata) -> Result<()> {
        self.metadata
            .update_metadata_direct(&file_id, metadata)
            .await
    }

    #[instrument(skip(self))]
    async fn handle_flush_file(&self, _file_id: FileId) -> Result<()> {
        // For now, we write through, so nothing to flush
        Ok(())
    }
}
