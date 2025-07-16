//! Tests for async FUSE operations

use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};

use proven_vsock_fuse::{
    DirectoryId, FileId, FileMetadata, FileType,
    encryption::{EncryptionLayer, MasterKey},
    fuse_async::{FuseAsyncHandler, FuseOperation},
    metadata::LocalEncryptedMetadataStore,
    storage::{StorageStats, TierStats},
};

// We need to create our own mock storage for tests
use parking_lot::RwLock;
use proven_vsock_fuse::{
    BlobId, StorageTier, TierHint,
    storage::{BlobData, BlobInfo, BlobStorage},
};
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

    pub fn get_stats(&self) -> MockStorageStats {
        let data = self.data.read();
        let total_size: u64 = data.values().map(|v| v.len() as u64).sum();
        MockStorageStats {
            blob_count: data.len(),
            total_size,
        }
    }
}

pub struct MockStorageStats {
    pub blob_count: usize,
    pub total_size: u64,
}

#[async_trait::async_trait]
impl BlobStorage for MockBlobStorage {
    async fn store_blob(
        &self,
        blob_id: BlobId,
        data: Vec<u8>,
        _tier_hint: TierHint,
    ) -> proven_vsock_fuse::error::Result<()> {
        self.data.write().insert(blob_id, data);
        Ok(())
    }

    async fn get_blob(&self, blob_id: BlobId) -> proven_vsock_fuse::error::Result<BlobData> {
        self.data
            .read()
            .get(&blob_id)
            .cloned()
            .map(|data| BlobData {
                data,
                tier: StorageTier::Hot,
            })
            .ok_or(proven_vsock_fuse::error::VsockFuseError::BlobNotFound { id: blob_id })
    }

    async fn delete_blob(&self, blob_id: BlobId) -> proven_vsock_fuse::error::Result<()> {
        self.data.write().remove(&blob_id);
        Ok(())
    }

    async fn list_blobs(
        &self,
        prefix: Option<&[u8]>,
    ) -> proven_vsock_fuse::error::Result<Vec<BlobInfo>> {
        let data = self.data.read();
        let blobs: Vec<BlobInfo> = data
            .iter()
            .filter(|(blob_id, _)| {
                if let Some(prefix) = prefix {
                    let blob_bytes = blob_id.0;
                    blob_bytes.starts_with(prefix)
                } else {
                    true
                }
            })
            .map(|(blob_id, data)| BlobInfo {
                blob_id: *blob_id,
                size: data.len() as u64,
                tier: StorageTier::Hot,
                created_at: std::time::SystemTime::now(),
                last_accessed: std::time::SystemTime::now(),
            })
            .collect();
        Ok(blobs)
    }

    async fn get_stats(&self) -> proven_vsock_fuse::error::Result<StorageStats> {
        let data = self.data.read();
        let total_size: u64 = data.values().map(|v| v.len() as u64).sum();
        Ok(StorageStats {
            hot_tier: TierStats {
                total_bytes: 1024 * 1024 * 1024, // 1GB
                used_bytes: total_size,
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

async fn create_test_handler() -> (FuseAsyncHandler, Arc<MockBlobStorage>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let master_key = MasterKey::generate();
    let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
    let storage = Arc::new(MockBlobStorage::new());

    let metadata_store = Arc::new(
        LocalEncryptedMetadataStore::with_journal(crypto.clone(), None, temp_dir.path()).unwrap(),
    );

    let handler = FuseAsyncHandler::new(metadata_store, storage.clone(), crypto, 4096);

    (handler, storage, temp_dir)
}

#[tokio::test]
async fn test_fuse_async_file_operations() {
    let (handler, _storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        handler_clone.run(rx).await;
    });

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Create a file
    let file_id = FileId::new();
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"test_async.txt".to_vec(),
        size: 0,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o644,
        file_type: FileType::Regular,
        nlink: 1,
        uid: 1000,
        gid: 1000,
    };

    // Test create file
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateFile {
        parent_id,
        name: b"test_async.txt".to_vec(),
        metadata: metadata.clone(),
        reply: reply_tx,
    })
    .unwrap();
    let result = reply_rx.await.unwrap();
    assert!(result.is_ok());

    // Test lookup
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::Lookup {
        parent_id,
        name: b"test_async.txt".to_vec(),
        reply: reply_tx,
    })
    .unwrap();
    let lookup_result = reply_rx.await.unwrap().unwrap();
    assert!(lookup_result.is_some());
    let entry = lookup_result.unwrap();
    assert_eq!(entry.file_id, file_id);

    // Test write
    let write_data = b"Hello, async FUSE!";
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::WriteFile {
        file_id,
        offset: 0,
        data: write_data.to_vec(),
        reply: reply_tx,
    })
    .unwrap();
    let written = reply_rx.await.unwrap().unwrap();
    assert_eq!(written, write_data.len());

    // Test read
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::ReadFile {
        file_id,
        offset: 0,
        size: write_data.len() as u32,
        reply: reply_tx,
    })
    .unwrap();
    let read_data = reply_rx.await.unwrap().unwrap();
    assert_eq!(read_data, write_data);

    // Test metadata update
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::GetMetadata {
        file_id,
        reply: reply_tx,
    })
    .unwrap();
    let updated_metadata = reply_rx.await.unwrap().unwrap().unwrap();
    assert_eq!(updated_metadata.size, write_data.len() as u64);
    assert!(!updated_metadata.blocks.is_empty());

    // Close channel and wait for handler
    drop(tx);
    handler_task.await.unwrap();
}

#[tokio::test]
async fn test_fuse_async_directory_operations() {
    let (handler, _storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        handler_clone.run(rx).await;
    });

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Create a directory
    let dir_id = FileId::new();
    let dir_metadata = FileMetadata {
        file_id: dir_id,
        parent_id: Some(parent_id),
        encrypted_name: b"test_dir".to_vec(),
        size: 4096,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o755,
        file_type: FileType::Directory,
        nlink: 2,
        uid: 1000,
        gid: 1000,
    };

    // Create directory
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateDirectory {
        parent_id,
        name: b"test_dir".to_vec(),
        metadata: dir_metadata,
        reply: reply_tx,
    })
    .unwrap();
    assert!(reply_rx.await.unwrap().is_ok());

    // List parent directory
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::ListDirectory {
        dir_id: parent_id,
        reply: reply_tx,
    })
    .unwrap();
    let entries = reply_rx.await.unwrap().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, b"test_dir");
    assert_eq!(entries[0].file_type, FileType::Directory);

    // Create file in subdirectory
    let file_in_dir = FileId::new();
    let file_metadata = FileMetadata {
        file_id: file_in_dir,
        parent_id: Some(DirectoryId(dir_id)),
        encrypted_name: b"file_in_dir.txt".to_vec(),
        size: 0,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o644,
        file_type: FileType::Regular,
        nlink: 1,
        uid: 1000,
        gid: 1000,
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateFile {
        parent_id: DirectoryId(dir_id),
        name: b"file_in_dir.txt".to_vec(),
        metadata: file_metadata,
        reply: reply_tx,
    })
    .unwrap();
    assert!(reply_rx.await.unwrap().is_ok());

    // List subdirectory
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::ListDirectory {
        dir_id: DirectoryId(dir_id),
        reply: reply_tx,
    })
    .unwrap();
    let subdir_entries = reply_rx.await.unwrap().unwrap();
    assert_eq!(subdir_entries.len(), 1);
    assert_eq!(subdir_entries[0].name, b"file_in_dir.txt");

    // Close channel and wait for handler
    drop(tx);
    handler_task.await.unwrap();
}

#[tokio::test]
async fn test_fuse_async_rename_operations() {
    let (handler, _storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        handler_clone.run(rx).await;
    });

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Create a file
    let file_id = FileId::new();
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"original.txt".to_vec(),
        size: 0,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o644,
        file_type: FileType::Regular,
        nlink: 1,
        uid: 1000,
        gid: 1000,
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateFile {
        parent_id,
        name: b"original.txt".to_vec(),
        metadata,
        reply: reply_tx,
    })
    .unwrap();
    assert!(reply_rx.await.unwrap().is_ok());

    // Rename the file
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::Rename {
        old_parent: parent_id,
        old_name: b"original.txt".to_vec(),
        new_parent: parent_id,
        new_name: b"renamed.txt".to_vec(),
        reply: reply_tx,
    })
    .unwrap();
    assert!(reply_rx.await.unwrap().is_ok());

    // Old name should not exist
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::Lookup {
        parent_id,
        name: b"original.txt".to_vec(),
        reply: reply_tx,
    })
    .unwrap();
    let old_lookup = reply_rx.await.unwrap().unwrap();
    assert!(old_lookup.is_none());

    // New name should exist
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::Lookup {
        parent_id,
        name: b"renamed.txt".to_vec(),
        reply: reply_tx,
    })
    .unwrap();
    let new_lookup = reply_rx.await.unwrap().unwrap();
    assert!(new_lookup.is_some());
    assert_eq!(new_lookup.unwrap().file_id, file_id);

    // Close channel and wait for handler
    drop(tx);
    handler_task.await.unwrap();
}

#[tokio::test]
async fn test_fuse_async_delete_operations() {
    let (handler, _storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        handler_clone.run(rx).await;
    });

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Create a file
    let file_id = FileId::new();
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"to_delete.txt".to_vec(),
        size: 0,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o644,
        file_type: FileType::Regular,
        nlink: 1,
        uid: 1000,
        gid: 1000,
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateFile {
        parent_id,
        name: b"to_delete.txt".to_vec(),
        metadata,
        reply: reply_tx,
    })
    .unwrap();
    assert!(reply_rx.await.unwrap().is_ok());

    // Delete the file
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::DeleteFile {
        parent_id,
        name: b"to_delete.txt".to_vec(),
        reply: reply_tx,
    })
    .unwrap();
    assert!(reply_rx.await.unwrap().is_ok());

    // File should not exist
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::Lookup {
        parent_id,
        name: b"to_delete.txt".to_vec(),
        reply: reply_tx,
    })
    .unwrap();
    let lookup_result = reply_rx.await.unwrap().unwrap();
    assert!(lookup_result.is_none());

    // Close channel and wait for handler
    drop(tx);
    handler_task.await.unwrap();
}

#[tokio::test]
async fn test_fuse_async_channel_operations() {
    let (handler, _storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        handler_clone.run(rx).await;
    });

    // Send operations through channel
    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Test lookup operation
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::Lookup {
        parent_id,
        name: b"nonexistent.txt".to_vec(),
        reply: reply_tx,
    })
    .unwrap();

    let lookup_result = reply_rx.await.unwrap();
    assert!(lookup_result.is_ok());
    assert!(lookup_result.unwrap().is_none());

    // Test create file operation
    let file_id = FileId::new();
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"channel_test.txt".to_vec(),
        size: 0,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o644,
        file_type: FileType::Regular,
        nlink: 1,
        uid: 1000,
        gid: 1000,
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateFile {
        parent_id,
        name: b"channel_test.txt".to_vec(),
        metadata,
        reply: reply_tx,
    })
    .unwrap();

    let create_result = reply_rx.await.unwrap();
    assert!(create_result.is_ok());

    // Test list directory operation
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::ListDirectory {
        dir_id: parent_id,
        reply: reply_tx,
    })
    .unwrap();

    let list_result = reply_rx.await.unwrap();
    assert!(list_result.is_ok());
    let entries = list_result.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, b"channel_test.txt");

    // Close channel
    drop(tx);

    // Wait for handler to finish
    handler_task.await.unwrap();
}

#[tokio::test]
#[ignore] // Temporarily ignore this test
async fn test_fuse_async_large_file_operations() {
    eprintln!("Starting test_fuse_async_large_file_operations");
    let (handler, storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        eprintln!("Handler task started for large file test");
        handler_clone.run(rx).await;
        eprintln!("Handler task finished");
    });

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));
    let file_id = FileId::new();

    eprintln!("Creating file...");
    // Create a file
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"large_file.bin".to_vec(),
        size: 0,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o644,
        file_type: FileType::Regular,
        nlink: 1,
        uid: 1000,
        gid: 1000,
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateFile {
        parent_id,
        name: b"large_file.bin".to_vec(),
        metadata,
        reply: reply_tx,
    })
    .unwrap();

    eprintln!("Waiting for create file response...");
    match reply_rx.await {
        Ok(Ok(_)) => eprintln!("File created successfully"),
        Ok(Err(e)) => {
            eprintln!("Failed to create file: {e:?}");
            panic!("File creation failed");
        }
        Err(e) => {
            eprintln!("Channel error: {e:?}");
            panic!("Channel error during file creation");
        }
    }

    eprintln!("Writing data in chunks...");
    // Write 1MB of data in chunks
    let chunk_size = 64 * 1024; // 64KB chunks
    let total_size = 1024 * 1024; // 1MB
    let mut offset = 0;

    for i in 0..(total_size / chunk_size) {
        eprintln!("Writing chunk {i} at offset {offset}");
        let data = vec![(i % 256) as u8; chunk_size];
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(FuseOperation::WriteFile {
            file_id,
            offset,
            data,
            reply: reply_tx,
        })
        .unwrap();

        // Add a small yield to ensure the handler task gets a chance to run
        tokio::task::yield_now().await;

        eprintln!("Waiting for write response...");
        match tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx).await {
            Ok(Ok(Ok(written))) => {
                eprintln!("Wrote {written} bytes");
                assert_eq!(written, chunk_size);
            }
            Ok(Ok(Err(e))) => {
                eprintln!("Write failed with error: {e:?}");
                panic!("Write operation failed: {e:?}");
            }
            Ok(Err(_)) => {
                eprintln!("oneshot channel error");
                panic!("Channel error");
            }
            Err(_) => {
                eprintln!("Write response timed out!");
                panic!("Timeout waiting for write response");
            }
        }
        offset += chunk_size as u64;
    }

    // Verify file size
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::GetMetadata {
        file_id,
        reply: reply_tx,
    })
    .unwrap();
    let final_metadata = reply_rx.await.unwrap().unwrap().unwrap();
    assert_eq!(final_metadata.size, total_size as u64);

    // Read back in different chunk sizes
    let read_chunk_size = 128 * 1024; // 128KB reads
    offset = 0;

    for _i in 0..(total_size / read_chunk_size) {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(FuseOperation::ReadFile {
            file_id,
            offset,
            size: read_chunk_size as u32,
            reply: reply_tx,
        })
        .unwrap();
        let data = reply_rx.await.unwrap().unwrap();
        assert_eq!(data.len(), read_chunk_size);

        // Verify data pattern
        let expected_value = ((offset / chunk_size as u64) % 256) as u8;
        assert_eq!(data[0], expected_value);

        offset += read_chunk_size as u64;
    }

    // Close channel and wait for handler
    eprintln!("Closing channel");
    drop(tx);
    eprintln!("Waiting for handler task to finish");
    match handler_task.await {
        Ok(_) => eprintln!("Handler task finished successfully"),
        Err(e) => eprintln!("Handler task panicked: {e:?}"),
    }

    // Verify storage statistics
    let stats = storage.get_stats();
    assert!(stats.total_size >= total_size as u64);
    assert!(stats.blob_count > 0);
}

#[tokio::test]
async fn test_fuse_async_single_block_write() {
    eprintln!("Starting test_fuse_async_single_block_write");
    let (handler, _storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        eprintln!("Handler task started for single block test");
        handler_clone.run(rx).await;
        eprintln!("Handler task finished");
    });

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));
    let file_id = FileId::new();

    eprintln!("Creating file...");
    // Create a file
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"single_block.bin".to_vec(),
        size: 0,
        blocks: vec![],
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
        permissions: 0o644,
        file_type: FileType::Regular,
        nlink: 1,
        uid: 1000,
        gid: 1000,
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::CreateFile {
        parent_id,
        name: b"single_block.bin".to_vec(),
        metadata,
        reply: reply_tx,
    })
    .unwrap();

    eprintln!("Waiting for create file response...");
    assert!(reply_rx.await.unwrap().is_ok());
    eprintln!("File created successfully");

    eprintln!("Writing a single 1KB block...");
    // Write a small amount of data (less than one block)
    let data = vec![0x42u8; 1024]; // 1KB of data
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(FuseOperation::WriteFile {
        file_id,
        offset: 0,
        data,
        reply: reply_tx,
    })
    .unwrap();

    eprintln!("Waiting for write response...");
    match tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx).await {
        Ok(Ok(Ok(written))) => {
            eprintln!("Wrote {written} bytes");
            assert_eq!(written, 1024);
        }
        Ok(Ok(Err(e))) => {
            eprintln!("Write failed with error: {e:?}");
            panic!("Write operation failed: {e:?}");
        }
        Ok(Err(_)) => {
            eprintln!("oneshot channel error");
            panic!("Channel error");
        }
        Err(_) => {
            eprintln!("Write response timed out!");
            panic!("Timeout waiting for write response");
        }
    }

    // Close channel and wait for handler
    eprintln!("Test completed, closing channel");
    drop(tx);
    handler_task.await.unwrap();
}
