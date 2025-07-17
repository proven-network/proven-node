//! Minimal test to debug the 16KB write hang issue

use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};

use proven_vsock_fuse::{
    DirectoryId, FileId, FileMetadata, FileType,
    encryption::{EncryptionLayer, MasterKey},
    fuse_async::{FuseAsyncHandler, FuseOperation},
    metadata::LocalEncryptedMetadataStore,
};

use proven_vsock_fuse::storage::mock::MockBlobStorage;

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
async fn test_exact_16kb_write() {
    eprintln!("Starting test_exact_16kb_write");
    let (handler, _storage, _temp_dir) = create_test_handler().await;
    let handler = Arc::new(handler);

    // Create channel
    let (tx, rx) = mpsc::unbounded_channel::<FuseOperation>();

    // Spawn handler task
    let handler_clone = handler.clone();
    let handler_task = tokio::spawn(async move {
        eprintln!("Handler task started");
        handler_clone.run(rx).await;
        eprintln!("Handler task finished");
    });

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));
    let file_id = FileId::new();

    // Create a file
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"test.bin".to_vec(),
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
        name: b"test.bin".to_vec(),
        metadata,
        reply: reply_tx,
    })
    .unwrap();

    assert!(reply_rx.await.unwrap().is_ok());
    eprintln!("File created");

    // Test different sizes around the 16KB boundary
    for size in [15 * 1024, 16 * 1024, 17 * 1024] {
        eprintln!("\n=== Testing write of {size} bytes ===");

        let data = vec![0x42u8; size];
        let (reply_tx, reply_rx) = oneshot::channel();

        tx.send(FuseOperation::WriteFile {
            file_id,
            offset: 0,
            data,
            reply: reply_tx,
        })
        .unwrap();

        eprintln!("Sent write operation, waiting for response...");

        match tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx).await {
            Ok(Ok(Ok(written))) => {
                eprintln!("SUCCESS: Wrote {written} bytes");
            }
            Ok(Ok(Err(e))) => {
                eprintln!("ERROR: Write failed: {e:?}");
            }
            Ok(Err(_)) => {
                eprintln!("ERROR: Channel closed");
            }
            Err(_) => {
                eprintln!("TIMEOUT: Write operation timed out!");
                panic!("Write of {size} bytes timed out");
            }
        }
    }

    // Close channel and wait for handler
    drop(tx);
    handler_task.await.unwrap();
}

#[tokio::test]
async fn test_sequential_small_writes() {
    eprintln!("Starting test_sequential_small_writes");
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
    let file_id = FileId::new();

    // Create a file
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"sequential.bin".to_vec(),
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
        name: b"sequential.bin".to_vec(),
        metadata,
        reply: reply_tx,
    })
    .unwrap();

    assert!(reply_rx.await.unwrap().is_ok());

    // Write 4 blocks sequentially (each 4KB)
    for i in 0..4 {
        eprintln!("Writing block {i}");
        let offset = i * 4096;
        let data = vec![(i + 1) as u8; 4096];

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(FuseOperation::WriteFile {
            file_id,
            offset,
            data,
            reply: reply_tx,
        })
        .unwrap();

        match tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx).await {
            Ok(Ok(Ok(written))) => {
                eprintln!("Block {i} written: {written} bytes");
            }
            _ => {
                panic!("Failed to write block {i}");
            }
        }
    }

    eprintln!("All blocks written successfully");

    // Close channel and wait for handler
    drop(tx);
    handler_task.await.unwrap();
}
