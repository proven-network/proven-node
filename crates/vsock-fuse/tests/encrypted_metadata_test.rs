//! Tests for encrypted metadata store functionality

use std::sync::Arc;
use tempfile::TempDir;

use proven_vsock_fuse::{
    DirectoryId, FileId, FileMetadata, FileType,
    encryption::{EncryptionLayer, MasterKey},
    error::Result,
    metadata::{LocalEncryptedMetadataStore, MetadataStorage},
};

// Mock remote storage for testing
struct MockRemoteStorage {
    stored_blobs:
        Arc<parking_lot::RwLock<std::collections::HashMap<proven_vsock_fuse::BlobId, Vec<u8>>>>,
}

impl MockRemoteStorage {
    fn new() -> Self {
        Self {
            stored_blobs: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl MetadataStorage for MockRemoteStorage {
    async fn store_blob(&self, blob_id: proven_vsock_fuse::BlobId, data: Vec<u8>) -> Result<()> {
        self.stored_blobs.write().insert(blob_id, data);
        Ok(())
    }

    async fn get_blob(&self, blob_id: proven_vsock_fuse::BlobId) -> Result<Vec<u8>> {
        self.stored_blobs
            .read()
            .get(&blob_id)
            .cloned()
            .ok_or(proven_vsock_fuse::error::VsockFuseError::BlobNotFound { id: blob_id })
    }

    async fn delete_blob(&self, blob_id: proven_vsock_fuse::BlobId) -> Result<()> {
        self.stored_blobs.write().remove(&blob_id);
        Ok(())
    }

    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<proven_vsock_fuse::BlobId>> {
        let blobs = self.stored_blobs.read();
        if let Some(_prefix) = prefix {
            // For simplicity, return all blobs in test
            Ok(blobs.keys().copied().collect())
        } else {
            Ok(blobs.keys().copied().collect())
        }
    }
}

#[tokio::test]
async fn test_encrypted_metadata_basic_operations() {
    let master_key = MasterKey::generate();
    let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
    let remote = Arc::new(MockRemoteStorage::new());

    let store = LocalEncryptedMetadataStore::new(crypto.clone(), Some(remote.clone()));

    // Create a file
    let file_id = FileId::new();
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
        encrypted_name: b"test_encrypted.txt".to_vec(),
        size: 1024,
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

    // Save metadata
    store.save_metadata(&metadata).await.unwrap();

    // Retrieve metadata
    let retrieved = store.get_metadata(&file_id).await.unwrap().unwrap();
    assert_eq!(retrieved.file_id, file_id);
    assert_eq!(retrieved.size, 1024);
}

#[tokio::test]
async fn test_encrypted_metadata_with_journaling() {
    let temp_dir = TempDir::new().unwrap();
    let master_key = MasterKey::generate();
    let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
    let remote = Arc::new(MockRemoteStorage::new());

    // Create store with journaling
    let store = Arc::new(
        LocalEncryptedMetadataStore::with_journal(
            crypto.clone(),
            Some(remote.clone()),
            temp_dir.path(),
        )
        .unwrap(),
    );

    // Create files and directories
    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Create a subdirectory
    let dir_id = FileId::new();
    let dir_metadata = FileMetadata {
        file_id: dir_id,
        parent_id: Some(parent_id),
        encrypted_name: b"subdir".to_vec(),
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

    store
        .create_directory(&parent_id, b"subdir", dir_metadata.clone())
        .await
        .unwrap();

    // Create files in the subdirectory
    let subdir_id = DirectoryId(dir_id);
    for i in 0..5 {
        let file_id = FileId::new();
        let file_metadata = FileMetadata {
            file_id,
            parent_id: Some(subdir_id),
            encrypted_name: format!("file{i}.txt").into_bytes(),
            size: 100 * (i + 1) as u64,
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

        store
            .create_file(
                &subdir_id,
                &format!("file{i}.txt").into_bytes(),
                file_metadata,
            )
            .await
            .unwrap();
    }

    // List directory contents
    let root_contents = store.list_directory(&parent_id).await.unwrap();
    assert_eq!(root_contents.len(), 1);
    assert_eq!(root_contents[0].name, b"subdir");

    let subdir_contents = store.list_directory(&subdir_id).await.unwrap();
    assert_eq!(subdir_contents.len(), 5);

    // Sync journal before dropping
    if let Some(local_store) = store.get_local_store() {
        local_store.sync_journal().unwrap();
    }

    // Drop store to force flush
    drop(store);

    // Create new store and verify persistence
    let recovered_store = LocalEncryptedMetadataStore::with_journal(
        crypto.clone(),
        Some(remote.clone()),
        temp_dir.path(),
    )
    .unwrap();

    // Verify directory structure persisted
    let root_contents = recovered_store.list_directory(&parent_id).await.unwrap();
    assert_eq!(root_contents.len(), 1);

    let subdir_contents = recovered_store.list_directory(&subdir_id).await.unwrap();
    assert_eq!(subdir_contents.len(), 5);
}

#[tokio::test]
async fn test_encrypted_metadata_operations() {
    let master_key = MasterKey::generate();
    let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
    let store = LocalEncryptedMetadataStore::new(crypto, None);

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Test file creation
    let file_id = FileId::new();
    let metadata = FileMetadata {
        file_id,
        parent_id: Some(parent_id),
        encrypted_name: b"test.txt".to_vec(),
        size: 1024,
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

    store
        .create_file(&parent_id, b"test.txt", metadata.clone())
        .await
        .unwrap();

    // Test lookup
    let entry = store
        .lookup_entry(&parent_id, b"test.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.file_id, file_id);
    assert_eq!(entry.file_type, FileType::Regular);

    // Test rename
    store
        .rename_entry(&parent_id, b"test.txt", &parent_id, b"renamed.txt")
        .await
        .unwrap();

    // Old name should not exist
    assert!(
        store
            .lookup_entry(&parent_id, b"test.txt")
            .await
            .unwrap()
            .is_none()
    );

    // New name should exist
    let renamed_entry = store
        .lookup_entry(&parent_id, b"renamed.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renamed_entry.file_id, file_id);

    // Test deletion
    store.delete_file(&parent_id, b"renamed.txt").await.unwrap();
    assert!(
        store
            .lookup_entry(&parent_id, b"renamed.txt")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_directory_operations() {
    let master_key = MasterKey::generate();
    let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
    let store = LocalEncryptedMetadataStore::new(crypto, None);

    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    // Create directory
    let dir_id = FileId::new();
    let dir_metadata = FileMetadata {
        file_id: dir_id,
        parent_id: Some(parent_id),
        encrypted_name: b"testdir".to_vec(),
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

    store
        .create_directory(&parent_id, b"testdir", dir_metadata)
        .await
        .unwrap();

    // Add file to directory
    let file_in_dir = FileId::new();
    let file_metadata = FileMetadata {
        file_id: file_in_dir,
        parent_id: Some(DirectoryId(dir_id)),
        encrypted_name: b"file_in_dir.txt".to_vec(),
        size: 100,
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

    store
        .create_file(&DirectoryId(dir_id), b"file_in_dir.txt", file_metadata)
        .await
        .unwrap();

    // Try to delete non-empty directory (should fail)
    let delete_result = store.delete_directory(&parent_id, b"testdir").await;
    assert!(delete_result.is_err());

    // Delete file first
    store
        .delete_file(&DirectoryId(dir_id), b"file_in_dir.txt")
        .await
        .unwrap();

    // Now delete empty directory
    store
        .delete_directory(&parent_id, b"testdir")
        .await
        .unwrap();

    // Verify directory is gone
    assert!(
        store
            .lookup_entry(&parent_id, b"testdir")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_metadata_stats() {
    let master_key = MasterKey::generate();
    let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
    let store = LocalEncryptedMetadataStore::new(crypto, None);

    // Get initial stats
    let initial_stats = store.get_stats().await;
    assert_eq!(initial_stats.total_files, 1); // Just root
    assert_eq!(initial_stats.total_directories, 1); // Just root

    // Add files and directories
    let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

    for i in 0..5 {
        let file_id = FileId::new();
        let metadata = FileMetadata {
            file_id,
            parent_id: Some(parent_id),
            encrypted_name: format!("file{i}.txt").into_bytes(),
            size: 1024,
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
        store
            .create_file(&parent_id, &format!("file{i}.txt").into_bytes(), metadata)
            .await
            .unwrap();
    }

    // Check updated stats
    let updated_stats = store.get_stats().await;
    assert_eq!(updated_stats.total_files, 6); // Root + 5 files
    assert_eq!(updated_stats.total_size, 9216); // 5 files * 1024 bytes + root dir 4096 bytes
}
