//! Encrypted metadata management for VSOCK-FUSE
//!
//! This module handles all file and directory metadata in encrypted form.

mod journal;
mod local_encrypted;
mod local_store;
mod snapshot;

pub use journal::{JournalEntry, MetadataJournal};
pub use local_encrypted::LocalEncryptedMetadataStore;
pub use local_store::{LocalMetadataStats, LocalMetadataStore};
pub use snapshot::{MetadataSnapshot, SnapshotManager};

use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    BlobId, BlobType, BlockLocation, DirectoryId, FileId, FileMetadata, FileType,
    encryption::EncryptionLayer,
    error::{Result, VsockFuseError},
};

/// In-memory cache for decrypted metadata
pub struct MetadataCache {
    /// File metadata cache
    files: LruCache<FileId, FileMetadata>,
    /// Directory contents cache
    directories: LruCache<DirectoryId, Vec<DirEntry>>,
    /// Maximum cache size in bytes
    max_size: usize,
    /// Current cache size in bytes
    current_size: usize,
}

impl MetadataCache {
    /// Create a new metadata cache
    pub fn new(max_size: usize) -> Self {
        // Estimate average metadata size
        const AVG_METADATA_SIZE: usize = 512;
        let capacity = max_size / AVG_METADATA_SIZE;

        Self {
            files: LruCache::new(capacity.try_into().unwrap()),
            directories: LruCache::new((capacity / 4).try_into().unwrap()),
            max_size,
            current_size: 0,
        }
    }

    /// Get file metadata from cache
    pub fn get_file(&mut self, file_id: &FileId) -> Option<&FileMetadata> {
        self.files.get(file_id)
    }

    /// Insert file metadata into cache
    pub fn insert_file(&mut self, file_id: FileId, metadata: FileMetadata) {
        let size = std::mem::size_of_val(&metadata)
            + metadata.blocks.len() * std::mem::size_of::<BlockLocation>();

        // Evict if necessary
        while self.current_size + size > self.max_size && !self.files.is_empty() {
            if let Some((_, evicted)) = self.files.pop_lru() {
                let evicted_size = std::mem::size_of_val(&evicted)
                    + evicted.blocks.len() * std::mem::size_of::<BlockLocation>();
                self.current_size = self.current_size.saturating_sub(evicted_size);
            }
        }

        self.files.put(file_id, metadata);
        self.current_size += size;
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.files.clear();
        self.directories.clear();
        self.current_size = 0;
    }
}

/// Directory entry information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    /// File name (encrypted)
    pub encrypted_name: Vec<u8>,
    /// File ID
    pub file_id: FileId,
    /// File type
    pub file_type: FileType,
    /// Inode number for FUSE
    pub inode: u64,
}

/// Directory entry for external use
#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    /// Decrypted name
    pub name: Vec<u8>,
    /// File identifier
    pub file_id: FileId,
    /// File type
    pub file_type: FileType,
}

/// Storage client interface for metadata operations
#[async_trait::async_trait]
pub trait MetadataStorage: Send + Sync {
    /// Store a blob
    async fn store_blob(&self, blob_id: BlobId, data: Vec<u8>) -> Result<()>;

    /// Retrieve a blob
    async fn get_blob(&self, blob_id: BlobId) -> Result<Vec<u8>>;

    /// Delete a blob
    async fn delete_blob(&self, blob_id: BlobId) -> Result<()>;

    /// List blobs with a prefix
    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<BlobId>>;
}

/// Encrypted metadata store
pub struct EncryptedMetadataStore {
    /// Decrypted metadata cache
    cache: Arc<RwLock<MetadataCache>>,
    /// Encryption layer
    crypto: Arc<EncryptionLayer>,
    /// Storage backend
    storage: Arc<dyn MetadataStorage>,
    /// Next inode number
    next_inode: Arc<RwLock<u64>>,
}

impl EncryptedMetadataStore {
    /// Create a new encrypted metadata store
    pub fn new(
        crypto: Arc<EncryptionLayer>,
        storage: Arc<dyn MetadataStorage>,
        cache_size: usize,
    ) -> Self {
        Self {
            cache: Arc::new(RwLock::new(MetadataCache::new(cache_size))),
            crypto,
            storage,
            next_inode: Arc::new(RwLock::new(2)), // Start at 2, 1 is reserved for root
        }
    }

    /// Ensure root directory exists
    pub async fn ensure_root_exists(&self) -> Result<()> {
        let root_id = FileId::from_bytes([0; 32]);
        let root_dir_id = DirectoryId(root_id);

        // Check if root metadata exists
        let blob_id = BlobId::from_file_id(&root_id, BlobType::Metadata);
        match self.storage.get_blob(blob_id).await {
            Ok(_) => {
                // Root already exists
                Ok(())
            }
            Err(_) => {
                // Create root directory
                let root_metadata = FileMetadata {
                    file_id: root_id,
                    parent_id: None,
                    encrypted_name: vec![],
                    size: 4096,
                    blocks: vec![],
                    created_at: std::time::SystemTime::now(),
                    modified_at: std::time::SystemTime::now(),
                    accessed_at: std::time::SystemTime::now(),
                    permissions: 0o755,
                    file_type: FileType::Directory,
                    nlink: 2,
                    uid: 0,
                    gid: 0,
                };

                // Save root metadata
                self.save_metadata(&root_metadata).await?;

                // Create empty directory entries for root
                self.save_directory(root_dir_id, vec![]).await?;

                Ok(())
            }
        }
    }

    /// Allocate a new inode number
    fn allocate_inode(&self) -> u64 {
        let mut next = self.next_inode.write();
        let inode = *next;
        *next += 1;
        inode
    }

    /// Save file metadata
    pub async fn save_metadata(&self, metadata: &FileMetadata) -> Result<()> {
        // Serialize metadata
        let plaintext = bincode::serialize(metadata)
            .map_err(|e| VsockFuseError::Internal(format!("Failed to serialize metadata: {e}")))?;

        // Encrypt metadata
        let encrypted = self.crypto.encrypt_metadata(&plaintext)?;

        // Store as blob
        let blob_id = BlobId::from_file_id(&metadata.file_id, BlobType::Metadata);
        self.storage.store_blob(blob_id, encrypted).await?;

        // Update cache
        self.cache
            .write()
            .insert_file(metadata.file_id, metadata.clone());

        Ok(())
    }

    /// Load file metadata
    pub async fn load_metadata(&self, file_id: &FileId) -> Result<FileMetadata> {
        // Check cache first
        if let Some(metadata) = self.cache.write().get_file(file_id) {
            return Ok(metadata.clone());
        }

        // Load from storage
        let blob_id = BlobId::from_file_id(file_id, BlobType::Metadata);
        let encrypted = self.storage.get_blob(blob_id).await?;

        // Decrypt
        let plaintext = self.crypto.decrypt_metadata(&encrypted)?;
        let metadata: FileMetadata = bincode::deserialize(&plaintext).map_err(|e| {
            VsockFuseError::Internal(format!("Failed to deserialize metadata: {e}"))
        })?;

        // Update cache
        self.cache.write().insert_file(*file_id, metadata.clone());

        Ok(metadata)
    }

    /// Delete file metadata
    pub async fn delete_metadata(&self, file_id: &FileId) -> Result<()> {
        let blob_id = BlobId::from_file_id(file_id, BlobType::Metadata);
        self.storage.delete_blob(blob_id).await?;

        // Remove from cache
        self.cache.write().files.pop(file_id);

        Ok(())
    }

    /// Add entry to directory
    async fn add_to_directory(
        &self,
        dir_id: DirectoryId,
        name: &str,
        file_id: FileId,
        file_type: FileType,
    ) -> Result<()> {
        // Load directory contents
        let mut entries = self.load_directory(dir_id).await?;

        // Encrypt name
        let encrypted_name = self.crypto.encrypt_filename(dir_id.as_file_id(), name)?;

        // Add new entry
        entries.push(DirEntry {
            encrypted_name,
            file_id,
            file_type,
            inode: self.allocate_inode(),
        });

        // Save updated directory
        self.save_directory(dir_id, entries).await?;

        Ok(())
    }

    /// Remove entry from directory
    async fn remove_from_directory(
        &self,
        dir_id: DirectoryId,
        name: &str,
    ) -> Result<Option<FileId>> {
        // Load directory contents
        let mut entries = self.load_directory(dir_id).await?;

        // Find and remove entry
        let name_hash = self.crypto.hash_filename(dir_id.as_file_id(), name);
        let mut removed_id = None;

        entries.retain(|entry| {
            if let Ok(entry_name) = self
                .crypto
                .decrypt_filename(dir_id.as_file_id(), &entry.encrypted_name)
            {
                let entry_hash = self.crypto.hash_filename(dir_id.as_file_id(), &entry_name);
                if entry_hash == name_hash {
                    removed_id = Some(entry.file_id);
                    return false;
                }
            }
            true
        });

        if removed_id.is_some() {
            // Save updated directory
            self.save_directory(dir_id, entries).await?;
        }

        Ok(removed_id)
    }

    /// Save directory contents
    async fn save_directory(&self, dir_id: DirectoryId, entries: Vec<DirEntry>) -> Result<()> {
        let plaintext = bincode::serialize(&entries)
            .map_err(|e| VsockFuseError::Internal(format!("Failed to serialize entries: {e}")))?;
        let encrypted = self.crypto.encrypt_metadata(&plaintext)?;

        let blob_id = BlobId::from_file_id(dir_id.as_file_id(), BlobType::Directory);
        self.storage.store_blob(blob_id, encrypted).await?;

        // Update cache
        self.cache.write().directories.put(dir_id, entries);

        Ok(())
    }

    /// Load directory contents
    pub async fn load_directory(&self, dir_id: DirectoryId) -> Result<Vec<DirEntry>> {
        // Check cache
        if let Some(entries) = self.cache.write().directories.get(&dir_id) {
            tracing::debug!(
                "Directory {:?} found in cache with {} entries",
                dir_id,
                entries.len()
            );
            return Ok(entries.clone());
        }

        tracing::debug!("Directory {:?} not in cache, loading from storage", dir_id);
        // Load from storage
        let blob_id = BlobId::from_file_id(dir_id.as_file_id(), BlobType::Directory);

        match self.storage.get_blob(blob_id).await {
            Ok(encrypted) => {
                let plaintext = self.crypto.decrypt_metadata(&encrypted)?;
                let entries: Vec<DirEntry> = bincode::deserialize(&plaintext).map_err(|e| {
                    VsockFuseError::Internal(format!("Failed to deserialize entries: {e}"))
                })?;

                // Update cache
                self.cache.write().directories.put(dir_id, entries.clone());

                Ok(entries)
            }
            Err(e) => {
                // Empty directory
                if matches!(
                    e,
                    crate::error::VsockFuseError::Storage(
                        crate::error::StorageError::BlobNotFound { .. }
                    )
                ) {
                    Ok(Vec::new())
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Lookup file by name in directory
    pub async fn lookup(&self, dir_id: DirectoryId, name: &str) -> Result<Option<FileMetadata>> {
        let entries = self.load_directory(dir_id).await?;
        let name_hash = self.crypto.hash_filename(dir_id.as_file_id(), name);

        for entry in entries {
            if let Ok(entry_name) = self
                .crypto
                .decrypt_filename(dir_id.as_file_id(), &entry.encrypted_name)
            {
                let entry_hash = self.crypto.hash_filename(dir_id.as_file_id(), &entry_name);
                if entry_hash == name_hash {
                    return Ok(Some(self.load_metadata(&entry.file_id).await?));
                }
            }
        }

        Ok(None)
    }

    /// Update file metadata
    pub async fn update_metadata<F>(&self, file_id: &FileId, updater: F) -> Result<FileMetadata>
    where
        F: FnOnce(&mut FileMetadata),
    {
        let mut metadata = self.load_metadata(file_id).await?;
        updater(&mut metadata);
        self.save_metadata(&metadata).await?;
        Ok(metadata)
    }

    /// Clear all caches
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Get metadata by file ID
    pub async fn get_metadata(&self, file_id: &FileId) -> Result<Option<FileMetadata>> {
        match self.load_metadata(file_id).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(e) => {
                if matches!(
                    e,
                    crate::error::VsockFuseError::Storage(
                        crate::error::StorageError::BlobNotFound { .. }
                    )
                ) {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Update metadata directly
    pub async fn update_metadata_direct(
        &self,
        _file_id: &FileId,
        metadata: FileMetadata,
    ) -> Result<()> {
        self.save_metadata(&metadata).await
    }

    /// Lookup entry in directory
    pub async fn lookup_entry(
        &self,
        parent_id: &DirectoryId,
        name: &[u8],
    ) -> Result<Option<DirectoryEntry>> {
        let entries = self.load_directory(*parent_id).await?;
        let name_str = std::str::from_utf8(name).map_err(|_| {
            crate::error::VsockFuseError::InvalidArgument {
                message: "Invalid UTF-8 in filename".to_string(),
            }
        })?;
        let name_hash = self.crypto.hash_filename(parent_id.as_file_id(), name_str);

        for entry in entries {
            if let Ok(entry_name) = self
                .crypto
                .decrypt_filename(parent_id.as_file_id(), &entry.encrypted_name)
            {
                let entry_hash = self
                    .crypto
                    .hash_filename(parent_id.as_file_id(), &entry_name);
                if entry_hash == name_hash {
                    return Ok(Some(DirectoryEntry {
                        name: entry_name.as_bytes().to_vec(),
                        file_id: entry.file_id,
                        file_type: entry.file_type,
                    }));
                }
            }
        }

        Ok(None)
    }

    /// List directory contents
    pub async fn list_directory(&self, dir_id: &DirectoryId) -> Result<Vec<DirectoryEntry>> {
        let entries = self.load_directory(*dir_id).await?;
        let mut result = Vec::new();

        for entry in entries {
            if let Ok(name) = self
                .crypto
                .decrypt_filename(dir_id.as_file_id(), &entry.encrypted_name)
            {
                result.push(DirectoryEntry {
                    name: name.as_bytes().to_vec(),
                    file_id: entry.file_id,
                    file_type: entry.file_type,
                });
            }
        }

        Ok(result)
    }

    /// Create a file
    pub async fn create_file(
        &self,
        parent_id: &DirectoryId,
        name: &[u8],
        metadata: FileMetadata,
    ) -> Result<()> {
        // Save file metadata
        self.save_metadata(&metadata).await?;

        // Add to parent directory
        let name_str = std::str::from_utf8(name).map_err(|_| {
            crate::error::VsockFuseError::InvalidArgument {
                message: "Invalid UTF-8 in filename".to_string(),
            }
        })?;
        self.add_to_directory(*parent_id, name_str, metadata.file_id, metadata.file_type)
            .await
    }

    /// Delete a file
    pub async fn delete_file(&self, parent_id: &DirectoryId, name: &[u8]) -> Result<()> {
        let name_str = std::str::from_utf8(name).map_err(|_| {
            crate::error::VsockFuseError::InvalidArgument {
                message: "Invalid UTF-8 in filename".to_string(),
            }
        })?;

        // Remove from directory
        if let Some(file_id) = self.remove_from_directory(*parent_id, name_str).await? {
            // Delete metadata
            self.delete_metadata(&file_id).await?;
        }

        Ok(())
    }

    /// Create a directory
    pub async fn create_directory(
        &self,
        parent_id: &DirectoryId,
        name: &[u8],
        metadata: FileMetadata,
    ) -> Result<()> {
        // Initialize empty directory
        let dir_id = DirectoryId(metadata.file_id);
        self.save_directory(dir_id, Vec::new()).await?;

        // Save directory metadata
        self.save_metadata(&metadata).await?;

        // Add to parent
        let name_str = std::str::from_utf8(name).map_err(|_| {
            crate::error::VsockFuseError::InvalidArgument {
                message: "Invalid UTF-8 in filename".to_string(),
            }
        })?;
        self.add_to_directory(*parent_id, name_str, metadata.file_id, metadata.file_type)
            .await
    }

    /// Delete a directory
    pub async fn delete_directory(&self, parent_id: &DirectoryId, name: &[u8]) -> Result<()> {
        let name_str = std::str::from_utf8(name).map_err(|_| {
            crate::error::VsockFuseError::InvalidArgument {
                message: "Invalid UTF-8 in filename".to_string(),
            }
        })?;

        // Find directory to delete
        if let Some(dir_file_id) = self.remove_from_directory(*parent_id, name_str).await? {
            let dir_id = DirectoryId(dir_file_id);

            // Check if empty
            let entries = self.load_directory(dir_id).await?;
            if !entries.is_empty() {
                return Err(crate::error::VsockFuseError::NotEmpty {
                    path: std::path::PathBuf::from(name_str),
                });
            }

            // Delete directory blob
            let blob_id = BlobId::from_file_id(&dir_file_id, BlobType::Directory);
            self.storage.delete_blob(blob_id).await?;

            // Delete metadata
            self.delete_metadata(&dir_file_id).await?;
        }

        Ok(())
    }

    /// Rename an entry
    pub async fn rename_entry(
        &self,
        old_parent: &DirectoryId,
        old_name: &[u8],
        new_parent: &DirectoryId,
        new_name: &[u8],
    ) -> Result<()> {
        let old_name_str = std::str::from_utf8(old_name).map_err(|_| {
            crate::error::VsockFuseError::InvalidArgument {
                message: "Invalid UTF-8 in old filename".to_string(),
            }
        })?;
        let new_name_str = std::str::from_utf8(new_name).map_err(|_| {
            crate::error::VsockFuseError::InvalidArgument {
                message: "Invalid UTF-8 in new filename".to_string(),
            }
        })?;

        // Find and remove from old location
        if let Some(file_id) = self
            .remove_from_directory(*old_parent, old_name_str)
            .await?
        {
            // Get metadata to determine file type
            let metadata = self.load_metadata(&file_id).await?;

            // Add to new location
            self.add_to_directory(*new_parent, new_name_str, file_id, metadata.file_type)
                .await?;

            // Update parent ID if moved to different directory
            if old_parent != new_parent {
                self.update_metadata(&file_id, |m| {
                    m.parent_id = Some(*new_parent);
                })
                .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::MasterKey;
    use std::collections::HashMap;
    use std::time::SystemTime;

    struct MockStorage {
        data: Arc<RwLock<HashMap<BlobId, Vec<u8>>>>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl MetadataStorage for MockStorage {
        async fn store_blob(&self, blob_id: BlobId, data: Vec<u8>) -> Result<()> {
            self.data.write().insert(blob_id, data);
            Ok(())
        }

        async fn get_blob(&self, blob_id: BlobId) -> Result<Vec<u8>> {
            self.data
                .read()
                .get(&blob_id)
                .cloned()
                .ok_or_else(|| crate::error::StorageError::BlobNotFound { blob_id }.into())
        }

        async fn delete_blob(&self, blob_id: BlobId) -> Result<()> {
            self.data.write().remove(&blob_id);
            Ok(())
        }

        async fn list_blobs(&self, _prefix: Option<&[u8]>) -> Result<Vec<BlobId>> {
            Ok(self.data.read().keys().cloned().collect())
        }
    }

    #[tokio::test]
    async fn test_metadata_create_and_load() {
        let master_key = MasterKey::generate();
        let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
        let storage = Arc::new(MockStorage::new());
        let store = EncryptedMetadataStore::new(crypto, storage, 1024 * 1024);

        let root_dir = DirectoryId::new();
        let file_id = FileId::new();
        let metadata = FileMetadata {
            file_id,
            parent_id: Some(root_dir),
            encrypted_name: vec![],
            size: 0,
            blocks: Vec::new(),
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            permissions: 0o644,
            file_type: FileType::Regular,
            nlink: 1,
            uid: 1000,
            gid: 1000,
        };
        store
            .create_file(&root_dir, b"test.txt", metadata.clone())
            .await
            .unwrap();

        // Load it back
        let loaded = store.load_metadata(&metadata.file_id).await.unwrap();
        assert_eq!(loaded.file_id, metadata.file_id);
        assert_eq!(loaded.size, 0);
        assert_eq!(loaded.file_type, FileType::Regular);
    }

    #[tokio::test]
    async fn test_directory_operations() {
        let master_key = MasterKey::generate();
        let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
        let storage = Arc::new(MockStorage::new());
        let store = EncryptedMetadataStore::new(crypto, storage, 1024 * 1024);

        let root_dir = DirectoryId::new();

        // Create files
        let file1_id = FileId::new();
        let file1 = FileMetadata {
            file_id: file1_id,
            parent_id: Some(root_dir),
            encrypted_name: vec![],
            size: 0,
            blocks: Vec::new(),
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            permissions: 0o644,
            file_type: FileType::Regular,
            nlink: 1,
            uid: 1000,
            gid: 1000,
        };
        store
            .create_file(&root_dir, b"file1.txt", file1.clone())
            .await
            .unwrap();

        let file2_id = FileId::new();
        let file2 = FileMetadata {
            file_id: file2_id,
            parent_id: Some(root_dir),
            encrypted_name: vec![],
            size: 0,
            blocks: Vec::new(),
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            permissions: 0o644,
            file_type: FileType::Regular,
            nlink: 1,
            uid: 1000,
            gid: 1000,
        };
        store
            .create_file(&root_dir, b"file2.txt", file2.clone())
            .await
            .unwrap();

        // List directory
        let entries = store.load_directory(root_dir).await.unwrap();
        assert_eq!(entries.len(), 2);

        // Lookup file
        let found = store.lookup(root_dir, "file1.txt").await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().file_id, file1.file_id);

        // Remove file
        let removed = store
            .remove_from_directory(root_dir, "file1.txt")
            .await
            .unwrap();
        assert_eq!(removed, Some(file1.file_id));

        // Verify removal
        let entries = store.load_directory(root_dir).await.unwrap();
        assert_eq!(entries.len(), 1);
    }
}
