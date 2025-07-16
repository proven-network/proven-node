//! Local metadata storage for the enclave
//!
//! This module provides a local storage backend for metadata that doesn't
//! require round trips to the host. Only data blobs go to the host.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    BlobId, DirectoryId, FileId, FileMetadata,
    error::{Result, VsockFuseError},
};

/// In-memory metadata storage that persists locally in the enclave
#[derive(Default)]
pub struct LocalMetadataStore {
    /// File metadata indexed by FileId
    files: Arc<RwLock<HashMap<FileId, FileMetadata>>>,
    /// Directory entries indexed by DirectoryId
    directories: Arc<RwLock<HashMap<DirectoryId, Vec<DirectoryEntry>>>>,
    /// Blob to FileId mapping for reverse lookups
    blob_index: Arc<RwLock<HashMap<BlobId, FileId>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: Vec<u8>,
    pub file_id: FileId,
    pub file_type: crate::FileType,
}

impl LocalMetadataStore {
    /// Create a new local metadata store
    pub fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            directories: Arc::new(RwLock::new(HashMap::new())),
            blob_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize with root directory
    pub fn init_root(&self) {
        let root_id = FileId::from_bytes([0; 32]);
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
            file_type: crate::FileType::Directory,
            nlink: 2,
            uid: 0,
            gid: 0,
        };

        self.files.write().insert(root_id, root_metadata);
        self.directories
            .write()
            .insert(DirectoryId(root_id), vec![]);
    }

    /// Get file metadata
    pub fn get_file_metadata(&self, file_id: &FileId) -> Option<FileMetadata> {
        self.files.read().get(file_id).cloned()
    }

    /// Store file metadata
    pub fn put_file_metadata(&self, metadata: FileMetadata) {
        // Update blob index for all blocks
        let mut blob_index = self.blob_index.write();
        for block in &metadata.blocks {
            blob_index.insert(block.blob_id, metadata.file_id);
        }
        drop(blob_index);

        self.files.write().insert(metadata.file_id, metadata);
    }

    /// Delete file metadata
    pub fn delete_file_metadata(&self, file_id: &FileId) -> bool {
        // Remove from blob index
        if let Some(metadata) = self.files.read().get(file_id) {
            let mut blob_index = self.blob_index.write();
            for block in &metadata.blocks {
                blob_index.remove(&block.blob_id);
            }
        }

        self.files.write().remove(file_id).is_some()
    }

    /// Get directory entries
    pub fn get_directory(&self, dir_id: &DirectoryId) -> Option<Vec<DirectoryEntry>> {
        self.directories.read().get(dir_id).cloned()
    }

    /// Store directory entries
    pub fn put_directory(&self, dir_id: DirectoryId, entries: Vec<DirectoryEntry>) {
        self.directories.write().insert(dir_id, entries);
    }

    /// Add entry to directory
    pub fn add_directory_entry(
        &self,
        dir_id: &DirectoryId,
        name: Vec<u8>,
        file_id: FileId,
        file_type: crate::FileType,
    ) -> Result<()> {
        let mut dirs = self.directories.write();
        let entries = dirs.entry(*dir_id).or_default();

        // Check if name already exists
        if entries.iter().any(|e| e.name == name) {
            return Err(VsockFuseError::AlreadyExists {
                path: std::path::PathBuf::from(String::from_utf8_lossy(&name).to_string()),
            });
        }

        entries.push(DirectoryEntry {
            name,
            file_id,
            file_type,
        });

        Ok(())
    }

    /// Remove entry from directory
    pub fn remove_directory_entry(
        &self,
        dir_id: &DirectoryId,
        name: &[u8],
    ) -> Option<DirectoryEntry> {
        let mut dirs = self.directories.write();
        if let Some(entries) = dirs.get_mut(dir_id)
            && let Some(pos) = entries.iter().position(|e| e.name == name)
        {
            return Some(entries.remove(pos));
        }
        None
    }

    /// Find entry in directory
    pub fn find_directory_entry(
        &self,
        dir_id: &DirectoryId,
        name: &[u8],
    ) -> Option<DirectoryEntry> {
        self.directories
            .read()
            .get(dir_id)?
            .iter()
            .find(|e| e.name == name)
            .cloned()
    }

    /// Get all metadata (for persistence/backup)
    pub fn export_all(&self) -> (Vec<FileMetadata>, HashMap<DirectoryId, Vec<DirectoryEntry>>) {
        let files = self.files.read().values().cloned().collect();
        let directories = self.directories.read().clone();
        (files, directories)
    }

    /// Import metadata (for recovery)
    pub fn import_all(
        &self,
        files: Vec<FileMetadata>,
        directories: HashMap<DirectoryId, Vec<DirectoryEntry>>,
    ) {
        let mut file_map = self.files.write();
        let mut blob_index = self.blob_index.write();

        for metadata in files {
            // Update blob index
            for block in &metadata.blocks {
                blob_index.insert(block.blob_id, metadata.file_id);
            }
            file_map.insert(metadata.file_id, metadata);
        }
        drop(file_map);
        drop(blob_index);

        *self.directories.write() = directories;
    }

    /// Check if a blob belongs to any file
    pub fn get_blob_owner(&self, blob_id: &BlobId) -> Option<FileId> {
        self.blob_index.read().get(blob_id).copied()
    }

    /// Get storage statistics
    pub fn get_stats(&self) -> LocalMetadataStats {
        let files = self.files.read();
        let total_files = files.len();
        let total_size: u64 = files.values().map(|m| m.size).sum();
        let total_blocks: usize = files.values().map(|m| m.blocks.len()).sum();

        LocalMetadataStats {
            total_files,
            total_directories: self.directories.read().len(),
            total_size,
            total_blocks,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalMetadataStats {
    pub total_files: usize,
    pub total_directories: usize,
    pub total_size: u64,
    pub total_blocks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_store_basic() {
        let store = LocalMetadataStore::new();
        store.init_root();

        // Check root exists
        let root_id = FileId::from_bytes([0; 32]);
        assert!(store.get_file_metadata(&root_id).is_some());
        assert!(store.get_directory(&DirectoryId(root_id)).is_some());

        // Add a file
        let file_id = FileId::new();
        let metadata = FileMetadata {
            file_id,
            parent_id: Some(DirectoryId(root_id)),
            encrypted_name: b"test.txt".to_vec(),
            size: 100,
            blocks: vec![],
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
            permissions: 0o644,
            file_type: crate::FileType::Regular,
            nlink: 1,
            uid: 1000,
            gid: 1000,
        };

        store.put_file_metadata(metadata.clone());
        let retrieved = store.get_file_metadata(&file_id).unwrap();
        assert_eq!(retrieved.file_id, metadata.file_id);
        assert_eq!(retrieved.size, metadata.size);

        // Add to directory
        assert!(
            store
                .add_directory_entry(
                    &DirectoryId(root_id),
                    b"test.txt".to_vec(),
                    file_id,
                    crate::FileType::Regular
                )
                .is_ok()
        );

        // Find in directory
        let entry = store.find_directory_entry(&DirectoryId(root_id), b"test.txt");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().file_id, file_id);
    }
}
