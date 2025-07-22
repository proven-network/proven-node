//! Local metadata storage for the enclave
//!
//! This module provides a local storage backend for metadata that doesn't
//! require round trips to the host. Only data blobs go to the host.

use parking_lot::RwLock;
use proven_logger::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::{
    BlobId, DirectoryId, FileId, FileMetadata,
    error::{Result, VsockFuseError},
    metadata::{JournalEntry, MetadataJournal, SnapshotManager},
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
    /// Journal for durability (optional)
    journal: Option<Arc<MetadataJournal>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: Vec<u8>,
    pub file_id: FileId,
    pub file_type: crate::FileType,
}

impl LocalMetadataStore {
    /// Create a new local metadata store without journaling
    pub fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            directories: Arc::new(RwLock::new(HashMap::new())),
            blob_index: Arc::new(RwLock::new(HashMap::new())),
            journal: None,
        }
    }

    /// Create a new local metadata store with journaling
    pub fn with_journal(journal_path: &Path) -> Result<Self> {
        let journal = MetadataJournal::new(journal_path, 100 * 1024 * 1024)?; // 100MB max
        let journal = Arc::new(journal);

        let mut store = Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            directories: Arc::new(RwLock::new(HashMap::new())),
            blob_index: Arc::new(RwLock::new(HashMap::new())),
            journal: Some(journal.clone()),
        };

        // Try to recover from snapshot first, then apply journal
        let temp_store = Self::new(); // Create temp store for snapshot manager
        let snapshot_manager = SnapshotManager::new(journal_path, Arc::new(temp_store));

        if let Ok(Some(snapshot_path)) = snapshot_manager.find_latest_snapshot() {
            if let Ok(snapshot) = snapshot_manager.load_snapshot(&snapshot_path) {
                info!("Recovering from snapshot at {snapshot_path:?}");
                let snapshot_seq = snapshot.sequence;
                store.import_snapshot(snapshot)?;
                // Apply journal entries after the snapshot
                store.recover_from_journal_after_sequence(snapshot_seq)?;
            } else {
                // Full journal recovery if snapshot load fails
                store.recover_from_journal()?;
            }
        } else {
            // No snapshot, full journal recovery
            store.recover_from_journal()?;
        }

        Ok(store)
    }

    /// Recover metadata from journal
    fn recover_from_journal(&mut self) -> Result<()> {
        if let Some(journal) = &self.journal {
            let entries = journal.read_entries()?;

            for (_seq, entry) in entries {
                match entry {
                    JournalEntry::FileMetadata { metadata } => {
                        self.files.write().insert(metadata.file_id, metadata);
                    }
                    JournalEntry::AddDirectoryEntry {
                        dir_id,
                        name,
                        file_id,
                        file_type,
                    } => {
                        let mut dirs = self.directories.write();
                        let entries = dirs.entry(dir_id).or_default();
                        entries.push(DirectoryEntry {
                            name,
                            file_id,
                            file_type,
                        });
                    }
                    JournalEntry::RemoveDirectoryEntry { dir_id, name } => {
                        let mut dirs = self.directories.write();
                        if let Some(entries) = dirs.get_mut(&dir_id) {
                            entries.retain(|e| e.name != name);
                        }
                    }
                    JournalEntry::CreateDirectory { dir_id } => {
                        self.directories.write().insert(dir_id, vec![]);
                    }
                    JournalEntry::DeleteFile { file_id } => {
                        self.files.write().remove(&file_id);
                    }
                    JournalEntry::RenameEntry {
                        old_parent,
                        old_name,
                        new_parent,
                        new_name,
                        file_id: _,
                    } => {
                        let mut dirs = self.directories.write();

                        // Remove from old parent
                        if let Some(entries) = dirs.get_mut(&old_parent)
                            && let Some(pos) = entries.iter().position(|e| e.name == old_name)
                        {
                            let entry = entries.remove(pos);

                            // Add to new parent
                            if let Some(new_entries) = dirs.get_mut(&new_parent) {
                                new_entries.push(DirectoryEntry {
                                    name: new_name,
                                    file_id: entry.file_id,
                                    file_type: entry.file_type,
                                });
                            }
                        }
                    }
                    JournalEntry::Checkpoint { .. } => {
                        // Checkpoint marks a consistent state
                    }
                }
            }
        }

        Ok(())
    }

    /// Recover metadata from journal entries after a specific sequence number
    fn recover_from_journal_after_sequence(&mut self, after_sequence: u64) -> Result<()> {
        if let Some(journal) = &self.journal {
            let entries = journal.read_entries()?;

            for (seq, entry) in entries {
                // Skip entries already in the snapshot
                if seq <= after_sequence {
                    continue;
                }

                match entry {
                    JournalEntry::FileMetadata { metadata } => {
                        self.files.write().insert(metadata.file_id, metadata);
                    }
                    JournalEntry::AddDirectoryEntry {
                        dir_id,
                        name,
                        file_id,
                        file_type,
                    } => {
                        let mut dirs = self.directories.write();
                        let entries = dirs.entry(dir_id).or_default();
                        entries.push(DirectoryEntry {
                            name,
                            file_id,
                            file_type,
                        });
                    }
                    JournalEntry::RemoveDirectoryEntry { dir_id, name } => {
                        let mut dirs = self.directories.write();
                        if let Some(entries) = dirs.get_mut(&dir_id) {
                            entries.retain(|e| e.name != name);
                        }
                    }
                    JournalEntry::CreateDirectory { dir_id } => {
                        self.directories.write().entry(dir_id).or_default();
                    }
                    JournalEntry::DeleteFile { file_id } => {
                        self.files.write().remove(&file_id);
                    }
                    JournalEntry::RenameEntry {
                        old_parent,
                        old_name,
                        new_parent,
                        new_name,
                        file_id: _,
                    } => {
                        let mut dirs = self.directories.write();

                        // Remove from old parent
                        if let Some(entries) = dirs.get_mut(&old_parent)
                            && let Some(pos) = entries.iter().position(|e| e.name == old_name)
                        {
                            let entry = entries.remove(pos);

                            // Add to new parent
                            if let Some(new_entries) = dirs.get_mut(&new_parent) {
                                new_entries.push(DirectoryEntry {
                                    name: new_name,
                                    file_id: entry.file_id,
                                    file_type: entry.file_type,
                                });
                            }
                        }
                    }
                    JournalEntry::Checkpoint { .. } => {
                        // Checkpoint marks a consistent state
                    }
                }
            }
        }

        Ok(())
    }

    /// Initialize with root directory
    pub fn init_root(&self) {
        let root_id = FileId::from_bytes([0; 32]);

        // Check if root already exists (from journal recovery)
        if self.files.read().contains_key(&root_id) {
            return;
        }

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

        // Write to journal first if available
        if let Some(journal) = &self.journal {
            if let Err(e) = journal.write_entry(JournalEntry::CreateDirectory {
                dir_id: DirectoryId(root_id),
            }) {
                error!("Failed to write root directory creation to journal: {e}");
            }
            if let Err(e) = journal.write_entry(JournalEntry::FileMetadata {
                metadata: root_metadata.clone(),
            }) {
                error!("Failed to write root metadata to journal: {e}");
            }
        }

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
        // Write to journal first
        if let Some(journal) = &self.journal
            && let Err(e) = journal.write_entry(JournalEntry::FileMetadata {
                metadata: metadata.clone(),
            })
        {
            error!("Failed to write metadata to journal: {e}");
        }

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
        // Write to journal first
        if let Some(journal) = &self.journal
            && let Err(e) = journal.write_entry(JournalEntry::DeleteFile { file_id: *file_id })
        {
            error!("Failed to write delete to journal: {e}");
        }

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
        // Write to journal first
        if let Some(journal) = &self.journal
            && let Err(e) = journal.write_entry(JournalEntry::CreateDirectory { dir_id })
        {
            error!("Failed to write directory creation to journal: {e}");
        }

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

        // Write to journal first
        if let Some(journal) = &self.journal
            && let Err(e) = journal.write_entry(JournalEntry::AddDirectoryEntry {
                dir_id: *dir_id,
                name: name.clone(),
                file_id,
                file_type,
            })
        {
            error!("Failed to write directory entry to journal: {e}");
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
            // Write to journal first
            if let Some(journal) = &self.journal
                && let Err(e) = journal.write_entry(JournalEntry::RemoveDirectoryEntry {
                    dir_id: *dir_id,
                    name: name.to_vec(),
                })
            {
                error!("Failed to write directory removal to journal: {e}");
            }

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

    /// Sync journal to disk
    pub fn sync_journal(&self) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.sync()?;
        }
        Ok(())
    }

    /// Compact journal by removing obsolete entries
    pub fn compact_journal(&self, checkpoint_seq: u64) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.compact(checkpoint_seq)?;
        }
        Ok(())
    }

    /// Get current journal sequence number
    pub fn current_journal_sequence(&self) -> Option<u64> {
        self.journal.as_ref().map(|j| j.current_sequence())
    }

    /// Export current state as a snapshot
    pub fn export_snapshot(
        &self,
        sequence: u64,
        timestamp: std::time::SystemTime,
    ) -> Result<super::MetadataSnapshot> {
        let files = self.files.read().clone();
        let directories = self.directories.read().clone();
        let blob_index = self.blob_index.read().clone();

        Ok(super::MetadataSnapshot {
            version: super::snapshot::SNAPSHOT_VERSION,
            timestamp,
            sequence,
            files,
            directories,
            blob_index,
        })
    }

    /// Import state from a snapshot
    pub fn import_snapshot(&self, snapshot: super::MetadataSnapshot) -> Result<()> {
        // Clear existing data
        self.files.write().clear();
        self.directories.write().clear();
        self.blob_index.write().clear();

        // Import snapshot data
        *self.files.write() = snapshot.files;
        *self.directories.write() = snapshot.directories;
        *self.blob_index.write() = snapshot.blob_index;

        Ok(())
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
