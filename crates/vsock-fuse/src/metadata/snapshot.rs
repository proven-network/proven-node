//! Metadata snapshot functionality
//!
//! This module provides snapshot capabilities for metadata persistence,
//! creating point-in-time backups of the entire metadata state.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    DirectoryId, FileId, FileMetadata,
    error::{Result, VsockFuseError},
    metadata::LocalMetadataStore,
};

// Use the DirectoryEntry from local_store which has Serialize/Deserialize
use super::local_store::DirectoryEntry;

/// Metadata snapshot format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    /// Snapshot version for compatibility
    pub version: u32,
    /// Timestamp when snapshot was created
    pub timestamp: std::time::SystemTime,
    /// Sequence number from journal
    pub sequence: u64,
    /// All file metadata
    pub files: HashMap<FileId, FileMetadata>,
    /// All directory entries
    pub directories: HashMap<DirectoryId, Vec<DirectoryEntry>>,
    /// Blob index mapping
    pub blob_index: HashMap<crate::BlobId, FileId>,
}

/// Current snapshot format version
pub const SNAPSHOT_VERSION: u32 = 1;

/// Snapshot manager for creating and loading snapshots
pub struct SnapshotManager {
    /// Base path for snapshots
    base_path: PathBuf,
    /// Reference to metadata store
    metadata_store: Arc<LocalMetadataStore>,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new(base_path: &Path, metadata_store: Arc<LocalMetadataStore>) -> Self {
        Self {
            base_path: base_path.to_path_buf(),
            metadata_store,
        }
    }

    /// Create a new snapshot
    pub fn create_snapshot(&self, sequence: u64) -> Result<PathBuf> {
        // Generate snapshot filename
        let timestamp = std::time::SystemTime::now();
        let filename = format!(
            "snapshot-{}-{}.bin",
            chrono::Utc::now().timestamp(),
            sequence
        );
        let snapshot_path = self.base_path.join(&filename);

        // Get current metadata state
        let snapshot = self.metadata_store.export_snapshot(sequence, timestamp)?;

        // Write snapshot to temp file first
        let temp_path = snapshot_path.with_extension("tmp");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| VsockFuseError::Io {
                message: format!("Failed to create snapshot file: {}", e),
                source: Some(e),
            })?;

        let mut writer = BufWriter::new(file);

        // Write magic header
        writer
            .write_all(b"VSFSNAP\0")
            .map_err(|e| VsockFuseError::Io {
                message: format!("Failed to write snapshot header: {}", e),
                source: Some(e),
            })?;

        // Serialize snapshot
        bincode::serialize_into(&mut writer, &snapshot).map_err(|e| {
            VsockFuseError::InvalidArgument {
                message: format!("Failed to serialize snapshot: {}", e),
            }
        })?;

        writer.flush().map_err(|e| VsockFuseError::Io {
            message: format!("Failed to flush snapshot: {}", e),
            source: Some(e),
        })?;

        drop(writer);

        // Atomic rename
        std::fs::rename(&temp_path, &snapshot_path).map_err(|e| VsockFuseError::Io {
            message: format!("Failed to finalize snapshot: {}", e),
            source: Some(e),
        })?;

        Ok(snapshot_path)
    }

    /// Load a snapshot from file
    pub fn load_snapshot(&self, path: &Path) -> Result<MetadataSnapshot> {
        let file = File::open(path).map_err(|e| VsockFuseError::Io {
            message: format!("Failed to open snapshot file: {}", e),
            source: Some(e),
        })?;

        let mut reader = BufReader::new(file);

        // Check magic header
        let mut header = [0u8; 8];
        use std::io::Read;
        reader
            .read_exact(&mut header)
            .map_err(|e| VsockFuseError::Io {
                message: format!("Failed to read snapshot header: {}", e),
                source: Some(e),
            })?;

        if &header != b"VSFSNAP\0" {
            return Err(VsockFuseError::InvalidArgument {
                message: "Invalid snapshot file format".to_string(),
            });
        }

        // Deserialize snapshot
        let snapshot: MetadataSnapshot =
            bincode::deserialize_from(reader).map_err(|e| VsockFuseError::InvalidArgument {
                message: format!("Failed to deserialize snapshot: {}", e),
            })?;

        // Check version compatibility
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(VsockFuseError::InvalidArgument {
                message: format!(
                    "Incompatible snapshot version: {} (expected {})",
                    snapshot.version, SNAPSHOT_VERSION
                ),
            });
        }

        Ok(snapshot)
    }

    /// Find the latest snapshot
    pub fn find_latest_snapshot(&self) -> Result<Option<PathBuf>> {
        let mut latest: Option<(PathBuf, std::time::SystemTime)> = None;

        let entries = std::fs::read_dir(&self.base_path).map_err(|e| VsockFuseError::Io {
            message: format!("Failed to read snapshot directory: {}", e),
            source: Some(e),
        })?;

        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    if name.starts_with("snapshot-") {
                        if let Ok(metadata) = entry.metadata() {
                            if let Ok(modified) = metadata.modified() {
                                match &latest {
                                    None => latest = Some((path, modified)),
                                    Some((_, last_time)) if modified > *last_time => {
                                        latest = Some((path, modified))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(latest.map(|(path, _)| path))
    }

    /// Clean up old snapshots, keeping the most recent N
    pub fn cleanup_old_snapshots(&self, keep_count: usize) -> Result<()> {
        let mut snapshots = Vec::new();

        let entries = std::fs::read_dir(&self.base_path).map_err(|e| VsockFuseError::Io {
            message: format!("Failed to read snapshot directory: {}", e),
            source: Some(e),
        })?;

        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    if name.starts_with("snapshot-") {
                        if let Ok(metadata) = entry.metadata() {
                            if let Ok(modified) = metadata.modified() {
                                snapshots.push((path, modified));
                            }
                        }
                    }
                }
            }
        }

        // Sort by modification time, newest first
        snapshots.sort_by(|a, b| b.1.cmp(&a.1));

        // Remove old snapshots
        for (path, _) in snapshots.into_iter().skip(keep_count) {
            std::fs::remove_file(&path).map_err(|e| VsockFuseError::Io {
                message: format!("Failed to remove old snapshot: {}", e),
                source: Some(e),
            })?;
        }

        Ok(())
    }

    /// Restore metadata from a snapshot
    pub fn restore_snapshot(&self, snapshot: MetadataSnapshot) -> Result<()> {
        self.metadata_store.import_snapshot(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_create_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_store = Arc::new(LocalMetadataStore::new());

        // Add some test data
        let file_id = FileId::new();
        let metadata = FileMetadata {
            file_id,
            parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
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

        metadata_store.put_file_metadata(metadata.clone());

        // Create snapshot
        let manager = SnapshotManager::new(temp_dir.path(), metadata_store.clone());
        let snapshot_path = manager.create_snapshot(42).unwrap();

        // Load snapshot
        let loaded = manager.load_snapshot(&snapshot_path).unwrap();
        assert_eq!(loaded.version, SNAPSHOT_VERSION);
        assert_eq!(loaded.sequence, 42);
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files.get(&file_id).unwrap().size, 100);
    }

    #[test]
    fn test_snapshot_restore() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_store = Arc::new(LocalMetadataStore::new());

        // Add initial data
        let file_id = FileId::new();
        let metadata = FileMetadata {
            file_id,
            parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
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

        metadata_store.put_file_metadata(metadata.clone());

        // Create snapshot
        let manager = SnapshotManager::new(temp_dir.path(), metadata_store.clone());
        let snapshot_path = manager.create_snapshot(1).unwrap();

        // Modify data
        metadata_store.delete_file_metadata(&file_id);
        assert!(metadata_store.get_file_metadata(&file_id).is_none());

        // Restore from snapshot
        let snapshot = manager.load_snapshot(&snapshot_path).unwrap();
        manager.restore_snapshot(snapshot).unwrap();

        // Verify restoration
        let restored = metadata_store.get_file_metadata(&file_id).unwrap();
        assert_eq!(restored.size, 100);
    }

    #[test]
    fn test_cleanup_old_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_store = Arc::new(LocalMetadataStore::new());
        let manager = SnapshotManager::new(temp_dir.path(), metadata_store);

        // Create multiple snapshots
        for i in 0..5 {
            manager.create_snapshot(i).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Verify 5 snapshots exist
        let count_before = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("bin"))
            .count();
        assert_eq!(count_before, 5);

        // Clean up, keeping only 2
        manager.cleanup_old_snapshots(2).unwrap();

        // Verify only 2 remain
        let count_after = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("bin"))
            .count();
        assert_eq!(count_after, 2);
    }
}
