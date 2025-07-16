//! Write-ahead logging for metadata operations
//!
//! This module provides durability for metadata changes through journaling.
//! All metadata operations are first written to the journal before being
//! applied to the in-memory store.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    DirectoryId, FileId, FileMetadata,
    error::{Result, VsockFuseError},
};

/// Journal entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalEntry {
    /// File metadata was created or updated
    FileMetadata { metadata: FileMetadata },
    /// Directory entry was added
    AddDirectoryEntry {
        dir_id: DirectoryId,
        name: Vec<u8>,
        file_id: FileId,
        file_type: crate::FileType,
    },
    /// Directory entry was removed
    RemoveDirectoryEntry { dir_id: DirectoryId, name: Vec<u8> },
    /// Directory was created
    CreateDirectory { dir_id: DirectoryId },
    /// File or directory was deleted
    DeleteFile { file_id: FileId },
    /// Entry was renamed
    RenameEntry {
        old_parent: DirectoryId,
        old_name: Vec<u8>,
        new_parent: DirectoryId,
        new_name: Vec<u8>,
        file_id: FileId,
    },
    /// Checkpoint marker - all entries before this have been applied
    Checkpoint {
        sequence: u64,
        timestamp: std::time::SystemTime,
    },
}

/// Journal writer for metadata operations
pub struct MetadataJournal {
    /// Path to journal file
    path: PathBuf,
    /// Active journal file writer
    writer: Arc<Mutex<Option<BufWriter<File>>>>,
    /// Sequence number for entries
    sequence: AtomicU64,
    /// Maximum journal size before rotation
    max_size: u64,
    /// Current journal size
    current_size: AtomicU64,
}

impl MetadataJournal {
    /// Create a new metadata journal
    pub fn new(base_path: &Path, max_size: u64) -> Result<Self> {
        let journal_path = base_path.join("metadata.journal");

        // Open or create journal file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .map_err(|e| VsockFuseError::Io {
                message: format!("Failed to open journal at {:?}: {}", journal_path, e),
                source: Some(e),
            })?;

        let current_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        let writer = BufWriter::new(file);

        Ok(Self {
            path: journal_path,
            writer: Arc::new(Mutex::new(Some(writer))),
            sequence: AtomicU64::new(0),
            max_size,
            current_size: AtomicU64::new(current_size),
        })
    }

    /// Write an entry to the journal
    pub fn write_entry(&self, entry: JournalEntry) -> Result<u64> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        // Encode entry with sequence number
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&sequence.to_le_bytes());

        let entry_bytes =
            bincode::serialize(&entry).map_err(|e| VsockFuseError::InvalidArgument {
                message: format!("Failed to encode journal entry: {}", e),
            })?;
        encoded.extend_from_slice(&(entry_bytes.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&entry_bytes);

        // Write to journal
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            writer.write_all(&encoded).map_err(|e| VsockFuseError::Io {
                message: format!("Journal I/O error at {:?}: {}", self.path, e),
                source: Some(e),
            })?;

            // Flush for durability
            writer.flush().map_err(|e| VsockFuseError::Io {
                message: format!("Journal I/O error at {:?}: {}", self.path, e),
                source: Some(e),
            })?;

            // Update size
            let entry_size = encoded.len() as u64;
            let new_size = self.current_size.fetch_add(entry_size, Ordering::SeqCst) + entry_size;

            // Check if rotation needed
            if new_size > self.max_size {
                self.rotate_journal(writer_guard.as_mut().unwrap())?;
            }
        }

        Ok(sequence)
    }

    /// Sync journal to disk
    pub fn sync(&self) -> Result<()> {
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            writer
                .get_ref()
                .sync_all()
                .map_err(|e| VsockFuseError::Io {
                    message: format!("Journal I/O error at {:?}: {}", self.path, e),
                    source: Some(e),
                })?;
        }
        Ok(())
    }

    /// Read all entries from the journal
    pub fn read_entries(&self) -> Result<Vec<(u64, JournalEntry)>> {
        // Close writer temporarily
        {
            let mut writer_guard = self.writer.lock();
            if let Some(writer) = writer_guard.take() {
                drop(writer);
            }
        }

        let mut entries = Vec::new();

        // Open for reading
        let file = File::open(&self.path).map_err(|e| VsockFuseError::Io {
            message: format!("Journal I/O error at {:?}: {}", self.path, e),
            source: Some(e),
        })?;
        let mut reader = BufReader::new(file);

        loop {
            // Read sequence number
            let mut seq_bytes = [0u8; 8];
            match reader.read_exact(&mut seq_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(VsockFuseError::Io {
                        message: format!("Journal I/O error at {:?}: {}", self.path, e),
                        source: Some(e),
                    });
                }
            }
            let sequence = u64::from_le_bytes(seq_bytes);

            // Read entry length
            let mut len_bytes = [0u8; 4];
            reader
                .read_exact(&mut len_bytes)
                .map_err(|e| VsockFuseError::Io {
                    message: format!("Journal I/O error at {:?}: {}", self.path, e),
                    source: Some(e),
                })?;
            let entry_len = u32::from_le_bytes(len_bytes) as usize;

            // Read entry
            let mut entry_bytes = vec![0u8; entry_len];
            reader
                .read_exact(&mut entry_bytes)
                .map_err(|e| VsockFuseError::Io {
                    message: format!("Journal I/O error at {:?}: {}", self.path, e),
                    source: Some(e),
                })?;

            // Decode entry
            let entry: JournalEntry = bincode::deserialize(&entry_bytes).map_err(|e| {
                VsockFuseError::InvalidArgument {
                    message: format!("Failed to decode journal entry: {}", e),
                }
            })?;

            entries.push((sequence, entry));

            // Update sequence counter
            self.sequence.store(sequence + 1, Ordering::SeqCst);
        }

        // Reopen for writing
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| VsockFuseError::Io {
                message: format!("Journal I/O error at {:?}: {}", self.path, e),
                source: Some(e),
            })?;

        let writer = BufWriter::new(file);
        *self.writer.lock() = Some(writer);

        Ok(entries)
    }

    /// Rotate the journal file
    fn rotate_journal(&self, writer: &mut BufWriter<File>) -> Result<()> {
        // Flush current writer
        writer.flush().map_err(|e| VsockFuseError::Io {
            message: format!("Journal I/O error at {:?}: {}", self.path, e),
            source: Some(e),
        })?;

        // Archive current journal
        let archive_path = self
            .path
            .with_extension(&format!("journal.{}", chrono::Utc::now().timestamp()));
        std::fs::rename(&self.path, &archive_path).map_err(|e| VsockFuseError::Io {
            message: format!("Journal I/O error at {:?}: {}", self.path, e),
            source: Some(e),
        })?;

        // Create new journal
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| VsockFuseError::Io {
                message: format!("Journal I/O error at {:?}: {}", self.path, e),
                source: Some(e),
            })?;

        *writer = BufWriter::new(new_file);
        self.current_size.store(0, Ordering::SeqCst);

        // Write checkpoint entry
        self.write_entry(JournalEntry::Checkpoint {
            sequence: self.sequence.load(Ordering::SeqCst),
            timestamp: std::time::SystemTime::now(),
        })?;

        Ok(())
    }

    /// Compact journal by removing obsolete entries
    pub fn compact(&self, applied_sequence: u64) -> Result<()> {
        // Read all entries
        let entries = self.read_entries()?;

        // Filter entries to keep
        let mut kept_entries = Vec::new();
        let mut seen_files = std::collections::HashSet::new();

        // Process in reverse order to keep only latest metadata
        for (seq, entry) in entries.into_iter().rev() {
            if seq > applied_sequence {
                // Keep all entries after checkpoint
                kept_entries.push((seq, entry));
            } else {
                // Keep only necessary entries for recovery
                match &entry {
                    JournalEntry::FileMetadata { metadata } => {
                        if seen_files.insert(metadata.file_id) {
                            kept_entries.push((seq, entry));
                        }
                    }
                    JournalEntry::Checkpoint { .. } => {
                        // Keep latest checkpoint
                        kept_entries.push((seq, entry));
                        break;
                    }
                    _ => {
                        // Directory operations are kept as-is
                        kept_entries.push((seq, entry));
                    }
                }
            }
        }

        // Reverse to maintain order
        kept_entries.reverse();

        // Close current writer
        *self.writer.lock() = None;

        // Write compacted journal
        let temp_path = self.path.with_extension("tmp");
        let temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| VsockFuseError::Io {
                message: format!("Journal I/O error at {:?}: {}", temp_path, e),
                source: Some(e),
            })?;

        let mut temp_writer = BufWriter::new(temp_file);

        for (_seq, entry) in kept_entries {
            let entry_bytes =
                bincode::serialize(&entry).map_err(|e| VsockFuseError::InvalidArgument {
                    message: format!("Failed to encode journal entry: {}", e),
                })?;
            let seq_bytes = self.sequence.fetch_add(1, Ordering::SeqCst).to_le_bytes();

            temp_writer
                .write_all(&seq_bytes)
                .map_err(|e| VsockFuseError::Io {
                    message: format!("Journal I/O error at {:?}: {}", temp_path, e),
                    source: Some(e),
                })?;
            temp_writer
                .write_all(&(entry_bytes.len() as u32).to_le_bytes())
                .map_err(|e| VsockFuseError::Io {
                    message: format!("Journal I/O error at {:?}: {}", temp_path, e),
                    source: Some(e),
                })?;
            temp_writer
                .write_all(&entry_bytes)
                .map_err(|e| VsockFuseError::Io {
                    message: format!("Journal I/O error at {:?}: {}", temp_path, e),
                    source: Some(e),
                })?;
        }

        temp_writer.flush().map_err(|e| VsockFuseError::Io {
            message: format!("Journal I/O error at {:?}: {}", temp_path, e),
            source: Some(e),
        })?;

        drop(temp_writer);

        // Atomic replace
        std::fs::rename(&temp_path, &self.path).map_err(|e| VsockFuseError::Io {
            message: format!("Journal I/O error at {:?}: {}", self.path, e),
            source: Some(e),
        })?;

        // Reopen for appending
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| VsockFuseError::Io {
                message: format!("Journal I/O error at {:?}: {}", self.path, e),
                source: Some(e),
            })?;

        let new_size = file.metadata().map(|m| m.len()).unwrap_or(0);

        *self.writer.lock() = Some(BufWriter::new(file));
        self.current_size.store(new_size, Ordering::SeqCst);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_journal_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let journal = MetadataJournal::new(temp_dir.path(), 1024 * 1024).unwrap();

        // Write some entries
        let metadata = FileMetadata {
            file_id: FileId::new(),
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

        let seq1 = journal
            .write_entry(JournalEntry::FileMetadata {
                metadata: metadata.clone(),
            })
            .unwrap();

        let seq2 = journal
            .write_entry(JournalEntry::AddDirectoryEntry {
                dir_id: DirectoryId(FileId::from_bytes([0; 32])),
                name: b"test.txt".to_vec(),
                file_id: metadata.file_id,
                file_type: crate::FileType::Regular,
            })
            .unwrap();

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);

        // Read entries back
        let entries = journal.read_entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 0);
        assert_eq!(entries[1].0, 1);

        // Verify entry contents
        match &entries[0].1 {
            JournalEntry::FileMetadata { metadata: m } => {
                assert_eq!(m.file_id, metadata.file_id);
                assert_eq!(m.size, 100);
            }
            _ => panic!("Unexpected entry type"),
        }
    }

    #[test]
    fn test_journal_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let journal = MetadataJournal::new(temp_dir.path(), 512).unwrap(); // Small size to trigger rotation

        // Write entries until rotation
        for i in 0..20 {
            let metadata = FileMetadata {
                file_id: FileId::new(),
                parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
                encrypted_name: format!("file{}.txt", i).into_bytes(),
                size: i * 100,
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

            journal
                .write_entry(JournalEntry::FileMetadata { metadata })
                .unwrap();
        }

        // Check that archive was created
        let files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();

        assert!(files.len() >= 2); // At least journal and one archive
    }
}
