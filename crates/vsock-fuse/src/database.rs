//! Database-specific optimizations for VSOCK-FUSE
//!
//! This module provides specialized handling for database files to ensure
//! ACID properties and optimal performance.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::{FileId, error::Result};

/// File lock types for POSIX compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// Shared/read lock (F_RDLCK)
    Read,
    /// Exclusive/write lock (F_WRLCK)
    Write,
    /// Unlock (F_UNLCK)
    Unlock,
}

/// File lock information
#[derive(Debug, Clone)]
pub struct FileLock {
    /// Type of lock
    pub lock_type: LockType,
    /// Process ID holding the lock
    pub pid: u32,
    /// Start offset of the lock
    pub start: u64,
    /// Length of the lock (0 = to EOF)
    pub len: u64,
}

/// Lock manager for file locking support
#[derive(Debug, Default)]
pub struct LockManager {
    /// Active locks by file ID
    locks: Arc<RwLock<HashMap<FileId, Vec<FileLock>>>>,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Test if a lock can be acquired (F_GETLK)
    pub fn test_lock(&self, file_id: &FileId, lock: &FileLock) -> Option<FileLock> {
        let locks = self.locks.read();

        if let Some(file_locks) = locks.get(file_id) {
            for existing in file_locks {
                if self.locks_conflict(existing, lock) {
                    return Some(existing.clone());
                }
            }
        }

        None
    }

    /// Set a lock (F_SETLK)
    pub fn set_lock(&self, file_id: &FileId, lock: FileLock) -> Result<()> {
        let mut locks = self.locks.write();

        if lock.lock_type == LockType::Unlock {
            // Remove the lock
            if let Some(file_locks) = locks.get_mut(file_id) {
                file_locks.retain(|l| l.pid != lock.pid || !self.ranges_overlap(l, &lock));
            }
        } else {
            // Check for conflicts
            if let Some(file_locks) = locks.get(file_id) {
                for existing in file_locks {
                    if existing.pid != lock.pid && self.locks_conflict(existing, &lock) {
                        return Err(crate::error::VsockFuseError::Locked {
                            path: std::path::PathBuf::new(),
                            holder: format!("PID {}", existing.pid),
                        });
                    }
                }
            }

            // Add the lock
            locks.entry(*file_id).or_default().push(lock);
        }

        Ok(())
    }

    /// Set a lock with wait (F_SETLKW) - for now, just tries once
    pub async fn set_lock_wait(&self, file_id: &FileId, lock: FileLock) -> Result<()> {
        // TODO: Implement proper waiting/blocking
        self.set_lock(file_id, lock)
    }

    /// Remove all locks for a file
    pub fn remove_file_locks(&self, file_id: &FileId) {
        self.locks.write().remove(file_id);
    }

    /// Remove all locks for a process
    pub fn remove_process_locks(&self, pid: u32) {
        let mut locks = self.locks.write();

        for file_locks in locks.values_mut() {
            file_locks.retain(|l| l.pid != pid);
        }

        // Remove empty entries
        locks.retain(|_, v| !v.is_empty());
    }

    /// Check if two locks conflict
    fn locks_conflict(&self, lock1: &FileLock, lock2: &FileLock) -> bool {
        // Same process can hold multiple locks
        if lock1.pid == lock2.pid {
            return false;
        }

        // Check if ranges overlap
        if !self.ranges_overlap(lock1, lock2) {
            return false;
        }

        // Two read locks don't conflict
        if lock1.lock_type == LockType::Read && lock2.lock_type == LockType::Read {
            return false;
        }

        true
    }

    /// Check if two lock ranges overlap
    fn ranges_overlap(&self, lock1: &FileLock, lock2: &FileLock) -> bool {
        let end1 = if lock1.len == 0 {
            u64::MAX
        } else {
            lock1.start + lock1.len
        };

        let end2 = if lock2.len == 0 {
            u64::MAX
        } else {
            lock2.start + lock2.len
        };

        lock1.start < end2 && lock2.start < end1
    }
}

/// Database file detector
pub struct DatabaseDetector {
    /// Patterns to match database files
    patterns: Vec<String>,
}

impl DatabaseDetector {
    /// Create a new database detector
    pub fn new(patterns: Vec<String>) -> Self {
        Self { patterns }
    }

    /// Check if a file is a database file
    pub fn is_database_file(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();

        for pattern in &self.patterns {
            if pattern.starts_with('*') && pattern.ends_with('*') {
                // Contains pattern
                let search = &pattern[1..pattern.len() - 1];
                if path_str.contains(search) {
                    return true;
                }
            } else if let Some(suffix) = pattern.strip_prefix('*') {
                // Ends with pattern
                if path_str.ends_with(suffix) {
                    return true;
                }
            } else if pattern.ends_with('*') {
                // Starts with pattern
                let prefix = &pattern[..pattern.len() - 1];
                if path_str.starts_with(prefix) {
                    return true;
                }
            } else {
                // Exact match
                if path_str == *pattern {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a file is likely a database journal/WAL file
    pub fn is_journal_file(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();

        // Common journal/WAL patterns
        path_str.ends_with("-journal")
            || path_str.ends_with("-wal")
            || path_str.ends_with(".log")
            || path_str.contains("-shm")
    }
}

/// Write mode for database files
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseWriteMode {
    /// Normal buffered writes
    Normal,
    /// Write-through for journal files
    WriteThrough,
    /// Direct I/O for main database files
    DirectIO,
}

/// Database file cache policy
#[derive(Debug, Clone)]
pub struct DatabaseCachePolicy {
    /// Write mode based on file type
    write_mode: DatabaseWriteMode,
    /// Whether to use read-ahead
    read_ahead: bool,
    /// Cache priority
    priority: u8,
}

impl DatabaseCachePolicy {
    /// Get policy for a database file
    pub fn for_file(path: &Path, detector: &DatabaseDetector) -> Self {
        if detector.is_journal_file(path) {
            // Journal files need write-through for durability
            Self {
                write_mode: DatabaseWriteMode::WriteThrough,
                read_ahead: false,
                priority: 255, // Highest priority
            }
        } else if detector.is_database_file(path) {
            // Main database files use direct I/O
            Self {
                write_mode: DatabaseWriteMode::DirectIO,
                read_ahead: false,
                priority: 200,
            }
        } else {
            // Normal files
            Self {
                write_mode: DatabaseWriteMode::Normal,
                read_ahead: true,
                priority: 100,
            }
        }
    }

    /// Get write mode
    pub fn write_mode(&self) -> DatabaseWriteMode {
        self.write_mode
    }

    /// Check if read-ahead is enabled
    pub fn read_ahead_enabled(&self) -> bool {
        self.read_ahead
    }

    /// Get cache priority
    pub fn cache_priority(&self) -> u8 {
        self.priority
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_manager() {
        let manager = LockManager::new();
        let file_id = FileId::new();

        // Set a write lock
        let lock1 = FileLock {
            lock_type: LockType::Write,
            pid: 1000,
            start: 0,
            len: 100,
        };

        assert!(manager.set_lock(&file_id, lock1.clone()).is_ok());

        // Try to set conflicting lock
        let lock2 = FileLock {
            lock_type: LockType::Write,
            pid: 2000,
            start: 50,
            len: 100,
        };

        assert!(manager.set_lock(&file_id, lock2.clone()).is_err());

        // Test lock should return the conflicting lock
        let conflict = manager.test_lock(&file_id, &lock2);
        assert!(conflict.is_some());
        assert_eq!(conflict.unwrap().pid, 1000);
    }

    #[test]
    fn test_database_detector() {
        let patterns = vec![
            "*.db".to_string(),
            "*.sqlite".to_string(),
            "*.sqlite3".to_string(),
        ];

        let detector = DatabaseDetector::new(patterns);

        assert!(detector.is_database_file(Path::new("test.db")));
        assert!(detector.is_database_file(Path::new("/path/to/data.sqlite")));
        assert!(!detector.is_database_file(Path::new("test.txt")));

        assert!(detector.is_journal_file(Path::new("test.db-journal")));
        assert!(detector.is_journal_file(Path::new("test.db-wal")));
    }
}
