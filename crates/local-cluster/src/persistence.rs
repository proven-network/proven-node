//! Persistent directory management for cluster nodes

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use proven_topology::NodeId;
use tracing::{info, warn};

/// Type alias for node directory assignments
type NodeAssignments = HashMap<NodeId, Vec<(String, u32)>>;

/// Manages persistent directories for nodes with reuse support
#[derive(Debug)]
pub struct PersistentDirManager {
    /// Maps specialization name to set of directory numbers in use
    used_dirs: Arc<RwLock<HashMap<String, HashSet<u32>>>>,
    /// Maps node ID to its assigned directories
    node_assignments: Arc<RwLock<NodeAssignments>>,
}

impl PersistentDirManager {
    /// Create a new persistent directory manager
    pub fn new() -> Self {
        Self {
            used_dirs: Arc::new(RwLock::new(HashMap::new())),
            node_assignments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or allocate a persistent directory for a specialization
    #[allow(dead_code)]
    #[allow(clippy::significant_drop_tightening)] // Need to hold lock for entire function
    pub fn get_directory(&self, specialization_prefix: &str) -> Option<u32> {
        let mut used_dirs = self.used_dirs.write();

        // Get the set of currently used directory numbers for this specialization
        let used_set = used_dirs
            .entry(specialization_prefix.to_string())
            .or_default();

        // Find existing directories on disk
        let home_dir = dirs::home_dir()?;
        let proven_dir = home_dir.join(".proven");

        if !proven_dir.exists() {
            // If .proven doesn't exist, start with directory 1
            used_set.insert(1);
            return Some(1);
        }

        // Find all existing directories with this prefix
        let mut existing_dirs = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&proven_dir) {
            for entry in entries.flatten() {
                if let Some(dir_name) = entry.file_name().to_str()
                    && dir_name.starts_with(&format!("{specialization_prefix}-"))
                    && let Some(number_part) =
                        dir_name.strip_prefix(&format!("{specialization_prefix}-"))
                    && let Ok(num) = number_part.parse::<u32>()
                {
                    existing_dirs.push(num);
                }
            }
        }

        existing_dirs.sort_unstable();

        // Try to reuse an existing directory that's not currently in use
        for &dir_num in &existing_dirs {
            if used_set.insert(dir_num) {
                return Some(dir_num);
            }
        }

        // All existing directories are in use, find the next available number
        let next_num = if existing_dirs.is_empty() {
            1
        } else {
            existing_dirs.iter().max().unwrap() + 1
        };

        used_set.insert(next_num);
        Some(next_num)
    }

    /// Release a persistent directory when a node is stopped
    #[allow(dead_code)]
    pub fn release_directory(&self, specialization_prefix: &str, dir_num: u32) {
        let mut used_dirs = self.used_dirs.write();
        if let Some(used_set) = used_dirs.get_mut(specialization_prefix) {
            used_set.remove(&dir_num);
        }
    }

    /// Track directory assignments for a node
    #[allow(dead_code)]
    pub fn track_node_assignments(&self, node_id: NodeId, assignments: Vec<(String, u32)>) {
        let mut node_assignments = self.node_assignments.write();
        node_assignments.insert(node_id, assignments);
    }

    /// Release all directories for a node
    #[allow(dead_code)]
    pub fn release_node_directories(&self, node_id: &NodeId) {
        let mut node_assignments = self.node_assignments.write();
        if let Some(assignments) = node_assignments.remove(node_id) {
            drop(node_assignments); // Release lock before calling other methods

            for (specialization_prefix, dir_num) in assignments {
                self.release_directory(&specialization_prefix, dir_num);
                info!(
                    "Released persistent directory {}-{} for node {}",
                    specialization_prefix, dir_num, node_id
                );
            }
        }
    }

    /// Create a symlink from session directory to persistent directory
    #[allow(dead_code)]
    pub fn create_symlink(persistent_dir: &PathBuf, session_dir: &PathBuf) -> Result<()> {
        use std::fs;

        // Create the persistent directory first (this is where real data will live)
        fs::create_dir_all(persistent_dir).with_context(|| {
            format!(
                "Failed to create persistent directory: {}",
                persistent_dir.display()
            )
        })?;

        // Create parent directory for session path if it doesn't exist
        if let Some(parent) = session_dir.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create parent directory for: {}",
                    session_dir.display()
                )
            })?;
        }

        // Remove session directory if it exists (so we can create symlink)
        if session_dir.exists() {
            if session_dir.is_dir() && !session_dir.is_symlink() {
                fs::remove_dir_all(session_dir).with_context(|| {
                    format!(
                        "Failed to remove existing directory: {}",
                        session_dir.display()
                    )
                })?;
            } else {
                fs::remove_file(session_dir).with_context(|| {
                    format!("Failed to remove existing file: {}", session_dir.display())
                })?;
            }
        }

        // Create the symlink from session location to persistent location
        if let Err(e) = symlink(persistent_dir, session_dir) {
            warn!(
                "Failed to create symlink from {:?} to {:?}: {}",
                session_dir, persistent_dir, e
            );
            // If symlink fails, create the session directory normally
            fs::create_dir_all(session_dir)?;
        } else {
            info!("Created symlink: {:?} -> {:?}", session_dir, persistent_dir);
        }

        Ok(())
    }
}

impl Default for PersistentDirManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "Method uses underscore-prefixed fields"]
    fn test_directory_allocation() {
        let manager = PersistentDirManager::new();

        // First allocation should be 1
        let dir1 = manager.get_directory("test").unwrap();
        assert_eq!(dir1, 1);

        // Second allocation should be 2
        let dir2 = manager.get_directory("test").unwrap();
        assert_eq!(dir2, 2);

        // Different specialization should start at 1
        let dir3 = manager.get_directory("other").unwrap();
        assert_eq!(dir3, 1);
    }

    #[test]
    #[ignore = "Method uses underscore-prefixed fields"]
    fn test_directory_release_and_reuse() {
        let manager = PersistentDirManager::new();

        // Allocate two directories
        let dir1 = manager.get_directory("test").unwrap();
        let dir2 = manager.get_directory("test").unwrap();
        assert_eq!(dir1, 1);
        assert_eq!(dir2, 2);

        // Release first directory
        manager.release_directory("test", dir1);

        // Next allocation should reuse directory 1
        let dir3 = manager.get_directory("test").unwrap();
        assert_eq!(dir3, 1);
    }

    #[test]
    #[ignore = "Method uses underscore-prefixed fields"]
    fn test_node_assignment_tracking() {
        let manager = PersistentDirManager::new();
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[0u8; 32]);
        let verifying_key = signing_key.verifying_key();
        let node_id = NodeId::from(verifying_key);

        // Track assignments
        let assignments = vec![("rocksdb".to_string(), 1), ("bitcoin".to_string(), 2)];
        manager.track_node_assignments(node_id, assignments);

        // Release node directories
        manager.release_node_directories(&node_id);

        // Directories should be available for reuse
        let rocksdb_dir = manager.get_directory("rocksdb").unwrap();
        assert_eq!(rocksdb_dir, 1);
    }
}
