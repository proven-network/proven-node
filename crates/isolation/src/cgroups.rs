//! Cgroups v2 control for process resource limits.
//!
//! This module provides functionality for setting up cgroups v2
//! memory controllers for isolated processes.

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::io::ErrorKind;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
use tracing::warn;
#[cfg(target_os = "linux")]
use tracing::{debug, error, info};

#[cfg(target_os = "linux")]
use crate::error::Error;
use crate::error::Result;

/// Cgroups v2 memory controller configuration
#[derive(Debug, Clone)]
pub struct CgroupMemoryConfig {
    /// Maximum memory limit in megabytes
    pub limit_mb: usize,
    /// Minimum memory guaranteed in megabytes
    pub min_mb: usize,
    /// Name for the cgroup
    pub name: String,
}

impl Default for CgroupMemoryConfig {
    fn default() -> Self {
        Self {
            limit_mb: 512, // 512MB default
            min_mb: 0,     // No guaranteed memory by default
            name: "isolation".to_string(),
        }
    }
}

/// Cgroups controller for process resource limits
#[derive(Debug)]
pub struct CgroupsController {
    /// Path to the cgroup
    #[cfg(target_os = "linux")]
    cgroup_path: PathBuf,

    /// Whether the cgroup was successfully created
    is_active: bool,

    /// Process ID of the isolated process
    #[cfg(target_os = "linux")]
    pid: u32,
}

// Helper function to attempt enabling memory controller delegation
#[cfg(target_os = "linux")]
fn try_enable_memory_delegation(cgroup_path: &Path) -> Result<()> {
    match fs::write(cgroup_path.join("cgroup.subtree_control"), "+memory") {
        Ok(_) => {
            debug!(
                "Successfully enabled memory controller delegation in {}",
                cgroup_path.display()
            );
            Ok(())
        }
        Err(e) => {
            debug!(
                "Failed to enable memory controller delegation in {} (may be okay/already enabled): {}",
                cgroup_path.display(),
                e
            );
            // Return error to indicate it might need the process move strategy
            Err(Error::Io("Failed to write +memory to subtree_control", e))
        }
    }
}

impl CgroupsController {
    #[cfg(target_os = "linux")]
    const CONTAINER_MAIN_CGROUP: &'static str = "container_main";

    /// Create a new cgroups controller for the specified process
    #[cfg(target_os = "linux")]
    pub fn new(pid: u32, memory_config: &CgroupMemoryConfig) -> Result<Self> {
        // Check if cgroups v2 unified hierarchy is available
        if !Path::new("/sys/fs/cgroup/cgroup.controllers").exists() {
            warn!("Cgroups v2 unified hierarchy not found. Memory limits will not be applied.");
            return Ok(Self {
                cgroup_path: PathBuf::new(),
                is_active: false,
                pid,
            });
        }

        // Check if memory controller is available
        let controllers = match fs::read_to_string("/sys/fs/cgroup/cgroup.controllers") {
            Ok(content) => content,
            Err(e) => {
                warn!("Failed to read available controllers: {}", e);
                return Ok(Self {
                    cgroup_path: PathBuf::new(),
                    is_active: false,
                    pid,
                });
            }
        };

        if !controllers.contains("memory") {
            warn!(
                "Memory controller not available in cgroups v2. Memory limits will not be applied."
            );
            return Ok(Self {
                cgroup_path: PathBuf::new(),
                is_active: false,
                pid,
            });
        }

        let parent_cgroup_path = Path::new("/sys/fs/cgroup");
        debug!(
            "Using container cgroup root: {}",
            parent_cgroup_path.display()
        );

        // Try enabling delegation, if fails, move all root procs to a new cgroup and retry
        let delegation_enabled = match try_enable_memory_delegation(parent_cgroup_path) {
            Ok(_) => true, // Succeeded on first try
            Err(_) => {
                // Failed, likely due to internal processes
                debug!(
                    "Initial attempt to enable memory delegation failed, attempting process move..."
                );

                let intermediate_group_path = parent_cgroup_path.join(Self::CONTAINER_MAIN_CGROUP);
                match fs::create_dir(&intermediate_group_path) {
                    Ok(_) => {
                        debug!(
                            "Created intermediate cgroup: {}",
                            intermediate_group_path.display()
                        );
                        // Move processes only if we just created the group
                        move_all_procs(parent_cgroup_path, &intermediate_group_path);
                    }
                    Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                        debug!(
                            "Intermediate cgroup already exists: {}",
                            intermediate_group_path.display()
                        );
                        // Assume processes were moved previously or don't need moving now
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create intermediate cgroup {}: {}. Cannot attempt process move.",
                            intermediate_group_path.display(),
                            e
                        );
                        // Fall through, retry delegation might still work if it was already enabled
                    }
                }

                // Retry enabling delegation
                match try_enable_memory_delegation(parent_cgroup_path) {
                    Ok(_) => true, // Succeeded on second try
                    Err(_) => {
                        warn!(
                            "Failed to enable memory delegation even after attempting process move."
                        );
                        false // Failed both times
                    }
                }
            }
        };

        if !delegation_enabled {
            warn!(
                "Memory controller delegation could not be enabled. Memory limits might not be enforced."
            );
            // Proceed anyway, but applying limits will likely fail later
        }

        // Create a unique cgroup name for the *isolated* process under the root
        let isolation_cgroup_name = format!("proven_isolation_{}_{}", memory_config.name, pid);
        let isolation_cgroup_path = parent_cgroup_path.join(&isolation_cgroup_name);

        debug!(
            "Creating isolation cgroup at {}",
            isolation_cgroup_path.display()
        );

        // Create the isolation cgroup directory
        match fs::create_dir(&isolation_cgroup_path) {
            Ok(_) => {
                debug!(
                    "Applying memory limits to {}: max={}MB, min={}MB",
                    isolation_cgroup_path.display(),
                    memory_config.limit_mb,
                    memory_config.min_mb
                );

                // Set memory limits in the new isolation cgroup
                match Self::apply_memory_limits(&isolation_cgroup_path, memory_config) {
                    Ok(_) => {
                        // Limits applied, proceed to add process
                        debug!("Memory limits applied successfully.");
                        if let Err(e) =
                            fs::write(isolation_cgroup_path.join("cgroup.procs"), pid.to_string())
                        {
                            error!(
                                "Failed to add isolated process {} to cgroup {}: {}",
                                pid,
                                isolation_cgroup_path.display(),
                                e
                            );
                            // Critical failure, cleanup isolation group and return Err
                            let _ = fs::remove_dir(&isolation_cgroup_path);
                            return Err(Error::Io(
                                "Failed to add isolated process to its cgroup",
                                e,
                            ));
                        }

                        info!(
                            "Successfully created cgroup {} and applied memory limit of {}MB to process {}",
                            isolation_cgroup_path.display(),
                            memory_config.limit_mb,
                            pid
                        );

                        Ok(Self {
                            cgroup_path: isolation_cgroup_path,
                            is_active: true, // Limits were applied successfully
                            pid,
                        })
                    }
                    Err(e) => {
                        error!(
                            "Failed to apply memory limits to {}: {}. Limits will not be enforced.",
                            isolation_cgroup_path.display(),
                            e
                        );
                        // Attempt cleanup of the isolation group, return inactive controller
                        let _ = fs::remove_dir(&isolation_cgroup_path);
                        Ok(Self {
                            cgroup_path: isolation_cgroup_path, // Store path for potential partial info
                            is_active: false,
                            pid,
                        })
                    }
                }
            }
            Err(create_err) => {
                warn!(
                    "Failed to create isolation cgroup directory {}: {}",
                    isolation_cgroup_path.display(),
                    create_err
                );
                // Could not create the directory, return inactive controller
                Ok(Self {
                    cgroup_path: isolation_cgroup_path,
                    is_active: false,
                    pid,
                })
            }
        }
    }

    /// Create a new cgroups controller for non-Linux platforms (no-op)
    ///
    /// # Errors
    ///
    /// None - just returns a Result for compatibility with the Linux implementation.
    #[cfg(not(target_os = "linux"))]
    pub fn new(_pid: u32, _memory_config: &CgroupMemoryConfig) -> Result<Self> {
        warn!("Cgroups are only available on Linux. Memory limits will not be applied.");
        Ok(Self { is_active: false })
    }

    /// Apply memory limits to the cgroup
    #[cfg(target_os = "linux")]
    fn apply_memory_limits(cgroup_path: &Path, config: &CgroupMemoryConfig) -> Result<()> {
        // Convert MB to bytes
        let memory_limit_bytes = config.limit_mb * 1024 * 1024;
        let memory_min_bytes = config.min_mb * 1024 * 1024;

        // Set memory.max (hard limit)
        if let Err(e) = fs::write(
            cgroup_path.join("memory.max"),
            memory_limit_bytes.to_string(),
        ) {
            warn!("Failed to set memory.max: {}", e);
            // Don't return error, try to continue with other settings
        }

        // Set memory.min (guaranteed memory)
        if config.min_mb > 0 {
            if let Err(e) = fs::write(cgroup_path.join("memory.min"), memory_min_bytes.to_string())
            {
                warn!("Failed to set memory.min: {}", e);
                // Don't return error, try to continue with other settings
            }
        }

        // Try to set swap to zero (might not work in all environments)
        if let Err(e) = fs::write(cgroup_path.join("memory.swap.max"), "0") {
            debug!("Failed to set memory.swap.max to zero: {}", e);
            // This is common in Docker, so just debug log it
        }

        // Reset memory.peak to allow for accurate monitoring
        let _ = fs::write(cgroup_path.join("memory.peak"), "0");

        Ok(())
    }

    /// Get the current memory usage of the process in bytes
    #[cfg(target_os = "linux")]
    pub fn current_memory_usage(&self) -> Result<Option<usize>> {
        if !self.is_active {
            return Ok(None);
        }

        // Check if the process is still in the cgroup
        match fs::read_to_string(self.cgroup_path.join("cgroup.procs")) {
            Ok(content) => {
                if !content.trim().contains(&self.pid.to_string()) {
                    debug!(
                        "Process {} is no longer in cgroup, checking memory.peak",
                        self.pid
                    );
                    // Process might have exited, check peak memory instead
                    return self.peak_memory_usage();
                }
            }
            Err(e) => {
                debug!("Failed to read cgroup.procs: {}", e);
                // Cgroup might be gone, try to check peak memory
                return self.peak_memory_usage();
            }
        }

        // Try to read current memory usage
        match fs::read_to_string(self.cgroup_path.join("memory.current")) {
            Ok(content) => {
                let content = content.trim();
                // Sometimes memory.current is empty or zero right after process exit
                if content.is_empty() || content == "0" {
                    debug!("memory.current is empty or zero, checking memory.peak");
                    return self.peak_memory_usage();
                }

                match content.parse::<usize>() {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(e) => {
                        warn!("Failed to parse memory usage: {}", e);
                        self.peak_memory_usage()
                    }
                }
            }
            Err(e) => {
                debug!("Failed to read memory.current: {}", e);
                self.peak_memory_usage()
            }
        }
    }

    /// Get the peak memory usage of the process in bytes
    #[cfg(target_os = "linux")]
    fn peak_memory_usage(&self) -> Result<Option<usize>> {
        match fs::read_to_string(self.cgroup_path.join("memory.peak")) {
            Ok(content) => {
                let content = content.trim();
                if content.is_empty() || content == "0" {
                    return Ok(None);
                }

                match content.parse::<usize>() {
                    Ok(bytes) => {
                        debug!("Using memory.peak value: {} bytes", bytes);
                        Ok(Some(bytes))
                    }
                    Err(e) => {
                        warn!("Failed to parse peak memory usage: {}", e);
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                debug!("Failed to read memory.peak: {}", e);
                Ok(None)
            }
        }
    }

    /// Get the current memory usage of the process in bytes (non-Linux platforms)
    ///
    /// # Errors
    ///
    /// None - just returns a Result for compatibility with the Linux implementation.
    #[cfg(not(target_os = "linux"))]
    #[allow(clippy::unused_self)]
    pub const fn current_memory_usage(&self) -> Result<Option<usize>> {
        Ok(None)
    }

    /// Clean up the cgroup
    #[cfg(target_os = "linux")]
    fn cleanup(&self) {
        if !self.cgroup_path.exists() {
            debug!(
                "Isolation cgroup {} already removed.",
                self.cgroup_path.display()
            );
            return; // Nothing to cleanup if path doesn't exist
        }

        debug!(
            "Cleaning up isolation cgroup at {}",
            self.cgroup_path.display()
        );

        // Try to remove the isolation cgroup directory
        match fs::remove_dir(&self.cgroup_path) {
            Ok(_) => {
                debug!("Successfully removed isolation cgroup directory");
            }
            Err(e) => {
                warn!(
                    "Failed to remove isolation cgroup directory {}: {}. Manual cleanup might be needed.",
                    self.cgroup_path.display(),
                    e
                );
            }
        }
    }

    /// Clean up the cgroup (non-Linux platforms)
    #[cfg(not(target_os = "linux"))]
    #[allow(clippy::unused_self)]
    const fn cleanup(&self) {
        // No-op
    }

    /// Check if the cgroup controller is active
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.is_active
    }
}

#[cfg(target_os = "linux")]
/// Attempts to move all processes from a source cgroup to a destination cgroup.
fn move_all_procs(source_cgroup: &Path, dest_cgroup: &Path) {
    let source_procs_path = source_cgroup.join("cgroup.procs");
    let dest_procs_path = dest_cgroup.join("cgroup.procs");

    match fs::read_to_string(&source_procs_path) {
        Ok(pids_str) => {
            let pids: Vec<&str> = pids_str.lines().collect();
            if pids.is_empty() {
                debug!(
                    "No processes found in source cgroup {}",
                    source_cgroup.display()
                );
                return;
            }
            debug!(
                "Attempting to move {} processes from {} to {}",
                pids.len(),
                source_cgroup.display(),
                dest_cgroup.display()
            );

            for pid_str in pids {
                if pid_str.trim().is_empty() {
                    continue;
                }
                match fs::write(&dest_procs_path, pid_str) {
                    Ok(_) => {
                        // Successfully moved
                        debug!("Moved PID {} to {}", pid_str, dest_cgroup.display());
                    }
                    Err(e) if e.raw_os_error() == Some(nix::libc::ESRCH) => {
                        // Process likely exited between read and write, ignore.
                        debug!("PID {} likely exited before move, skipping.", pid_str);
                    }
                    Err(e) => {
                        // Other errors (permissions, etc.) - log warning and continue
                        warn!(
                            "Failed to move PID {} to {}: {}",
                            pid_str,
                            dest_cgroup.display(),
                            e
                        );
                    }
                }
            }
        }
        Err(e) => {
            warn!(
                "Failed to read processes from source cgroup {}: {}",
                source_cgroup.display(),
                e
            );
        }
    }
}

impl Drop for CgroupsController {
    fn drop(&mut self) {
        if self.is_active {
            let () = self.cleanup();
        }
    }
}
