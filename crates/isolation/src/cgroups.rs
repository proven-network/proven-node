//! Cgroups v2 control for process resource limits.
//!
//! This module provides functionality for setting up cgroups v2
//! memory controllers for isolated processes.

#[cfg(target_os = "linux")]
use std::fs;
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

    /// Process ID
    #[cfg(target_os = "linux")]
    pid: u32,
}

impl CgroupsController {
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

        // Detect Docker environment - check if we're in a Docker container
        let docker_cgroup = Path::new("/sys/fs/cgroup/docker");
        let parent_cgroup = if docker_cgroup.exists() {
            debug!("Docker cgroup detected, using Docker hierarchy");
            docker_cgroup.to_path_buf()
        } else {
            debug!("Using root cgroup hierarchy");
            Path::new("/sys/fs/cgroup").to_path_buf()
        };

        // Verify that memory controller is enabled in parent cgroup
        match fs::read_to_string(parent_cgroup.join("cgroup.subtree_control")) {
            Ok(content) => {
                if !content.contains("memory") {
                    debug!("Memory controller not enabled in parent cgroup, attempting to enable");
                    if let Err(e) =
                        fs::write(parent_cgroup.join("cgroup.subtree_control"), "+memory")
                    {
                        warn!("Failed to enable memory controller in parent cgroup: {}", e);
                        // Continue anyway, might still work
                    }
                } else {
                    debug!("Memory controller is already enabled in parent cgroup");
                }
            }
            Err(e) => {
                warn!("Failed to read parent cgroup subtree_control: {}", e);
                // Continue anyway, might still work
            }
        }

        // Create a unique cgroup name for this process
        let cgroup_name = format!("proven_isolation_{}_{}", memory_config.name, pid);
        let cgroup_path = parent_cgroup.join(&cgroup_name);

        debug!("Creating cgroup at {}", cgroup_path.display());

        // Create the cgroup directory
        match fs::create_dir_all(&cgroup_path) {
            Ok(_) => {
                debug!(
                    "Setting memory limits: max={}MB, min={}MB",
                    memory_config.limit_mb, memory_config.min_mb
                );

                // Set memory limits
                Self::apply_memory_limits(&cgroup_path, memory_config)?;

                // Add the process to the cgroup
                debug!("Adding process {} to cgroup {}", pid, cgroup_path.display());
                if let Err(e) = fs::write(cgroup_path.join("cgroup.procs"), pid.to_string()) {
                    error!("Failed to add process {} to cgroup: {}", pid, e);
                    // Try to clean up the cgroup
                    let _ = fs::remove_dir_all(&cgroup_path);
                    return Err(Error::Io("Failed to add process to cgroup", e));
                }

                info!(
                    "Successfully applied memory limit of {}MB to process {}",
                    memory_config.limit_mb, pid
                );

                Ok(Self {
                    cgroup_path,
                    is_active: true,
                    pid,
                })
            }
            Err(e) => {
                warn!("Failed to create cgroup directory: {}", e);
                Ok(Self {
                    cgroup_path,
                    is_active: false,
                    pid,
                })
            }
        }
    }

    /// Create a new cgroups controller for non-Linux platforms (no-op)
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
    #[cfg(not(target_os = "linux"))]
    pub fn current_memory_usage(&self) -> Result<Option<usize>> {
        Ok(None)
    }

    /// Clean up the cgroup
    #[cfg(target_os = "linux")]
    pub fn cleanup(&self) -> Result<()> {
        if !self.is_active {
            return Ok(());
        }

        debug!("Cleaning up cgroup at {}", self.cgroup_path.display());

        // Determine parent cgroup
        let parent_cgroup_path = if let Some(parent) = self.cgroup_path.parent() {
            parent.to_path_buf()
        } else {
            Path::new("/sys/fs/cgroup").to_path_buf()
        };

        // Try to move the process to the parent cgroup
        if let Err(e) = fs::write(
            parent_cgroup_path.join("cgroup.procs"),
            self.pid.to_string(),
        ) {
            // This can fail if process already exited, so just log a warning
            debug!("Failed to move process to parent cgroup: {}", e);
        }

        // Try to remove the cgroup directory
        match fs::remove_dir_all(&self.cgroup_path) {
            Ok(_) => {
                debug!("Successfully removed cgroup directory");
                Ok(())
            }
            Err(_) => {
                // Just log this error since it's common for cleanup to fail in Docker
                debug!("Cannot remove cgroup directory (likely already cleaned up)");
                // Return Ok instead of Err since this is a cleanup operation
                Ok(())
            }
        }
    }

    /// Clean up the cgroup (non-Linux platforms)
    #[cfg(not(target_os = "linux"))]
    pub fn cleanup(&self) -> Result<()> {
        Ok(())
    }

    /// Check if the cgroup controller is active
    pub fn is_active(&self) -> bool {
        self.is_active
    }
}

impl Drop for CgroupsController {
    fn drop(&mut self) {
        if self.is_active {
            let _ = self.cleanup();
        }
    }
}
