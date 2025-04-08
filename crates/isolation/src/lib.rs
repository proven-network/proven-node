//! Isolation primitives for spawning untrusted processes in a secure manner.
//!
//! This crate provides a way to isolate third-party applications using Linux
//! namespaces and cgroups v2.
//!
//! # Example
//!
//! ```rust,no_run
//! use async_trait::async_trait;
//! use proven_isolation::{IsolatedApplication, IsolationManager, Result};
//! use std::path::PathBuf;
//!
//! struct MyApp {
//!     name: String,
//!     executable: String,
//! }
//!
//! #[async_trait]
//! impl IsolatedApplication for MyApp {
//!     fn args(&self) -> Vec<String> {
//!         vec!["--config=/etc/config.json".to_string()]
//!     }
//!
//!     fn executable(&self) -> &str {
//!         &self.executable
//!     }
//!
//!     fn name(&self) -> &str {
//!         &self.name
//!     }
//!
//!     async fn is_ready_check(
//!         &self,
//!         _process: &proven_isolation::IsolatedProcess,
//!     ) -> Result<bool> {
//!         // In a real application, this might check an HTTP endpoint
//!         Ok(true)
//!     }
//! }
//!
//! async fn run() -> Result<()> {
//!     let app = MyApp {
//!         name: "my-app".to_string(),
//!         executable: "path/to/app".to_string(),
//!     };
//!
//!     let manager = IsolationManager::new();
//!     let process = manager.spawn(app).await?;
//!
//!     // App is now running and ready
//!
//!     // Shut down when done
//!     process.shutdown().await?;
//!
//!     Ok(())
//! }
//! ```
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use nix::sys::signal::Signal;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

mod cgroups;
mod error;
mod namespaces;
mod network;
mod spawn;
#[cfg(test)]
mod tests;

pub use cgroups::{CgroupMemoryConfig, CgroupsController};
pub use error::{Error, Result};
pub use namespaces::{IsolationNamespaces, NamespaceOptions};
pub use network::{VethPair, check_root_permissions};
pub use spawn::{IsolatedProcess, IsolatedProcessOptions, IsolatedProcessSpawner, spawn_process};

/// Represents a volume mount from the host to the container
#[derive(Debug, Clone)]
pub struct VolumeMount {
    /// The path on the host system
    pub host_path: PathBuf,
    /// The path inside the container where the volume will be mounted
    pub container_path: PathBuf,
    /// Whether the mount should be read-only
    pub read_only: bool,
}

impl VolumeMount {
    /// Creates a new volume mount
    #[must_use]
    pub fn new<P: AsRef<Path>>(host_path: P, container_path: P) -> Self {
        Self {
            host_path: host_path.as_ref().to_path_buf(),
            container_path: container_path.as_ref().to_path_buf(),
            read_only: false,
        }
    }

    /// Creates a new read-only volume mount
    #[must_use]
    pub fn new_read_only<P: AsRef<Path>>(host_path: P, container_path: P) -> Self {
        Self {
            host_path: host_path.as_ref().to_path_buf(),
            container_path: container_path.as_ref().to_path_buf(),
            read_only: true,
        }
    }
}

/// Trait for applications that can be run in isolation.
#[async_trait]
pub trait IsolatedApplication: Send + Sync + 'static {
    /// Returns the arguments for the process
    fn args(&self) -> Vec<String>;

    /// Returns the root directory for chroot if needed
    fn chroot_dir(&self) -> Option<PathBuf> {
        None
    }

    /// Returns any environment variables that should be set
    fn env(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Returns the executable path
    fn executable(&self) -> &str;

    /// Handles a line of output from stdout
    fn handle_stdout(&self, line: &str) {
        tracing::info!("{}: {}", self.name(), line);
    }

    /// Handles a line of output from stderr
    fn handle_stderr(&self, line: &str) {
        tracing::warn!("{}: {}", self.name(), line);
    }

    /// Performs a readiness check for the application
    ///
    /// This can be used to check HTTP endpoints, files, or any other criteria.
    /// The process parameter provides access to the running process.
    ///
    /// Return true if the application is ready, false otherwise.
    ///
    /// # Errors
    ///
    /// This function is allowed to return errors, which will be propagated
    /// to the caller.
    async fn is_ready_check(&self, _process: &IsolatedProcess) -> Result<bool> {
        Ok(true) // By default, assume the application is ready immediately
    }

    /// Returns how often to run the readiness check in milliseconds
    fn is_ready_check_interval_ms(&self) -> u64 {
        1000 // Default to checking once per second
    }

    /// Returns the maximum number of readiness checks to perform
    ///
    /// Returns None for unlimited checks (will continue until the process exits)
    /// A value of 1 would check just once
    fn is_ready_check_max(&self) -> Option<u32> {
        None // Default to unlimited checks
    }

    /// Returns the memory requirements in megabytes (MB)
    /// Default is 512MB if not specified
    fn memory_limit_mb(&self) -> usize {
        512
    }

    /// Returns the minimum memory guaranteed in megabytes (MB)
    /// Default is 0, meaning no guaranteed memory
    fn memory_min_mb(&self) -> usize {
        0
    }

    /// Returns the name of the application
    fn name(&self) -> &str;

    /// Returns the namespaces options for the application
    fn namespace_options(&self) -> NamespaceOptions {
        NamespaceOptions::default()
    }

    /// Creates any configuration files needed before starting the application
    async fn prepare_config(&self) -> Result<()> {
        Ok(())
    }

    /// Returns the signal to use when shutting down the application
    fn shutdown_signal(&self) -> Signal {
        Signal::SIGTERM
    }

    /// Returns a list of TCP ports that should be forwarded from the host to the container
    ///
    /// The same port number will be used on both the host and container.
    /// This is only used when network namespaces are enabled.
    fn tcp_ports(&self) -> Vec<u16> {
        Vec::new()
    }

    /// Returns a list of UDP ports that should be forwarded from the host to the container
    ///
    /// The same port number will be used on both the host and container.
    /// This is only used when network namespaces are enabled.
    fn udp_ports(&self) -> Vec<u16> {
        Vec::new()
    }

    /// Returns the volume mounts that should be available to the application
    ///
    /// Each volume mount specifies a path from the host system that should be
    /// mounted into the container at a specific location.
    ///
    /// This is only used when mount namespaces are enabled.
    fn volume_mounts(&self) -> Vec<VolumeMount> {
        Vec::new()
    }

    /// Returns the working directory for the process
    fn working_dir(&self) -> Option<PathBuf> {
        None
    }
}

/// Configuration for an isolated application.
#[derive(Debug, Clone)]
pub struct IsolationConfig {
    /// Whether to use a chroot
    pub use_chroot: bool,

    /// Whether to use IPC namespaces
    pub use_ipc_namespace: bool,

    /// Whether to use memory limits
    pub use_memory_limits: bool,

    /// Whether to use mount namespaces
    pub use_mount_namespace: bool,

    /// Whether to use network namespaces
    pub use_network_namespace: bool,

    /// Whether to use PID namespaces
    pub use_pid_namespace: bool,

    /// Whether to use user namespaces
    pub use_user_namespace: bool,

    /// Whether to use UTS namespaces
    pub use_uts_namespace: bool,
}

impl Default for IsolationConfig {
    fn default() -> Self {
        Self {
            use_chroot: false, // Disabled by default as it requires more setup
            use_ipc_namespace: true,
            use_memory_limits: true,
            use_mount_namespace: true,
            use_network_namespace: true,
            use_pid_namespace: true,
            use_user_namespace: true,
            use_uts_namespace: true,
        }
    }
}

/// Manages isolated applications.
#[derive(Default)]
pub struct IsolationManager {
    config: IsolationConfig,
}

impl IsolationManager {
    /// Creates a new isolation manager with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new isolation manager with the specified configuration.
    #[must_use]
    pub fn with_config(config: IsolationConfig) -> Self {
        Self { config }
    }

    /// Builds the options for spawning an isolated process.
    ///
    /// # Errors
    ///
    /// Returns an error if the options could not be built.
    async fn build_options<A: IsolatedApplication>(
        &self,
        application: A,
    ) -> Result<IsolatedProcessOptions> {
        let mut options = IsolatedProcessOptions::new(application.executable(), application.args());

        // Set the application for output handling and other functionality
        let application = Arc::new(application);
        options = options.with_application(application.clone());

        // Apply configuration from the application
        if let Some(working_dir) = application.working_dir() {
            options = options.with_working_dir(working_dir);
        }

        if let Some(chroot_dir) = application.chroot_dir() {
            if self.config.use_chroot {
                options = options.with_chroot(chroot_dir);
            }
        }

        // Configure namespaces
        let mut namespace_options = application.namespace_options();
        if !self.config.use_user_namespace {
            namespace_options.use_user = false;
        }
        if !self.config.use_pid_namespace {
            namespace_options.use_pid = false;
        }
        if !self.config.use_network_namespace {
            namespace_options.use_network = false;
        }
        if !self.config.use_mount_namespace {
            namespace_options.use_mount = false;
        }
        if !self.config.use_uts_namespace {
            namespace_options.use_uts = false;
        }
        if !self.config.use_ipc_namespace {
            namespace_options.use_ipc = false;
        }
        options = options.with_namespaces(namespace_options);

        // Set environment variables
        for (key, value) in application.env() {
            options = options.with_env(key, value);
        }

        // Configure memory limits
        if self.config.use_memory_limits {
            let memory_config = CgroupMemoryConfig {
                name: application.name().to_string(),
                limit_mb: application.memory_limit_mb(),
                min_mb: application.memory_min_mb(),
            };
            options = options.with_memory_control(memory_config);
        }

        // Wait for application to be ready for configuration
        debug!("Preparing configuration for {}", application.name());
        application.prepare_config().await?;

        Ok(options)
    }

    /// Spawns an isolated process.
    ///
    /// # Errors
    ///
    /// Returns an error if the process could not be spawned.
    pub async fn spawn<A: IsolatedApplication>(
        &self,
        application: A,
    ) -> Result<(IsolatedProcess, JoinHandle<()>)> {
        let mut spawner = IsolatedProcessSpawner::new(self.build_options(application).await?);
        spawner.spawn().await
    }
}
