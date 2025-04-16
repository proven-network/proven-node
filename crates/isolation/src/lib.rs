//! Isolation primitives for spawning untrusted processes in a secure manner.
//!
//! This crate provides a way to isolate third-party applications using Linux
//! namespaces and cgroups v2.
//!
//! # Example
//!
//! ```rust,no_run
//! use async_trait::async_trait;
//! use proven_isolation::{IsolatedApplication, Result, spawn};
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
//! }
//!
//! async fn run() -> Result<()> {
//!     let app = MyApp {
//!         name: "my-app".to_string(),
//!         executable: "path/to/app".to_string(),
//!     };
//!
//!     let (process, _) = spawn(app).await?;
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

use std::sync::Arc;

use tokio::task::JoinHandle;
use tracing::{debug, warn};

mod cgroups;
mod error;
mod isolated_application;
mod namespaces;
mod network;
mod spawn;
mod volume_mount;

pub use cgroups::{CgroupMemoryConfig, CgroupsController};
pub use error::{Error, Result};
pub use isolated_application::IsolatedApplication;
pub use namespaces::{IsolationNamespaces, NamespaceOptions};
pub use network::{VethPair, check_root_permissions};
pub use spawn::{IsolatedProcess, IsolatedProcessOptions, IsolatedProcessSpawner, spawn_process};
pub use volume_mount::VolumeMount;

/// Spawns an isolated process for the given application.
///
/// This is the main entry point for the isolation API. It takes an application that
/// implements the `IsolatedApplication` trait and spawns it in an isolated environment.
///
/// # Example
///
/// ```rust,no_run
/// use async_trait::async_trait;
/// use proven_isolation::{IsolatedApplication, Result, spawn};
/// use std::path::PathBuf;
///
/// struct MyApp {
///     name: String,
///     executable: String,
/// }
///
/// #[async_trait]
/// impl IsolatedApplication for MyApp {
///     fn args(&self) -> Vec<String> {
///         vec!["--config=/etc/config.json".to_string()]
///     }
///
///     fn executable(&self) -> &str {
///         &self.executable
///     }
///
///     fn name(&self) -> &str {
///         &self.name
///     }
/// }
///
/// async fn run() -> Result<()> {
///     let app = MyApp {
///         name: "my-app".to_string(),
///         executable: "path/to/app".to_string(),
///     };
///
///     let (process, _) = spawn(app).await?;
///
///     // App is now running and ready
///
///     // Shut down when done
///     process.shutdown().await?;
///
///     Ok(())
/// }
/// ```
///
/// # Errors
///
/// Returns an error if the process could not be spawned or if the readiness checks fail.
pub async fn spawn<A: IsolatedApplication>(
    application: A,
) -> Result<(IsolatedProcess, JoinHandle<()>)> {
    let application: Arc<dyn IsolatedApplication> = Arc::new(application);
    let mut options = IsolatedProcessOptions::new(
        application.clone(),
        application.executable(),
        application.args(),
    );

    // Apply configuration from the application
    if let Some(working_dir) = application.working_dir() {
        options = options.with_working_dir(working_dir);
    }

    options = options.with_volume_mounts(application.volume_mounts());

    // Configure namespaces based on application preferences
    let mut namespace_options = application.namespace_options();
    if !application.use_user_namespace() {
        namespace_options.use_user = false;
    }
    if !application.use_pid_namespace() {
        namespace_options.use_pid = false;
    }
    if !application.use_network_namespace() {
        namespace_options.use_network = false;
    }
    if !application.use_mount_namespace() {
        namespace_options.use_mount = false;
    }
    if !application.use_uts_namespace() {
        namespace_options.use_uts = false;
    }
    if !application.use_ipc_namespace() {
        namespace_options.use_ipc = false;
    }
    options = options.with_namespaces(namespace_options);

    // Set environment variables
    for (key, value) in application.env() {
        options = options.with_env(key, value);
    }

    // Configure memory limits if enabled
    if application.use_memory_limits() {
        let memory_config = CgroupMemoryConfig {
            name: application.name().to_string(),
            limit_mb: application.memory_limit_mb(),
            min_mb: application.memory_min_mb(),
        };
        options = options.with_memory_control(memory_config);
    }

    // Wait for application to be ready for configuration
    debug!("Preparing configuration for {}", application.name());

    let mut spawner = IsolatedProcessSpawner::new(options);
    spawner.spawn().await
}
