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
use tracing::warn;

mod cgroups;
mod error;
mod isolated_application;
mod namespaces;
mod network;
mod spawn;
mod volume_mount;

pub use cgroups::{CgroupMemoryConfig, CgroupsController};
pub use error::{Error, Result};
pub use isolated_application::{IsolatedApplication, ReadyCheckInfo};
pub use namespaces::{IsolationNamespaces, NamespaceOptions};
pub use network::{VethPair, check_root_permissions};
pub use spawn::{IsolatedProcess, IsolatedProcessOptions, IsolatedProcessSpawner};
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
    // Configure namespaces based on application preferences
    let mut namespace_options = NamespaceOptions::default();
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

    // Configure memory limits if enabled
    let memory_control = if application.use_memory_limits() {
        Some(CgroupMemoryConfig {
            name: application.name().to_string(),
            limit_mb: application.memory_limit_mb(),
            min_mb: application.memory_min_mb(),
        })
    } else {
        None
    };

    let app = Arc::new(application);

    let options = IsolatedProcessOptions {
        args: app.args(),
        env: app.env().into_iter().collect(),
        executable: app.executable().into(),
        memory_control,
        namespaces: namespace_options,
        is_ready_check: {
            let app = Arc::clone(&app);
            Box::new(move |info| {
                let app = Arc::clone(&app);
                Box::pin(async move { app.is_ready_check(info).await })
            })
        },
        is_ready_check_interval_ms: app.is_ready_check_interval_ms(),
        is_ready_check_max: app.is_ready_check_max(),
        shutdown_signal: app.shutdown_signal(),
        shutdown_timeout: app.shutdown_timeout(),
        stdout_handler: {
            let app = Arc::clone(&app);
            Box::new(move |line| app.handle_stdout(line))
        },
        stderr_handler: {
            let app = Arc::clone(&app);
            Box::new(move |line| app.handle_stderr(line))
        },
        tcp_ports: app.tcp_port_forwards(),
        udp_ports: app.udp_port_forwards(),
        volume_mounts: app.volume_mounts(),
        working_dir: app.working_dir(),
    };

    let mut spawner = IsolatedProcessSpawner::new(options);
    spawner.spawn().await
}
