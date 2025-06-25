//! Isolation primitives for spawning untrusted processes in a secure manner.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

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

use tracing::warn;

/// Counter for generating unique Veth pair IP addresses
static IP_COUNTER: AtomicU32 = AtomicU32::new(2);

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
///     let process = spawn(app).await?;
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
#[allow(clippy::cast_possible_truncation)]
pub async fn spawn<A: IsolatedApplication>(application: A) -> Result<IsolatedProcess> {
    let counter = IP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let host_veth_interface_name = format!("veth{counter}");
    let isolated_veth_interface_name = format!("veth{}", counter + 1);

    // Use a unique subnet for each veth pair to avoid conflicts
    // Each container gets its own /24 subnet: 10.0.{counter}.0/24
    let subnet_id = counter;
    let host_ip_address = IpAddr::V4(Ipv4Addr::new(10, 0, subnet_id as u8, 1));
    let isolated_ip_address = IpAddr::V4(Ipv4Addr::new(10, 0, subnet_id as u8, 2));

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
        host_ip_address,
        host_veth_interface_name,
        is_ready_check: {
            let app = Arc::clone(&app);
            Box::new(move |info| {
                let app = Arc::clone(&app);
                Box::pin(async move { app.is_ready_check(info).await })
            })
        },
        is_ready_check_interval_ms: app.is_ready_check_interval_ms(),
        is_ready_check_max: app.is_ready_check_max(),
        isolated_ip_address,
        isolated_veth_interface_name,
        memory_control,
        namespaces: namespace_options,
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
