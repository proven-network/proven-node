use crate::IsolatedProcess;
use crate::NamespaceOptions;
use crate::VolumeMount;

use std::error::Error as StdError;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use nix::sys::signal::Signal;

/// Trait for applications that can be run in isolation.
#[async_trait]
pub trait IsolatedApplication: Send + Sync + 'static {
    /// Returns the arguments for the process
    fn args(&self) -> Vec<String> {
        Vec::new()
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
    async fn is_ready_check(&self, _process: &IsolatedProcess) -> Result<bool, Box<dyn StdError>> {
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

    /// Returns the signal to use when shutting down the application
    fn shutdown_signal(&self) -> Signal {
        Signal::SIGTERM
    }

    /// Returns the graceful shutdown timeout
    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    /// Returns a list of TCP ports that should be forwarded from the host to the container
    ///
    /// The same port number will be used on both the host and container.
    /// This is only used when network namespaces are enabled.
    fn tcp_port_forwards(&self) -> Vec<u16> {
        Vec::new()
    }

    /// Returns a list of UDP ports that should be forwarded from the host to the container
    ///
    /// The same port number will be used on both the host and container.
    /// This is only used when network namespaces are enabled.
    fn udp_port_forwards(&self) -> Vec<u16> {
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
