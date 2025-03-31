//! Process spawning functionality for isolated applications.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::IsolatedApplication;
use crate::cgroups::{CgroupMemoryConfig, CgroupsController};
use crate::error::{Error, Result};
use crate::namespaces::NamespaceOptions;

/// Options for spawning an isolated process.
#[derive(Clone)]
pub struct IsolatedProcessOptions {
    /// The arguments to pass to the executable.
    pub args: Vec<String>,

    /// The root directory for chroot.
    pub chroot_dir: Option<PathBuf>,

    /// Environment variables to set.
    pub env: HashMap<String, String>,

    /// The executable to run.
    pub executable: PathBuf,

    /// Memory control configuration using cgroups.
    pub memory_control: Option<CgroupMemoryConfig>,

    /// Namespace options.
    pub namespaces: NamespaceOptions,

    /// Application for handling process output.
    pub application: Option<Arc<dyn IsolatedApplication>>,

    /// The working directory for the process.
    pub working_dir: Option<PathBuf>,
}

impl std::fmt::Debug for IsolatedProcessOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsolatedProcessOptions")
            .field("args", &self.args)
            .field("chroot_dir", &self.chroot_dir)
            .field("env", &self.env)
            .field("executable", &self.executable)
            .field("memory_control", &self.memory_control)
            .field("namespaces", &self.namespaces)
            .field("application", &format!("Arc<dyn IsolatedApplication>"))
            .field("working_dir", &self.working_dir)
            .finish()
    }
}

impl IsolatedProcessOptions {
    /// Creates a new `IsolatedProcessOptions`.
    #[must_use]
    pub fn new<P: AsRef<Path>, A: AsRef<OsStr>>(
        executable: P,
        args: impl IntoIterator<Item = A>,
    ) -> Self {
        Self {
            args: args
                .into_iter()
                .map(|a| a.as_ref().to_string_lossy().to_string())
                .collect(),
            chroot_dir: None,
            env: HashMap::new(),
            executable: executable.as_ref().to_path_buf(),
            memory_control: None,
            namespaces: NamespaceOptions::default(),
            application: None,
            working_dir: None,
        }
    }

    /// Sets the working directory for the process.
    #[must_use]
    pub fn with_working_dir<P: AsRef<Path>>(mut self, working_dir: P) -> Self {
        self.working_dir = Some(working_dir.as_ref().to_path_buf());
        self
    }

    /// Sets the root directory for chroot.
    #[must_use]
    pub fn with_chroot<P: AsRef<Path>>(mut self, chroot_dir: P) -> Self {
        self.chroot_dir = Some(chroot_dir.as_ref().to_path_buf());
        self
    }

    /// Sets an environment variable for the process.
    #[must_use]
    pub fn with_env<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    /// Sets the namespace options for the process.
    #[must_use]
    pub fn with_namespaces(mut self, namespaces: NamespaceOptions) -> Self {
        self.namespaces = namespaces;
        self
    }

    /// Sets the memory control configuration for the process.
    #[must_use]
    pub fn with_memory_control(mut self, memory_control: CgroupMemoryConfig) -> Self {
        self.memory_control = Some(memory_control);
        self
    }

    /// Sets the output handler for the process.
    #[must_use]
    pub fn with_application(mut self, application: Arc<dyn IsolatedApplication>) -> Self {
        self.application = Some(application);
        self
    }
}

/// Represents a running isolated process.
pub struct IsolatedProcess {
    /// Cgroups controller if active
    cgroups_controller: Option<CgroupsController>,

    /// Process ID
    pid: u32,

    /// Shutdown token to request termination
    shutdown_token: CancellationToken,

    /// Task tracker for all tasks associated with this process
    task_tracker: TaskTracker,
}

impl IsolatedProcess {
    /// Returns the process ID.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Waits for the process to exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the process exits with a non-zero status.
    pub async fn wait(&self) -> Result<ExitStatus> {
        let pid_i32 = self.pid as i32;
        let check_interval = Duration::from_millis(100);

        // Loop until the process no longer exists
        loop {
            let exists = unsafe { libc::kill(pid_i32, 0) == 0 };

            if !exists {
                // Process has exited
                let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                if errno == libc::ESRCH {
                    // Process doesn't exist (this is the normal case)
                    debug!("Process {} no longer exists", self.pid);
                    return Ok(ExitStatus::from_raw(0));
                }

                // Some other error occurred (e.g., permission denied)
                warn!(
                    "Error checking process {}: {}",
                    self.pid,
                    std::io::Error::last_os_error()
                );
                return Ok(ExitStatus::from_raw(0));
            }

            // Process still exists, wait a bit and try again
            tokio::time::sleep(check_interval).await;
        }
    }

    /// Sends a signal to the process.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal could not be sent.
    pub fn signal(&self, signal: Signal) -> Result<()> {
        let pid = Pid::from_raw(self.pid as i32);
        signal::kill(pid, signal).map_err(|e| {
            Error::Io(
                "Failed to send signal to process",
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            )
        })
    }

    /// Shuts down the process.
    ///
    /// # Errors
    ///
    /// Returns an error if the process could not be shut down.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down process");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        // Clean up cgroups if active
        if let Some(ref controller) = self.cgroups_controller {
            if let Err(e) = controller.cleanup() {
                warn!("Failed to clean up cgroups: {}", e);
            }
        }

        info!("Process shut down");
        Ok(())
    }

    /// Returns the current memory usage of the process in bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the memory usage could not be retrieved.
    pub fn current_memory_usage(&self) -> Result<Option<usize>> {
        if let Some(ref controller) = self.cgroups_controller {
            controller.current_memory_usage()
        } else {
            Ok(None)
        }
    }

    /// Returns the current memory usage of the process in megabytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the memory usage could not be retrieved.
    pub fn current_memory_usage_mb(&self) -> Result<Option<f64>> {
        if let Some(bytes) = self.current_memory_usage()? {
            // Convert bytes to MB
            let mb = bytes as f64 / (1024.0 * 1024.0);
            Ok(Some(mb))
        } else {
            Ok(None)
        }
    }

    /// Returns whether memory control is active for this process.
    pub fn has_memory_control(&self) -> bool {
        self.cgroups_controller
            .as_ref()
            .map_or(false, |c| c.is_active())
    }
}

/// Spawns isolated processes.
#[derive(Debug)]
pub struct IsolatedProcessSpawner {
    /// Options for the process.
    pub options: IsolatedProcessOptions,
}

impl IsolatedProcessSpawner {
    /// Creates a new `IsolatedProcessSpawner`.
    #[must_use]
    pub fn new(options: IsolatedProcessOptions) -> Self {
        Self { options }
    }

    /// Spawns an isolated process.
    ///
    /// # Errors
    ///
    /// Returns an error if the process could not be spawned.
    pub async fn spawn(&self) -> Result<IsolatedProcess> {
        let options = self.options.clone();
        let shutdown_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        #[cfg(target_os = "linux")]
        let mut cmd = {
            // Prepare the command to be executed with appropriate isolation
            let unshare_args = self.build_unshare_args(&options)?;

            // Construct the full command
            let mut cmd = Command::new("unshare");
            cmd.args(unshare_args);

            // Set the working directory if specified
            if let Some(ref working_dir) = options.working_dir {
                cmd.current_dir(working_dir);
            }

            // Set environment variables
            for (key, value) in &options.env {
                cmd.env(key, value);
            }

            cmd
        };

        #[cfg(not(target_os = "linux"))]
        let mut cmd = {
            // On non-Linux platforms, just run the command directly
            warn!(
                "Linux isolation features (namespaces, chroot, seccomp) are not available on this platform. Running process without isolation."
            );

            // Construct the direct command
            let mut cmd = Command::new(&options.executable);
            cmd.args(&options.args);

            // Set the working directory if specified
            if let Some(ref working_dir) = options.working_dir {
                cmd.current_dir(working_dir);
            }

            // Set environment variables
            for (key, value) in &options.env {
                cmd.env(key, value);
            }

            cmd
        };

        // Setup stdio
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        debug!("Spawning process: {:?}", cmd);

        // Spawn the command
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Io("Failed to spawn process", e))?;

        // Get the process ID - this should always be available for a successfully spawned process
        let pid = child.id().ok_or_else(|| {
            Error::SpawnProcess("No PID available for spawned process".to_string())
        })?;

        debug!("Process spawned with PID: {}", pid);

        // Take stdout and stderr before they're moved into closures
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        // Create a cancellation token for the child process
        let _child_token = shutdown_token.child_token();

        // Start a task to handle stdout from the child process
        if let Some(stdout) = stdout {
            if let Some(ref application) = options.application {
                let application = Arc::clone(application);
                task_tracker.spawn(async move {
                    let mut lines = BufReader::new(stdout).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        application.handle_stdout(&line);
                    }
                });
            } else {
                task_tracker.spawn(async move {
                    let mut lines = BufReader::new(stdout).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        info!("process stdout: {}", line);
                    }
                });
            }
        }

        // Start a task to handle stderr from the child process
        if let Some(stderr) = stderr {
            if let Some(ref application) = options.application {
                let application = Arc::clone(application);
                task_tracker.spawn(async move {
                    let mut lines = BufReader::new(stderr).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        application.handle_stderr(&line);
                    }
                });
            } else {
                task_tracker.spawn(async move {
                    let mut lines = BufReader::new(stderr).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        warn!("process stderr: {}", line);
                    }
                });
            }
        }

        // Monitor child process
        let pid_for_task = pid;
        let shutdown_token_clone = shutdown_token.clone();
        tokio::task::spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(status) => {
                            if status.success() {
                                info!("Process exited with status: {}", status);
                            } else {
                                error!("Process exited with non-zero status: {}", status);
                            }
                        }
                        Err(err) => {
                            error!("Failed to wait for process: {}", err);
                        }
                    }
                }
                () = shutdown_token_clone.cancelled() => {
                    info!("Shutdown requested, terminating process...");

                    // Convert to i32 for the kill operation
                    let raw_pid = pid_for_task as i32;
                    let pid = Pid::from_raw(raw_pid);

                    if let Err(err) = signal::kill(pid, Signal::SIGTERM) {
                        error!("Failed to send SIGTERM to process: {}", err);
                    }

                    // Wait for the process to exit with a timeout
                    if let Ok(result) = tokio::time::timeout(
                        Duration::from_secs(10),
                        child.wait()
                    ).await {
                        match result {
                            Ok(status) => {
                                info!("Process exited with status: {}", status);
                            }
                            Err(err) => {
                                error!("Failed to wait for process: {}", err);
                            }
                        }
                    } else {
                        error!("Timeout waiting for process to exit, killing...");
                        if let Err(err) = child.kill().await {
                            error!("Failed to kill process: {}", err);
                        }
                    }
                }
            }
        });

        task_tracker.close();

        // Create a cgroups controller if memory control is configured
        let cgroups_controller = if let Some(ref memory_config) = options.memory_control {
            match CgroupsController::new(pid, memory_config) {
                Ok(controller) => {
                    if controller.is_active() {
                        debug!("Cgroups memory controller activated for process {}", pid);
                        Some(controller)
                    } else {
                        warn!("Cgroups memory controller could not be activated");
                        None
                    }
                }
                Err(e) => {
                    error!("Failed to create cgroups controller: {}", e);
                    None
                }
            }
        } else {
            None
        };

        Ok(IsolatedProcess {
            cgroups_controller,
            pid,
            shutdown_token,
            task_tracker,
        })
    }

    /// Build the arguments for the unshare command.
    ///
    /// This is only available on Linux.
    #[cfg(target_os = "linux")]
    fn build_unshare_args(&self, options: &IsolatedProcessOptions) -> Result<Vec<String>> {
        let mut args = Vec::new();

        // Add namespace arguments using the options.namespaces.to_unshare_args() helper
        args.extend(options.namespaces.to_unshare_args());

        // Add chroot if specified
        if let Some(ref chroot_dir) = options.chroot_dir {
            args.push("chroot".to_string());
            args.push(chroot_dir.to_string_lossy().to_string());
        }

        // Add the actual command and arguments
        args.push(options.executable.to_string_lossy().to_string());
        args.extend(options.args.clone());

        Ok(args)
    }
}

/// Spawn an isolated process with the given options.
///
/// # Errors
///
/// Returns an error if the process could not be spawned.
pub async fn spawn_process(options: IsolatedProcessOptions) -> Result<IsolatedProcess> {
    let spawner = IsolatedProcessSpawner::new(options);
    spawner.spawn().await
}
