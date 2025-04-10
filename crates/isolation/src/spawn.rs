//! Process spawning functionality for isolated applications.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::net::IpAddr;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::cgroups::{CgroupMemoryConfig, CgroupsController};
use crate::error::{Error, Result};
use crate::namespaces::NamespaceOptions;
use crate::network::{VethPair, check_root_permissions};
use crate::{IsolatedApplication, VolumeMount};

/// Options for spawning an isolated process.
#[derive(Clone)]
pub struct IsolatedProcessOptions {
    /// Application for handling process output.
    pub application: Arc<dyn IsolatedApplication>,

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

    /// The working directory for the process.
    pub working_dir: Option<PathBuf>,

    /// Volume mounts to make available to the process
    pub volume_mounts: Vec<VolumeMount>,
}

impl std::fmt::Debug for IsolatedProcessOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsolatedProcessOptions")
            .field("application", &format!("Arc<dyn IsolatedApplication>"))
            .field("args", &self.args)
            .field("chroot_dir", &self.chroot_dir)
            .field("env", &self.env)
            .field("executable", &self.executable)
            .field("memory_control", &self.memory_control)
            .field("namespaces", &self.namespaces)
            .field("working_dir", &self.working_dir)
            .field("volume_mounts", &self.volume_mounts)
            .finish()
    }
}

impl IsolatedProcessOptions {
    /// Creates a new `IsolatedProcessOptions`.
    #[must_use]
    pub fn new<P: AsRef<Path>, A: AsRef<OsStr>>(
        application: Arc<dyn IsolatedApplication>,
        executable: P,
        args: impl IntoIterator<Item = A>,
    ) -> Self {
        Self {
            application,
            args: args
                .into_iter()
                .map(|a| a.as_ref().to_string_lossy().to_string())
                .collect(),
            chroot_dir: None,
            env: HashMap::new(),
            executable: executable.as_ref().to_path_buf(),
            memory_control: None,
            namespaces: NamespaceOptions::default(),
            working_dir: None,
            volume_mounts: Vec::new(),
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

    /// Sets the volume mounts for the process
    #[must_use]
    pub fn with_volume_mounts(mut self, volume_mounts: Vec<VolumeMount>) -> Self {
        self.volume_mounts = volume_mounts;
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

    /// The virtual ethernet pair for network communication between host and container
    veth_pair: Option<Arc<VethPair>>,
}

impl IsolatedProcess {
    /// Returns the process ID.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Get the container's IP address (as reachable from the host)
    pub fn container_ip(&self) -> Option<IpAddr> {
        self.veth_pair.as_ref().map(|veth| veth.container_ip())
    }

    /// Get the host's IP address (as reachable from the container)
    pub fn host_ip(&self) -> Option<IpAddr> {
        self.veth_pair.as_ref().map(|veth| veth.host_ip())
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

    /// The virtual ethernet pair for network communication between host and container
    pub veth_pair: Option<Arc<VethPair>>,
}

impl IsolatedProcessSpawner {
    /// Creates a new `IsolatedProcessSpawner`.
    #[must_use]
    pub fn new(options: IsolatedProcessOptions) -> Self {
        Self {
            options,
            veth_pair: None,
        }
    }

    /// Build the arguments for the unshare command.
    ///
    /// This is only available on Linux.
    #[cfg(target_os = "linux")]
    fn build_unshare_args(&self, options: &IsolatedProcessOptions) -> Result<Vec<String>> {
        let mut args = Vec::new();

        // Add namespace arguments using the options.namespaces.to_unshare_args() helper
        args.extend(options.namespaces.to_unshare_args());

        // Instead of directly executing the target program, we'll use /bin/sh as an intermediate
        args.push("--".to_string());
        args.push("/bin/sh".to_string());
        args.push("-c".to_string());

        // Build the setup and exec command
        let mut setup_cmd = String::new();

        // Sleep for 5s to allow the network to be setup before executing the target program
        setup_cmd.push_str(&format!("sleep 5; "));

        // If using chroot, set up the chroot environment first
        if let Some(ref chroot_dir) = options.chroot_dir {
            // Create basic directory structure
            setup_cmd.push_str(&format!(
                "mkdir -p {}/bin {}/lib {}/lib64 {}/usr/lib {}/usr/lib64; ",
                chroot_dir.display(),
                chroot_dir.display(),
                chroot_dir.display(),
                chroot_dir.display(),
                chroot_dir.display()
            ));

            // Copy /bin/sh and its dependencies
            setup_cmd.push_str(&format!("cp /bin/sh {}/bin/; ", chroot_dir.display()));

            // Copy common library directories
            setup_cmd.push_str(&format!(
                "cp -r /lib/* {}/lib/ 2>/dev/null || true; ",
                chroot_dir.display()
            ));
            setup_cmd.push_str(&format!(
                "cp -r /lib64/* {}/lib64/ 2>/dev/null || true; ",
                chroot_dir.display()
            ));
            setup_cmd.push_str(&format!(
                "cp -r /usr/lib/* {}/usr/lib/ 2>/dev/null || true; ",
                chroot_dir.display()
            ));
            setup_cmd.push_str(&format!(
                "cp -r /usr/lib64/* {}/usr/lib64/ 2>/dev/null || true; ",
                chroot_dir.display()
            ));

            // Make executables executable
            setup_cmd.push_str(&format!(
                "chmod +x {}/bin/* 2>/dev/null || true; ",
                chroot_dir.display()
            ));
        }

        // Set up volume mounts if mount namespace is enabled
        if options.namespaces.use_mount {
            for mount in &options.volume_mounts {
                // If we're using chroot, adjust paths to be relative to the chroot
                let container_path = if let Some(ref chroot_dir) = options.chroot_dir {
                    // Strip the leading slash if present and join with chroot dir
                    let rel_path = mount
                        .container_path
                        .strip_prefix("/")
                        .unwrap_or(&mount.container_path);
                    chroot_dir.join(rel_path)
                } else {
                    mount.container_path.clone()
                };

                // Create mount point directories
                setup_cmd.push_str(&format!("mkdir -p {}; ", container_path.display()));

                // Perform bind mount
                let mount_opts = if mount.read_only { "ro,bind" } else { "bind" };
                setup_cmd.push_str(&format!(
                    "mount --bind {} {} && mount -o remount,{} {} {}; ",
                    mount.host_path.display(),
                    container_path.display(),
                    mount_opts,
                    container_path.display(),
                    container_path.display()
                ));
            }
        }

        // Change to working directory if specified (before chroot)
        if let Some(ref working_dir) = options.working_dir {
            if let Some(ref chroot_dir) = options.chroot_dir {
                // Make working dir relative to chroot
                let rel_path = working_dir.strip_prefix("/").unwrap_or(working_dir);
                let full_path = chroot_dir.join(rel_path);
                setup_cmd.push_str(&format!("cd {} && ", full_path.display()));
            } else {
                setup_cmd.push_str(&format!("cd {} && ", working_dir.display()));
            }
        }

        // Add chroot if specified
        if let Some(ref chroot_dir) = options.chroot_dir {
            // When using chroot, use /bin/sh to execute the target program
            setup_cmd.push_str(&format!(
                "chroot {} /bin/sh -c \"exec {}{}\"",
                chroot_dir.display(),
                options.executable.display(),
                if options.args.is_empty() {
                    "".to_string()
                } else {
                    format!(" {}", options.args.join(" "))
                }
            ));
        } else {
            // If not using chroot, use exec to replace the shell
            setup_cmd.push_str(&format!(
                "exec {} {}",
                options.executable.display(),
                options.args.join(" ")
            ));
        }

        // Log the shell script before adding it to args
        debug!("Shell script for setup and exec: {}", setup_cmd);

        // Add the setup command as a single argument
        args.push(setup_cmd);

        Ok(args)
    }

    /// Spawns an isolated process.
    ///
    /// # Errors
    ///
    /// Returns an error if the process could not be spawned.
    pub async fn spawn(&mut self) -> Result<(IsolatedProcess, JoinHandle<()>)> {
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
                "Linux isolation features (namespaces, chroot, cgroups) are not available on this platform. Running process without isolation."
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

        // Track the start time to compensate for the network setup sleep
        let start_time = std::time::Instant::now();

        // Spawn the command
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Io("Failed to spawn process", e))?;

        // Get the process ID - this should always be available for a successfully spawned process
        let mut pid = child.id().ok_or_else(|| {
            Error::SpawnProcess("No PID available for spawned process".to_string())
        })?;

        // If using PID namespace process will be forked se find process with `pgrep -P [PARENT_PID]`
        if options.namespaces.use_pid {
            info!("Finding process with pgrep -P {}", pid);

            // Add retry logic to find the child PID
            let max_retries = 10;
            let retry_delay = Duration::from_millis(100);

            for attempt in 1..=max_retries {
                let mut pgrep_cmd = Command::new("pgrep");
                pgrep_cmd.arg("-P").arg(pid.to_string());

                let pgrep_output = pgrep_cmd
                    .output()
                    .await
                    .map_err(|e| Error::Io("Failed to run pgrep", e))?;

                let stdout = String::from_utf8_lossy(&pgrep_output.stdout);
                debug!("pgrep output (attempt {}): '{}'", attempt, stdout);

                if !stdout.trim().is_empty() {
                    // Try to parse the child PID
                    if let Some(found_pid) = stdout
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse().ok())
                    {
                        info!(
                            "Found child process with PID: {} (after {} attempt(s))",
                            found_pid, attempt
                        );
                        pid = found_pid;
                        break;
                    }
                }

                if attempt < max_retries {
                    debug!(
                        "Child process not found yet, retrying in {}ms",
                        retry_delay.as_millis()
                    );
                    tokio::time::sleep(retry_delay).await;
                } else {
                    warn!(
                        "Could not find child process after {} attempts, using parent PID",
                        max_retries
                    );
                }
            }
        }

        debug!("Process spawned with PID: {}", pid);

        // Setup network connectivity if network namespace is used
        let veth_pair = if options.namespaces.use_network {
            // Check if we have root permissions for network setup
            if !check_root_permissions()? {
                warn!("Network namespaces require root permissions for port forwarding setup");
                None
            } else {
                // Set up the veth pair with port lists from application
                match VethPair::new(
                    pid,
                    options.application.tcp_port_forwards(),
                    options.application.udp_port_forwards(),
                )
                .await
                {
                    Ok(veth) => {
                        let veth = Arc::new(veth);
                        self.veth_pair = Some(Arc::clone(&veth));
                        Some(veth)
                    }
                    Err(e) => {
                        warn!("Failed to set up network: {}", e);
                        None
                    }
                }
            }
        } else {
            None
        };

        // Take stdout and stderr before they're moved into closures
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        // Start a task to handle stdout from the child process
        if let Some(stdout) = stdout {
            let app = Arc::clone(&options.application);
            task_tracker.spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    app.handle_stdout(&line);
                }
            });
        }

        // Start a task to handle stderr from the child process
        if let Some(stderr) = stderr {
            let app = Arc::clone(&options.application);
            task_tracker.spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    app.handle_stderr(&line);
                }
            });
        }

        // Monitor child process
        let pid_for_task = pid;
        let shutdown_signal = options.application.shutdown_signal();
        let shutdown_timeout = options.application.shutdown_timeout();
        let shutdown_token_for_task = shutdown_token.clone();
        let join_handle = tokio::task::spawn(async move {
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
                () = shutdown_token_for_task.cancelled() => {
                    info!("Shutdown requested, terminating process...");

                    // Convert to i32 for the kill operation
                    let pid = Pid::from_raw(pid_for_task as i32);

                    debug!("Sending {} to process {}", shutdown_signal, pid);

                    if let Err(err) = signal::kill(pid, shutdown_signal) {
                        error!("Failed to send {} to process: {}", shutdown_signal, err);
                    }

                    // Wait for the actual process to exit, not the unshare parent
                    let check_interval = Duration::from_millis(100);
                    let start = std::time::Instant::now();

                    loop {
                        let process_exists = unsafe { libc::kill(pid_for_task as i32, 0) } == 0;

                        if !process_exists {
                            info!("Process {} has exited after signal", pid_for_task);
                            break;
                        }

                        if start.elapsed() > shutdown_timeout {
                            error!("Timeout waiting for process {} to exit, killing...", pid_for_task);

                            // Try to kill the process forcefully
                            if let Err(err) = signal::kill(pid, Signal::SIGKILL) {
                                error!("Failed to kill process {}: {}", pid_for_task, err);
                            }

                            // As a last resort, try to kill the child process (unshare)
                            if let Err(err) = child.kill().await {
                                error!("Failed to kill unshare process: {}", err);
                            }

                            break;
                        }

                        tokio::time::sleep(check_interval).await;
                    }

                    // Still wait for the child to avoid zombie processes
                    if let Err(err) = child.wait().await {
                        error!("Failed to wait for unshare process: {}", err);
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

        // Create the IsolatedProcess instance
        let process = IsolatedProcess {
            cgroups_controller,
            pid,
            shutdown_token,
            task_tracker,
            veth_pair,
        };

        // We might need to compensate for the network setup sleep before starting the readiness checks
        let compensation = Duration::from_secs(5) - (std::time::Instant::now() - start_time);

        if compensation > Duration::from_millis(0) {
            debug!(
                "Sleeping for {}ms before starting readiness checks",
                compensation.as_millis()
            );
            tokio::time::sleep(compensation).await;
        }

        // Perform readiness checks
        let interval = Duration::from_millis(options.application.is_ready_check_interval_ms());
        let max_attempts = options.application.is_ready_check_max();
        let mut attempts = 0;

        debug!(
            "Starting readiness checks with interval: {}ms, max attempts: {:?}",
            interval.as_millis(),
            max_attempts
        );

        loop {
            // Check if process has exited while we were waiting
            if unsafe { libc::kill(pid as i32, 0) } != 0 {
                return Err(Error::ReadinessCheck(
                    "Process exited during readiness checks".to_string(),
                ));
            }

            match options.application.is_ready_check(&process).await {
                Ok(true) => {
                    debug!("Application reported ready after {} attempts", attempts + 1);
                    break;
                }
                Ok(false) => {
                    attempts += 1;
                    if let Some(max) = max_attempts {
                        if attempts >= max {
                            return Err(Error::ReadinessCheck(format!(
                                "Application not ready after {} attempts",
                                attempts
                            )));
                        }
                    }
                    debug!("Application not ready, attempt {}, waiting...", attempts);
                    tokio::time::sleep(interval).await;
                }
                Err(e) => {
                    return Err(Error::ReadinessCheck(e.to_string()));
                }
            }
        }

        Ok((process, join_handle))
    }
}

/// Spawn an isolated process with the given options.
///
/// # Errors
///
/// Returns an error if the process could not be spawned.
pub async fn spawn_process(
    options: IsolatedProcessOptions,
) -> Result<(IsolatedProcess, JoinHandle<()>)> {
    let mut spawner = IsolatedProcessSpawner::new(options);
    spawner.spawn().await
}
