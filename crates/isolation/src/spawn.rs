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
    pub chroot_dir: PathBuf,

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
        // Generate a random string for the chroot directory
        let chroot_dir = format!("/tmp/chroot/{}", uuid::Uuid::new_v4().to_string());

        Self {
            application,
            args: args
                .into_iter()
                .map(|a| a.as_ref().to_string_lossy().to_string())
                .collect(),
            chroot_dir: PathBuf::from(chroot_dir),
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

        // Create basic directory structure
        setup_cmd.push_str(&format!(
            "mkdir -p {}/bin {}/lib {}/usr/bin {}/usr/lib {}/tmp; ",
            options.chroot_dir.display(),
            options.chroot_dir.display(),
            options.chroot_dir.display(),
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Copy essential command-line utilities used by many applications
        setup_cmd.push_str(&format!("for cmd in tr xargs sh uname echo cat grep sed awk; do cp $(which $cmd) {}/bin/ || cp $(which $cmd) {}/usr/bin/ || true; done; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()));

        // Copy SSL certificates (needed by many applications for TLS)
        setup_cmd.push_str(&format!(
            "mkdir -p {}/etc/ssl/certs; cp -r /etc/ssl/certs/* {}/etc/ssl/certs/ || true; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Create /etc/nsswitch.conf for proper name resolution
        setup_cmd.push_str(&format!(
            "mkdir -p {}/etc; echo 'hosts: files dns' > {}/etc/nsswitch.conf; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Mount the host library directories as read-only
        setup_cmd.push_str(&format!(
            "mount --bind -o ro /lib {}/lib || true; ",
            options.chroot_dir.display()
        ));

        setup_cmd.push_str(&format!(
            "mount --bind -o ro /usr/lib {}/usr/lib || true; ",
            options.chroot_dir.display()
        ));

        // Mount /dev/null, /dev/zero, /dev/random, /dev/urandom
        setup_cmd.push_str(&format!("mkdir -p {}/dev; ", options.chroot_dir.display()));

        for dev in ["null", "zero", "random", "urandom"] {
            setup_cmd.push_str(&format!(
                "touch {0}/dev/{1}; mount --bind /dev/{1} {0}/dev/{1} || true; ",
                options.chroot_dir.display(),
                dev
            ));
        }

        // Create /dev/mqueue
        setup_cmd.push_str(&format!(
            "mkdir -p {}/dev/mqueue; mount -t mqueue none {}/dev/mqueue || true; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Create /dev/pts
        setup_cmd.push_str(&format!(
            "mkdir -p {}/dev/pts; mount -t devpts devpts {}/dev/pts || true; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Create tmpfs for /dev/shm
        // TODO: Make this configurable based on application needs
        setup_cmd.push_str(&format!(
            "mkdir -p {}/dev/shm; mount -t tmpfs -o size=256m tmpfs {}/dev/shm || true; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Create tmpfs for /tmp
        // TODO: Make this configurable based on application needs
        setup_cmd.push_str(&format!(
            "mkdir -p {}/tmp; mount -t tmpfs -o size=4g,mode=1777 tmpfs {}/tmp || true; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Create /proc
        setup_cmd.push_str(&format!(
            "mkdir -p {}/proc; mount -t proc proc {}/proc || true; ",
            options.chroot_dir.display(),
            options.chroot_dir.display()
        ));

        // Set up volume mounts
        for mount in &options.volume_mounts {
            // Strip the leading slash if present and join with chroot dir
            let rel_path = mount
                .container_path
                .strip_prefix("/")
                .unwrap_or(&mount.container_path);
            let container_path = options.chroot_dir.join(rel_path);

            // Create mount point directories
            setup_cmd.push_str(&format!("mkdir -p {}; ", container_path.display()));

            // Mount the volume
            if mount.read_only {
                setup_cmd.push_str(&format!(
                    "mount --bind -o ro {} {} || true; ",
                    mount.host_path.display(),
                    container_path.display()
                ));
            } else {
                setup_cmd.push_str(&format!(
                    "mount --bind {} {} || true; ",
                    mount.host_path.display(),
                    container_path.display()
                ));
            }
        }

        // Set env variables
        for (key, value) in &options.env {
            setup_cmd.push_str(&format!("export {}=\"{}\"; ", key, value));
        }

        // Use the full path inside the chroot
        let exec_path = options.executable.to_string_lossy();

        // Execute the command inside the chroot (with working directory if specified)
        if let Some(ref working_dir) = options.working_dir {
            // Change to the working directory before executing the command
            setup_cmd.push_str(&format!(
                "exec /usr/sbin/chroot {} sh -c \"cd {} && exec {}{}\"",
                options.chroot_dir.display(),
                working_dir.display(),
                exec_path,
                if options.args.is_empty() {
                    "".to_string()
                } else {
                    format!(" {}", options.args.join(" "))
                }
            ));
        } else {
            // Execute the command directly
            setup_cmd.push_str(&format!(
                "exec /usr/sbin/chroot {} {}{}",
                options.chroot_dir.display(),
                exec_path,
                if options.args.is_empty() {
                    "".to_string()
                } else {
                    format!(" {}", options.args.join(" "))
                }
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
            debug!("Finding process with pgrep -P {}", pid);

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
                        debug!(
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

        // Create resolv.conf for DNS resolution inside chroot if network namespace is used
        if let Some(veth) = veth_pair.as_ref() {
            let etc_dir = options.chroot_dir.join("etc");
            let resolv_conf_path = etc_dir.join("resolv.conf");

            // Create /etc directory if it doesn't exist
            if let Err(e) = std::fs::create_dir_all(&etc_dir) {
                warn!("Failed to create /etc directory in chroot: {}", e);
            } else {
                // Point to the host's veth IP - DNS requests will be forwarded to actual DNS servers via iptables
                let resolv_conf_content = format!("nameserver {}\n", veth.host_ip());
                if let Err(e) = std::fs::write(&resolv_conf_path, resolv_conf_content) {
                    warn!("Failed to write resolv.conf in chroot: {}", e);
                } else {
                    debug!(
                        "Created resolv.conf in chroot pointing to {} (DNS queries will be forwarded to host's DNS servers)",
                        veth.host_ip()
                    );
                }
            }
        }

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

                    // Remove the chroot directory
                    if let Err(err) = std::fs::remove_dir_all(&options.chroot_dir) {
                        error!("Failed to remove chroot directory: {}", err);
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
