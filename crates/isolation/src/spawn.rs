//! Process spawning functionality for isolated applications.

use crate::ReadyCheckInfo;
use crate::cgroups::{CgroupMemoryConfig, CgroupsController};
use crate::error::Error;
use crate::namespaces::NamespaceOptions;
use crate::network::VethPair;
#[cfg(target_os = "linux")]
use crate::network::VethPairOptions;
#[cfg(target_os = "linux")]
use crate::network::check_root_permissions;
use crate::volume_mount::VolumeMount;

use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::net::IpAddr;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

/// Options for spawning an isolated process.
pub struct IsolatedProcessOptions {
    /// The arguments to pass to the executable.
    pub args: Vec<String>,

    /// Environment variables to set.
    pub env: HashMap<String, String>,

    /// The executable to run.
    pub executable: PathBuf,

    /// The IP address to use for the host.
    pub host_ip_address: IpAddr,

    /// The interface name for the host veth pair.
    pub host_veth_interface_name: String,

    /// Readiness check function
    pub is_ready_check: Box<
        dyn for<'a> FnMut(
                ReadyCheckInfo,
            )
                -> Pin<Box<dyn Future<Output = Result<bool, Box<dyn StdError>>> + Send>>
            + Send
            + Sync
            + 'static,
    >,

    /// How often to run readiness check in milliseconds
    pub is_ready_check_interval_ms: u64,

    /// Maximum number of readiness checks
    pub is_ready_check_max: Option<u32>,

    /// The IP address to use for the isolated application.
    pub isolated_ip_address: IpAddr,

    /// The interface name for the isolated veth pair.
    pub isolated_veth_interface_name: String,

    /// Memory control configuration using cgroups.
    pub memory_control: Option<CgroupMemoryConfig>,

    /// Namespace options.
    pub namespaces: NamespaceOptions,

    /// Handler for stderr lines
    pub stderr_handler: Box<dyn FnMut(&str) + Send + Sync + 'static>,

    /// Handler for stdout lines
    pub stdout_handler: Box<dyn FnMut(&str) + Send + Sync + 'static>,

    /// Signal to use for shutdown
    pub shutdown_signal: Signal,

    /// Timeout for graceful shutdown
    pub shutdown_timeout: Duration,

    /// TCP ports to forward
    pub tcp_ports: Vec<u16>,

    /// UDP ports to forward
    pub udp_ports: Vec<u16>,

    /// Volume mounts to make available to the process
    pub volume_mounts: Vec<VolumeMount>,

    /// The working directory for the process.
    pub working_dir: Option<PathBuf>,
}

/// Represents a running isolated process.
pub struct IsolatedProcess {
    /// Cgroups controller if active
    cgroups_controller: Option<CgroupsController>,

    /// Exit status of the process
    exit_status: Arc<Mutex<Option<ExitStatus>>>,

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

    /// Returns true if the process is running
    pub async fn running(&self) -> bool {
        self.exit_status.lock().await.is_none()
    }

    /// Waits for the process to exit
    pub async fn wait(&self) -> ExitStatus {
        let check_interval = Duration::from_millis(1000);

        // Loop until there's an exit status
        loop {
            let exit_status = self.exit_status.lock().await;

            if let Some(exit_status) = exit_status.as_ref() {
                return exit_status.clone();
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
    pub fn signal(&self, signal: Signal) -> Result<(), Error> {
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
    pub async fn shutdown(&self) -> Result<(), Error> {
        info!("Shutting down process");

        self.shutdown_token.cancel();
        self.wait().await;
        self.task_tracker.wait().await;

        info!("Process shut down");

        Ok(())
    }

    /// Returns the current memory usage of the process in bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the memory usage could not be retrieved.
    pub fn current_memory_usage(&self) -> Result<Option<usize>, Error> {
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
    pub fn current_memory_usage_mb(&self) -> Result<Option<f64>, Error> {
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
pub struct IsolatedProcessSpawner {
    /// The arguments to pass to the executable.
    pub args: Vec<String>,

    /// The root directory for chroot.
    pub chroot_dir: PathBuf,

    /// Environment variables to set.
    pub env: HashMap<String, String>,

    /// The executable to run.
    pub executable: PathBuf,

    /// The IP address to use for the host.
    pub host_ip_address: IpAddr,

    /// The interface name for the host veth pair.
    pub host_veth_interface_name: String,

    /// Readiness check function
    pub is_ready_check: Box<
        dyn for<'a> FnMut(
                ReadyCheckInfo,
            )
                -> Pin<Box<dyn Future<Output = Result<bool, Box<dyn StdError>>> + Send>>
            + Send
            + Sync
            + 'static,
    >,

    /// How often to run readiness check in milliseconds
    pub is_ready_check_interval_ms: u64,

    /// Maximum number of readiness checks
    pub is_ready_check_max: Option<u32>,

    /// The IP address to use for the isolated application.
    pub isolated_ip_address: IpAddr,

    /// The interface name for the isolated veth pair.
    pub isolated_veth_interface_name: String,

    /// Memory control configuration using cgroups.
    pub memory_control: Option<CgroupMemoryConfig>,

    /// Namespace options.
    pub namespaces: NamespaceOptions,

    /// Handler for stderr lines
    pub stderr_handler: Box<dyn FnMut(&str) + Send + Sync + 'static>,

    /// Handler for stdout lines
    pub stdout_handler: Box<dyn FnMut(&str) + Send + Sync + 'static>,

    /// Signal to use for shutdown
    pub shutdown_signal: Signal,

    /// Timeout for graceful shutdown
    pub shutdown_timeout: Duration,

    /// TCP ports to forward
    pub tcp_ports: Vec<u16>,

    /// UDP ports to forward
    pub udp_ports: Vec<u16>,

    /// The virtual ethernet pair for network communication between host and container
    pub veth_pair: Option<Arc<VethPair>>,

    /// Volume mounts to make available to the process
    pub volume_mounts: Vec<VolumeMount>,

    /// The working directory for the process.
    pub working_dir: Option<PathBuf>,
}

impl std::fmt::Debug for IsolatedProcessSpawner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsolatedProcessSpawner")
            .field("args", &self.args)
            .field("chroot_dir", &self.chroot_dir)
            .field("env", &self.env)
            .field("executable", &self.executable)
            .field("is_ready_check", &"<function>")
            .field(
                "is_ready_check_interval_ms",
                &self.is_ready_check_interval_ms,
            )
            .field("is_ready_check_max", &self.is_ready_check_max)
            .field("memory_control", &self.memory_control)
            .field("namespaces", &self.namespaces)
            .field("stderr_handler", &"<function>")
            .field("stdout_handler", &"<function>")
            .field("shutdown_signal", &self.shutdown_signal)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("tcp_ports", &self.tcp_ports)
            .field("udp_ports", &self.udp_ports)
            .field("veth_pair", &self.veth_pair)
            .field("volume_mounts", &self.volume_mounts)
            .field("working_dir", &self.working_dir)
            .finish()
    }
}

impl IsolatedProcessSpawner {
    /// Creates a new `IsolatedProcessSpawner`.
    #[must_use]
    pub fn new(
        IsolatedProcessOptions {
            args,
            env,
            executable,
            host_ip_address,
            host_veth_interface_name,
            is_ready_check,
            is_ready_check_interval_ms,
            is_ready_check_max,
            isolated_ip_address,
            isolated_veth_interface_name,
            memory_control,
            namespaces,
            shutdown_signal,
            shutdown_timeout,
            stdout_handler,
            stderr_handler,
            tcp_ports,
            udp_ports,
            volume_mounts,
            working_dir,
        }: IsolatedProcessOptions,
    ) -> Self {
        // Generate a random string for the chroot directory
        let chroot_dir = format!("/tmp/chroot/{}", uuid::Uuid::new_v4().to_string());

        Self {
            args,
            chroot_dir: PathBuf::from(chroot_dir),
            env,
            executable,
            host_ip_address,
            host_veth_interface_name,
            memory_control,
            namespaces,
            is_ready_check,
            is_ready_check_interval_ms,
            is_ready_check_max,
            isolated_ip_address,
            isolated_veth_interface_name,
            shutdown_signal,
            shutdown_timeout,
            stdout_handler,
            stderr_handler,
            tcp_ports,
            udp_ports,
            veth_pair: None,
            volume_mounts,
            working_dir,
        }
    }

    /// Build the arguments for the unshare command.
    ///
    /// This is only available on Linux.
    #[cfg(target_os = "linux")]
    fn build_unshare_args(&self) -> Result<Vec<String>, Error> {
        let mut args = Vec::new();

        // Add namespace arguments using the options.namespaces.to_unshare_args() helper
        args.extend(self.namespaces.to_unshare_args());

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
            self.chroot_dir.display(),
            self.chroot_dir.display(),
            self.chroot_dir.display(),
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Copy essential command-line utilities used by many applications
        setup_cmd.push_str(&format!("for cmd in tr xargs sh uname echo cat grep sed awk; do cp $(which $cmd) {}/bin/ || cp $(which $cmd) {}/usr/bin/ || true; done; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()));

        // Copy SSL certificates (needed by many applications for TLS)
        setup_cmd.push_str(&format!(
            "mkdir -p {}/etc/ssl/certs; cp -r /etc/ssl/certs/* {}/etc/ssl/certs/ || true; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Create /etc/nsswitch.conf for proper name resolution
        setup_cmd.push_str(&format!(
            "mkdir -p {}/etc; echo 'hosts: files dns' > {}/etc/nsswitch.conf; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Create mock /etc/passwd and /etc/group
        setup_cmd.push_str(&format!(
            "echo 'root:x:0:0:root:/usr/sbin/nologin' > {}/etc/passwd; ",
            self.chroot_dir.display()
        ));

        // Create /etc/resolv.conf for DNS resolution inside chroot
        setup_cmd.push_str(&format!(
            "mkdir -p {}/etc; echo 'nameserver {}\noptions ndots:0' > {}/etc/resolv.conf; ",
            self.chroot_dir.display(),
            self.host_ip_address,
            self.chroot_dir.display()
        ));

        setup_cmd.push_str(&format!(
            "echo 'root:x:0:root' > {}/etc/group; ",
            self.chroot_dir.display()
        ));

        // Mount the host library directories as read-only
        setup_cmd.push_str(&format!(
            "mount --bind -o ro /lib {}/lib || true; ",
            self.chroot_dir.display()
        ));

        setup_cmd.push_str(&format!(
            "mount --bind -o ro /usr/lib {}/usr/lib || true; ",
            self.chroot_dir.display()
        ));

        // Mount /dev/null, /dev/zero, /dev/random, /dev/urandom
        setup_cmd.push_str(&format!("mkdir -p {}/dev; ", self.chroot_dir.display()));

        for dev in ["null", "zero", "random", "urandom"] {
            setup_cmd.push_str(&format!(
                "touch {0}/dev/{1}; mount --bind /dev/{1} {0}/dev/{1} || true; ",
                self.chroot_dir.display(),
                dev
            ));
        }

        // Create /dev/mqueue
        setup_cmd.push_str(&format!(
            "mkdir -p {}/dev/mqueue; mount -t mqueue none {}/dev/mqueue || true; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Create /dev/pts
        setup_cmd.push_str(&format!(
            "mkdir -p {}/dev/pts; mount -t devpts devpts {}/dev/pts || true; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Create tmpfs for /dev/shm
        // TODO: Make this configurable based on application needs
        setup_cmd.push_str(&format!(
            "mkdir -p {}/dev/shm; mount -t tmpfs -o size=256m tmpfs {}/dev/shm || true; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Create tmpfs for /tmp
        // TODO: Make this configurable based on application needs
        setup_cmd.push_str(&format!(
            "mkdir -p {}/tmp; mount -t tmpfs -o size=4g,mode=1777 tmpfs {}/tmp || true; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Create /proc
        setup_cmd.push_str(&format!(
            "mkdir -p {}/proc; mount -t proc proc {}/proc || true; ",
            self.chroot_dir.display(),
            self.chroot_dir.display()
        ));

        // Set up volume mounts
        for mount in &self.volume_mounts {
            // Strip the leading slash if present and join with chroot dir
            let rel_path = mount
                .container_path
                .strip_prefix("/")
                .unwrap_or(&mount.container_path);
            let container_path = self.chroot_dir.join(rel_path);

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
        for (key, value) in &self.env {
            setup_cmd.push_str(&format!("export {}=\"{}\"; ", key, value));
        }

        // Use the full path inside the chroot
        let exec_path = self.executable.to_string_lossy();

        // Execute the command inside the chroot (with working directory if specified)
        if let Some(ref working_dir) = self.working_dir {
            // Change to the working directory before executing the command
            setup_cmd.push_str(&format!(
                "exec /usr/sbin/chroot {} sh -c \"cd {} && exec {}{}\"",
                self.chroot_dir.display(),
                working_dir.display(),
                exec_path,
                if self.args.is_empty() {
                    "".to_string()
                } else {
                    format!(" {}", self.args.join(" "))
                }
            ));
        } else {
            // Execute the command directly
            setup_cmd.push_str(&format!(
                "exec /usr/sbin/chroot {} {}{}",
                self.chroot_dir.display(),
                exec_path,
                if self.args.is_empty() {
                    "".to_string()
                } else {
                    format!(" {}", self.args.join(" "))
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
    pub async fn spawn(&mut self) -> Result<(IsolatedProcess, JoinHandle<()>), Error> {
        let shutdown_token = CancellationToken::new();
        let task_tracker = TaskTracker::new();

        #[cfg(target_os = "linux")]
        let mut cmd = {
            // Prepare the command to be executed with appropriate isolation
            let unshare_args = self.build_unshare_args()?;

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
            let mut cmd = Command::new(&self.executable);
            cmd.args(&self.args);

            // Set the working directory if specified
            if let Some(ref working_dir) = self.working_dir {
                cmd.current_dir(working_dir);
            }

            // Set environment variables
            for (key, value) in &self.env {
                cmd.env(key, value);
            }

            cmd
        };

        // Setup stdio
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        debug!("Spawning process: {:?}", cmd);

        // Track the start time to compensate for the network setup sleep
        #[cfg(target_os = "linux")]
        let start_time = std::time::Instant::now();

        // Spawn the command
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Io("Failed to spawn process", e))?;

        // Get the process ID
        #[allow(unused_mut)]
        let mut pid = child.id().ok_or_else(|| {
            Error::SpawnProcess("No PID available for spawned process".to_string())
        })?;

        // If using PID namespace process will be forked so find process with `pgrep -P [PARENT_PID]`
        #[cfg(target_os = "linux")]
        if self.namespaces.use_pid {
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
        #[cfg(target_os = "linux")]
        if self.namespaces.use_network {
            // Check if we have root permissions for network setup
            if !check_root_permissions()? {
                warn!("Network namespaces require root permissions for port forwarding setup");
            } else {
                // Set up the veth pair with port lists
                match VethPair::new(VethPairOptions {
                    host_ip_address: self.host_ip_address,
                    host_veth_interface_name: self.host_veth_interface_name.clone(),
                    isolated_ip_address: self.isolated_ip_address,
                    isolated_pid: pid,
                    isolated_veth_interface_name: self.isolated_veth_interface_name.clone(),
                    tcp_port_forwards: self.tcp_ports.clone(),
                    udp_port_forwards: self.udp_ports.clone(),
                })
                .await
                {
                    Ok(veth) => {
                        let veth = Arc::new(veth);
                        self.veth_pair = Some(Arc::clone(&veth));
                    }
                    Err(e) => {
                        warn!("Failed to set up network: {}", e);
                    }
                }
            }
        }

        // Take stdout and stderr before they're moved into closures
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        // Start a task to handle stdout from the child process
        if let Some(stdout) = stdout {
            let mut stdout_handler = std::mem::replace(&mut self.stdout_handler, Box::new(|_| {}));
            task_tracker.spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    stdout_handler(&line);
                }
            });
        }

        // Start a task to handle stderr from the child process
        if let Some(stderr) = stderr {
            let mut stderr_handler = std::mem::replace(&mut self.stderr_handler, Box::new(|_| {}));
            task_tracker.spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    stderr_handler(&line);
                }
            });
        }

        // Exit status wrapped in a mutex to share between task and main thread
        let exit_status = Arc::new(Mutex::new(None));

        // Monitor child process
        let pid_for_task = pid;
        let shutdown_signal = self.shutdown_signal;
        let shutdown_timeout = self.shutdown_timeout;
        let shutdown_token_for_task = shutdown_token.clone();
        let exit_status_for_task = exit_status.clone();
        #[cfg(target_os = "linux")]
        let chroot_dir = self.chroot_dir.clone();

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

                            *exit_status_for_task.lock().await = Some(status);
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

                            // Set the exit status
                            *exit_status_for_task.lock().await = Some(ExitStatus::from_raw(
                                libc::CLD_KILLED,
                            ));

                            break;
                        }

                        tokio::time::sleep(check_interval).await;
                    }

                    // Still wait for the child to avoid zombie processes
                    if let Err(err) = child.wait().await {
                        error!("Failed to wait for unshare process: {}", err);
                    }

                    // Remove the chroot directory
                    #[cfg(target_os = "linux")]
                    if let Err(err) = std::fs::remove_dir_all(&chroot_dir) {
                        error!("Failed to remove chroot directory: {}", err);
                    }
                }
            }
        });

        task_tracker.close();

        // Create a cgroups controller if memory control is configured
        let cgroups_controller = if let Some(ref memory_config) = self.memory_control {
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
            exit_status,
            pid,
            shutdown_token,
            task_tracker,
            veth_pair: self.veth_pair.clone(),
        };

        // We might need to compensate for the network setup sleep before starting the readiness checks
        #[cfg(target_os = "linux")]
        let compensation = Duration::from_secs(5) - (std::time::Instant::now() - start_time);

        #[cfg(target_os = "linux")]
        if compensation > Duration::from_millis(0) {
            debug!(
                "Sleeping for {}ms before starting readiness checks",
                compensation.as_millis()
            );
            tokio::time::sleep(compensation).await;
        }

        // Perform readiness checks
        let interval = Duration::from_millis(self.is_ready_check_interval_ms);
        let max_attempts = self.is_ready_check_max;
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

            // Wait for container IP if using network namespace
            #[cfg(target_os = "linux")]
            let ready_check_info = if self.namespaces.use_network {
                match self.veth_pair.as_ref().map(|v| v.container_ip()) {
                    Some(ip) => ReadyCheckInfo {
                        ip_address: ip,
                        pid,
                    },
                    None => {
                        attempts += 1;
                        if let Some(max) = max_attempts {
                            if attempts >= max {
                                return Err(Error::ReadinessCheck(
                                    "Container IP not available after max attempts".to_string(),
                                ));
                            }
                        }
                        debug!(
                            "Container IP not available yet, attempt {}, waiting...",
                            attempts
                        );
                        tokio::time::sleep(interval).await;
                        continue;
                    }
                }
            } else {
                // If not using network namespace, use localhost
                ReadyCheckInfo {
                    ip_address: std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                    pid,
                }
            };

            #[cfg(not(target_os = "linux"))]
            let ready_check_info = ReadyCheckInfo {
                ip_address: std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                pid,
            };

            match (self.is_ready_check)(ready_check_info).await {
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
