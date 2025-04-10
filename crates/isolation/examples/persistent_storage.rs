//! Example demonstrating persistent storage between runs of an IsolatedApplication.
//!
//! This example shows how to:
//! 1. Set up a persistent storage directory that survives between runs
//! 2. Mount this directory into the isolated environment using volume mounts
//! 3. Read and write files that persist between runs
//!
//! The example creates a simple counter application that:
//! 1. Reads a counter value from a file
//! 2. Increments it
//! 3. Writes it back
//! 4. Shows how the value persists between runs

use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

use proven_isolation::{
    Error, IsolatedApplication, IsolationConfig, IsolationManager, NamespaceOptions, Result,
    VolumeMount,
};

/// A simple counter application that maintains state between runs
struct CounterApp {
    /// The path to the persistent storage directory
    storage_dir: PathBuf,
}

impl CounterApp {
    /// Creates a new counter application
    async fn new(storage_dir: PathBuf) -> Result<Self> {
        // Get the path to the C source file
        let counter_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/persistent_storage/counter.c");

        // Create root filesystem structure
        let root_dir = PathBuf::from("/tmp/counter-storage/root");
        fs::create_dir_all(&root_dir.join("bin"))
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        // Compile the counter program statically
        let counter_path = root_dir.join("bin/counter");
        let output = tokio::process::Command::new("gcc")
            .arg("-static")
            .arg("-o")
            .arg(&counter_path)
            .arg(&counter_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile counter program", e))?;

        if !output.status.success() {
            return Err(Error::Io(
                "Failed to compile counter program",
                std::io::Error::new(std::io::ErrorKind::Other, "Compilation failed"),
            ));
        }

        // Make the program executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = tokio::fs::metadata(&counter_path)
                .await
                .map_err(|e| Error::Io("Failed to get counter metadata", e))?
                .permissions();

            perms.set_mode(0o755);
            tokio::fs::set_permissions(&counter_path, perms)
                .await
                .map_err(|e| Error::Io("Failed to set counter permissions", e))?;
        }

        Ok(Self { storage_dir })
    }

    /// Parse a log line and forward to the appropriate tracing macro
    fn parse_log_line(&self, line: &str) {
        // Parse the log line based on its prefix
        if line.starts_with("[INFO]") {
            let content = line.trim_start_matches("[INFO]").trim();
            info!(target: "counter-app", "{}", content);
        } else if line.starts_with("[DEBUG]") {
            let content = line.trim_start_matches("[DEBUG]").trim();
            debug!(target: "counter-app", "{}", content);
        } else if line.starts_with("[WARN]") {
            let content = line.trim_start_matches("[WARN]").trim();
            warn!(target: "counter-app", "{}", content);
        } else if line.starts_with("[ERROR]") {
            let content = line.trim_start_matches("[ERROR]").trim();
            error!(target: "counter-app", "{}", content);
        } else {
            // For unrecognized log formats, just log as info
            info!(target: "counter-app", "{}", line);
        }
    }
}

#[async_trait]
impl IsolatedApplication for CounterApp {
    fn name(&self) -> &str {
        "counter-app"
    }

    fn executable(&self) -> &str {
        "/bin/counter"
    }

    fn args(&self) -> Vec<String> {
        Vec::new()
    }

    fn working_dir(&self) -> Option<PathBuf> {
        None
    }

    fn namespace_options(&self) -> NamespaceOptions {
        // Enable mount and user namespaces for proper mount permissions
        NamespaceOptions {
            use_mount: true,
            use_network: false,
            use_pid: false,
            use_ipc: false,
            use_uts: false,
            use_user: true, // Enable user namespace for mount permissions
        }
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        let data_path = PathBuf::from("/data");
        vec![
            // Mount the persistent data directory
            VolumeMount::new(&self.storage_dir, &data_path),
        ]
    }

    fn chroot_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from("/tmp/counter-storage/root"))
    }

    fn handle_stdout(&self, line: &str) {
        self.parse_log_line(line);
    }

    fn handle_stderr(&self, line: &str) {
        error!(target: "counter-app", "{}", line);
    }

    async fn prepare_config(&self) -> Result<()> {
        // Create the storage directory if it doesn't exist
        if !self.storage_dir.exists() {
            fs::create_dir_all(&self.storage_dir)
                .await
                .map_err(|e| Error::Io("Failed to create storage directory", e))?;
        }

        // Create the data directory inside the chroot environment
        let data_dir_in_chroot = PathBuf::from("/tmp/counter-storage/root/data");
        fs::create_dir_all(&data_dir_in_chroot)
            .await
            .map_err(|e| Error::Io("Failed to create data directory in chroot", e))?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt::init();

    // Create a persistent storage directory
    let storage_dir = PathBuf::from("/tmp/counter-storage/data");
    if !storage_dir.exists() {
        fs::create_dir_all(&storage_dir)
            .await
            .map_err(|e| Error::Io("Failed to create storage directory", e))?;
    }

    // Configure the isolation manager with mount and chroot enabled
    let config = IsolationConfig {
        use_chroot: true, // Enable chroot for filesystem isolation
        use_ipc_namespace: false,
        use_memory_limits: false,
        use_mount_namespace: true,
        use_network_namespace: false,
        use_pid_namespace: false,
        use_user_namespace: true, // Enable user namespace for mount permissions
        use_uts_namespace: false,
    };

    // Create the isolation manager with our config
    let manager = IsolationManager::with_config(config);

    // Run the counter application multiple times to demonstrate persistence
    for run in 1..=3 {
        info!("Run #{}", run);

        let app = CounterApp::new(storage_dir.clone()).await?;
        let (process, _join_handle) = manager.spawn(app).await?;

        // Wait for the process to exit
        process.wait().await?;

        // Clean up the process
        process.shutdown().await?;

        // Read the current counter value
        let counter_file = storage_dir.join("counter.txt");
        if counter_file.exists() {
            let value = fs::read_to_string(&counter_file)
                .await
                .map_err(|e| Error::Io("Failed to read counter file", e))?;
            info!("Counter value after run {}: {}", run, value.trim());
        }

        // Wait a bit between runs
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
