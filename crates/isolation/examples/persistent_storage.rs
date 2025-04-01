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

use proven_isolation::{
    Error, IsolatedApplication, IsolationConfig, IsolationManager, NamespaceOptions, Result,
    VolumeMount,
};

/// A simple counter application that maintains state between runs
struct CounterApp {
    /// The path to the persistent storage directory
    storage_dir: PathBuf,
    /// The path to the counter executable
    executable_path: PathBuf,
}

impl CounterApp {
    /// Creates a new counter application
    async fn new(storage_dir: PathBuf) -> Result<Self> {
        // Get the path to the C source file
        let counter_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/persistent_storage/counter.c");

        // Compile the counter daemon
        let output = tokio::process::Command::new("gcc")
            .arg("-o")
            .arg("/tmp/counter-storage/bin/counter")
            .arg(&counter_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile counter daemon", e))?;

        if !output.status.success() {
            return Err(Error::Io(
                "Failed to compile counter daemon",
                std::io::Error::new(std::io::ErrorKind::Other, "Compilation failed"),
            ));
        }

        // Make the program executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let counter_path = PathBuf::from("/tmp/counter-storage/bin/counter");
            let mut perms = tokio::fs::metadata(&counter_path)
                .await
                .map_err(|e| Error::Io("Failed to get counter metadata", e))?
                .permissions();

            perms.set_mode(0o755);
            tokio::fs::set_permissions(&counter_path, perms)
                .await
                .map_err(|e| Error::Io("Failed to set counter permissions", e))?;
        }

        Ok(Self {
            storage_dir,
            executable_path: PathBuf::from("/tmp/counter-storage/bin/counter"),
        })
    }
}

#[async_trait]
impl IsolatedApplication for CounterApp {
    fn name(&self) -> &str {
        "counter-app"
    }

    fn executable(&self) -> &str {
        self.executable_path.to_str().unwrap()
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
        vec![VolumeMount::new(&self.storage_dir, &PathBuf::from("/data"))]
    }

    fn chroot_dir(&self) -> Option<PathBuf> {
        None // No chroot needed
    }

    async fn prepare_config(&self) -> Result<()> {
        // Create the storage directory if it doesn't exist
        if !self.storage_dir.exists() {
            fs::create_dir_all(&self.storage_dir)
                .await
                .map_err(|e| Error::Io("Failed to create storage directory", e))?;
        }

        // Create the bin directory if it doesn't exist
        let bin_dir = PathBuf::from("/tmp/counter-storage/bin");
        fs::create_dir_all(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        // Create the /data directory in the container
        let data_dir = PathBuf::from("/data");
        fs::create_dir_all(&data_dir)
            .await
            .map_err(|e| Error::Io("Failed to create data directory", e))?;

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

    // Configure the isolation manager with mount namespace enabled
    let config = IsolationConfig {
        use_chroot: false,
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
        println!("\nRun #{}", run);

        let app = CounterApp::new(storage_dir.clone()).await?;
        let process = manager.spawn(app).await?;

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
            println!("Counter value after run {}: {}", run, value.trim());
        }

        // Wait a bit between runs
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
