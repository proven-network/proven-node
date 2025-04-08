//! Example of memory isolation using cgroups.
//!
//! This example demonstrates:
//! 1. Setting strict memory limits for isolated processes
//! 2. Monitoring memory usage in real-time
//! 3. Handling out-of-memory scenarios
//!
//! Note: This example requires Linux with cgroups v2 support.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use proven_isolation::{
    Error, IsolatedApplication, IsolatedProcess, IsolationConfig, IsolationManager, Result,
};
use tokio::fs;
use tokio::time::interval;
use tracing::info;

/// Memory limit in MB
const MEMORY_LIMIT_MB: usize = 20;

/// Application that intentionally uses increasing amounts of memory
struct MemoryStressTest {
    /// Path to the test program
    executable_path: PathBuf,
    /// Directory to work in
    work_dir: PathBuf,
}

impl MemoryStressTest {
    /// Create a new memory stress test
    async fn new<P: AsRef<std::path::Path>>(work_dir: P) -> Result<Self> {
        let work_dir = work_dir.as_ref().to_path_buf();

        // Create the work directory
        tokio::fs::create_dir_all(&work_dir)
            .await
            .map_err(|e| Error::Io("Failed to create work directory", e))?;

        // Create the bin directory
        let bin_dir = work_dir.join("bin");
        fs::create_dir_all(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        // Use the C program for memory stress testing
        let test_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/memory_isolation/test.c");

        // Compile the test program
        let output = tokio::process::Command::new("gcc")
            .arg("-o")
            .arg(bin_dir.join("memory_test"))
            .arg(&test_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile memory test program", e))?;

        if !output.status.success() {
            return Err(Error::Io(
                "Failed to compile memory test program",
                std::io::Error::new(std::io::ErrorKind::Other, "Compilation failed"),
            ));
        }

        // Make the program executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let test_program_path = bin_dir.join("memory_test");
            let mut perms = tokio::fs::metadata(&test_program_path)
                .await
                .map_err(|e| Error::Io("Failed to get test program metadata", e))?
                .permissions();

            perms.set_mode(0o755);
            tokio::fs::set_permissions(&test_program_path, perms)
                .await
                .map_err(|e| Error::Io("Failed to set test program permissions", e))?;
        }

        // Debug the file system structure
        info!("Contents of work directory:");
        let mut entries = fs::read_dir(&work_dir)
            .await
            .map_err(|e| Error::Io("Failed to read work directory", e))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Io("Failed to read directory entry", e))?
        {
            info!("- {}", entry.path().display());
        }

        info!("Contents of bin directory:");
        let mut bin_entries = fs::read_dir(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to read bin directory", e))?;

        while let Some(entry) = bin_entries
            .next_entry()
            .await
            .map_err(|e| Error::Io("Failed to read directory entry", e))?
        {
            info!("- {}", entry.path().display());
        }

        Ok(Self {
            executable_path: bin_dir.join("memory_test"),
            work_dir,
        })
    }
}

#[async_trait]
impl IsolatedApplication for MemoryStressTest {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        self.executable_path.to_str().unwrap()
    }

    fn name(&self) -> &str {
        "memory-stress-test"
    }

    fn working_dir(&self) -> Option<PathBuf> {
        Some(self.work_dir.clone())
    }

    fn memory_limit_mb(&self) -> usize {
        MEMORY_LIMIT_MB
    }

    fn memory_min_mb(&self) -> usize {
        5 // Guarantee at least 5MB
    }

    async fn is_ready_check(&self, _process: &IsolatedProcess) -> Result<bool> {
        Ok(true)
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        100 // Check quickly
    }

    fn is_ready_check_max(&self) -> Option<u32> {
        Some(1) // One check is enough
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with defaults
    tracing_subscriber::fmt().init();

    // Set up temporary directory
    let temp_dir =
        tempfile::tempdir().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

    let work_dir = temp_dir.path().to_path_buf();

    info!("Starting memory isolation example");
    info!("Work directory: {}", work_dir.display());
    info!("Memory limit: {}MB", MEMORY_LIMIT_MB);

    // Create the memory stress test
    let test = MemoryStressTest::new(&work_dir).await?;

    // Configure the isolation manager with memory limits
    let config = IsolationConfig {
        use_chroot: false,
        use_ipc_namespace: true,
        use_memory_limits: true,
        use_mount_namespace: true,
        use_network_namespace: false,
        use_pid_namespace: true,
        use_user_namespace: true,
        use_uts_namespace: true,
    };

    info!("Running with isolation config: {:?}", config);

    let manager = IsolationManager::with_config(config);

    // Check if memory control is likely to be available
    info!(
        "Memory limits are enabled in config, attempting to enforce {}MB limit",
        MEMORY_LIMIT_MB
    );

    // Spawn the isolated process
    info!("Spawning isolated process...");
    let (process, _join_handle) = manager.spawn(test).await?;

    info!("Process is running with PID: {}", process.pid());

    // Set up for concurrent monitoring and waiting
    let mut interval = interval(Duration::from_secs(1));
    let start_time = std::time::Instant::now();
    let mut i = 0;

    // Print the monitoring header
    info!(
        "Starting memory usage monitoring (limit: {}MB)",
        MEMORY_LIMIT_MB
    );
    info!("{:=^50}", " MEMORY MONITOR ");
    info!("Time (s) | Usage (MB) | % of Limit");
    info!("{:-^50}", "");

    // Loop to monitor memory and check for process exit
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Check memory usage
                if let Ok(Some(usage_mb)) = process.current_memory_usage_mb() {
                    let percent = (usage_mb / MEMORY_LIMIT_MB as f64) * 100.0;
                    info!("{:7} | {:9.2} | {:9.1}%", i, usage_mb, percent);
                } else {
                    info!("{:7} | {:9} | {:9}", i, "N/A", "N/A");
                }
                i += 1;

                // Try to check if process is still running using the PID
                // Use kill with signal 0 to check if process exists without sending a signal
                let exists = unsafe {
                    libc::kill(process.pid() as libc::pid_t, 0) == 0
                };

                if !exists {
                    info!("Process {} no longer exists, stopping monitoring", process.pid());
                    break;
                }
            }
            status = process.wait() => {
                // Process has exited
                info!("Process has exited, stopping monitoring");
                match status {
                    Ok(exit_status) => info!("Process exited with status: {}", exit_status),
                    Err(e) => info!("Error waiting for process: {}", e),
                }
                break;
            }
        }
    }

    // Print monitoring footer
    info!("{:=^50}", "");
    info!(
        "Memory monitoring completed after {} seconds",
        start_time.elapsed().as_secs()
    );

    Ok(())
}
