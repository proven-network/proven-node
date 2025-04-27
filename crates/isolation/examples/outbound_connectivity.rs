//! Example showing how to enable outbound network connectivity for isolated processes.
//!
//! This example demonstrates:
//! 1. Starting an isolated process that makes outbound HTTP requests
//! 2. Configuring network namespace to allow outbound connectivity
//! 3. Verifying that the isolated process can successfully connect to external sites

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use proven_isolation::{Error, IsolatedApplication, Result};
use tokio::fs;
use tokio::time::Duration;
use tracing::{debug, info};

/// Application that makes an outbound HTTP request to example.com
struct OutboundConnectivityTest {
    /// The executable path
    executable_path: PathBuf,
}

impl OutboundConnectivityTest {
    /// Create a new outbound connectivity test application
    async fn new(root_dir: &Path) -> Result<Self> {
        // Create a minimal filesystem inside the root directory
        let bin_dir = root_dir.join("bin");
        fs::create_dir_all(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        // Get the path to the C source file
        let test_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/outbound_connectivity/test.c");

        debug!(
            "Compiling outbound connectivity test from {}",
            test_c_path.display()
        );

        // Compile the test program
        let output = tokio::process::Command::new("gcc")
            .arg("-o")
            .arg(bin_dir.join("connectivity_test"))
            .arg(&test_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile test program", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Io(
                "Failed to compile test program",
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Compilation failed: {}", stderr),
                ),
            ));
        }

        // Make the program executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let test_path = bin_dir.join("connectivity_test");
            let mut perms = tokio::fs::metadata(&test_path)
                .await
                .map_err(|e| Error::Io("Failed to get test program metadata", e))?
                .permissions();

            perms.set_mode(0o755);
            tokio::fs::set_permissions(&test_path, perms)
                .await
                .map_err(|e| Error::Io("Failed to set test program permissions", e))?;
        }

        Ok(Self {
            executable_path: bin_dir.join("connectivity_test"),
        })
    }
}

#[async_trait]
impl IsolatedApplication for OutboundConnectivityTest {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        self.executable_path
            .to_str()
            .unwrap_or("/bin/connectivity_test")
    }

    fn name(&self) -> &str {
        "outbound-connectivity-test"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with defaults
    tracing_subscriber::fmt::init();

    info!("🚀 Starting outbound connectivity example");
    info!(
        "This example demonstrates running a process in isolation that can make outbound network requests"
    );

    // Set up temporary directory
    let temp_dir =
        tempfile::tempdir().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

    let root_dir = temp_dir.path().to_path_buf();
    info!("📁 Test root directory: {}", root_dir.display());

    // Create the test application
    let test_app = OutboundConnectivityTest::new(&root_dir).await?;

    info!("🔄 Spawning test process and waiting for it to become ready...");

    let process = proven_isolation::spawn(test_app).await?;

    // Wait a bit before shutting down
    info!("Waiting 5 seconds before shutdown");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shut down the process
    info!("🛑 Shutting down test process...");
    process.shutdown().await?;

    Ok(())
}
