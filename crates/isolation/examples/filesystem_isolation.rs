//! Example of filesystem isolation using chroot.
//!
//! This example demonstrates:
//! 1. Creating a minimal filesystem with only allowed files
//! 2. Using chroot to restrict file access
//! 3. Verifying that the process cannot access files outside the chroot
//!
//! Note: This example requires root privileges to use chroot.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use proven_isolation::{
    Error, IsolatedApplication, IsolatedProcess, IsolationConfig, IsolationManager,
    NamespaceOptions, Result,
};
use tokio::fs;
use tracing::info;

/// Application that attempts to access files from an isolated environment
struct FilesystemTest {
    /// The chroot directory
    chroot_dir: PathBuf,
}

impl FilesystemTest {
    /// Create a new filesystem test
    async fn new(chroot_dir: &Path) -> Result<Self> {
        // Create a minimal filesystem inside the chroot

        // 1. Create required directories
        let bin_dir = chroot_dir.join("bin");
        fs::create_dir_all(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        let lib_dir = chroot_dir.join("lib");
        fs::create_dir_all(&lib_dir)
            .await
            .map_err(|e| Error::Io("Failed to create lib directory", e))?;

        let lib64_dir = chroot_dir.join("lib64");
        fs::create_dir_all(&lib64_dir)
            .await
            .map_err(|e| Error::Io("Failed to create lib64 directory", e))?;

        // 2. Create an allowed file that should be accessible
        let allowed_file = chroot_dir.join("allowed.txt");
        fs::write(
            &allowed_file,
            "This file should be accessible from the isolated environment",
        )
        .await
        .map_err(|e| Error::Io("Failed to create allowed file", e))?;

        // Use the externally defined C test program
        let test_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/filesystem_isolation/test.c");

        // Compile the test program statically to avoid library dependencies
        let output = tokio::process::Command::new("gcc")
            .arg("-static")
            .arg("-o")
            .arg(bin_dir.join("test_program"))
            .arg(&test_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile test program", e))?;

        if !output.status.success() {
            return Err(Error::Io(
                "Failed to compile test program",
                std::io::Error::new(std::io::ErrorKind::Other, "Compilation failed"),
            ));
        }

        // Make the program executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let test_program_path = bin_dir.join("test_program");
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
        println!("Contents of chroot directory:");
        let mut entries = fs::read_dir(chroot_dir)
            .await
            .map_err(|e| Error::Io("Failed to read chroot directory", e))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Io("Failed to read directory entry", e))?
        {
            println!("- {}", entry.path().display());
        }

        println!("Contents of chroot/bin directory:");
        let mut bin_entries = fs::read_dir(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to read bin directory", e))?;

        while let Some(entry) = bin_entries
            .next_entry()
            .await
            .map_err(|e| Error::Io("Failed to read directory entry", e))?
        {
            println!("- {}", entry.path().display());
        }

        Ok(Self {
            chroot_dir: chroot_dir.to_path_buf(),
        })
    }
}

#[async_trait]
impl IsolatedApplication for FilesystemTest {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        "/bin/test_program"
    }

    fn name(&self) -> &str {
        "filesystem-test"
    }

    fn chroot_dir(&self) -> Option<PathBuf> {
        Some(self.chroot_dir.clone())
    }

    fn namespace_options(&self) -> NamespaceOptions {
        // Use a comprehensive set of namespaces for isolation
        let mut options = NamespaceOptions::all();
        // Ensure mount namespace is used
        options.use_mount = true;
        options
    }

    async fn is_ready_check(&self, process: &IsolatedProcess) -> Result<bool> {
        // Consider it ready if the process is running
        Ok(process.pid().is_some())
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

    info!("Starting filesystem isolation example");

    // Set up temporary directories
    let temp_dir =
        tempfile::tempdir().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

    let chroot_dir = temp_dir.path().join("chroot");

    info!("Chroot directory: {}", chroot_dir.display());

    // Create the filesystem test
    let test = FilesystemTest::new(&chroot_dir).await?;

    // Configure the isolation manager with chroot enabled
    let config = IsolationConfig {
        use_chroot: true,
        use_ipc_namespace: true,
        use_memory_limits: false,
        use_mount_namespace: true,
        use_network_namespace: true,
        use_pid_namespace: true,
        use_user_namespace: true,
        use_uts_namespace: true,
    };

    info!("Running with isolation config: {:?}", config);

    let manager = IsolationManager::with_config(config);

    // Check if running as root, which is required for chroot
    let uid = unsafe { libc::getuid() };
    if uid != 0 {
        println!(
            "Warning: Not running as root (UID: {}). Chroot operations may fail.",
            uid
        );
        println!("Try running with sudo: sudo cargo run --example filesystem_isolation");
    } else {
        println!("Running as root (UID: 0), chroot operations should succeed.");
    }

    // Debug the file system setup
    let output = tokio::process::Command::new("ls")
        .arg("-la")
        .arg(&chroot_dir)
        .output()
        .await
        .map_err(|e| Error::Io("Failed to list chroot directory", e))?;

    println!("Detailed chroot directory listing:");
    println!("{}", String::from_utf8_lossy(&output.stdout));

    let output = tokio::process::Command::new("file")
        .arg(chroot_dir.join("bin/test_program"))
        .output()
        .await
        .map_err(|e| Error::Io("Failed to get file information", e))?;

    println!("Binary file information:");
    println!("{}", String::from_utf8_lossy(&output.stdout));

    // Spawn the isolated process with verbose logging
    info!("Spawning isolated process...");

    let process = manager.spawn(test).await?;

    // Process is now running
    info!("Process is running with PID: {:?}", process.pid());

    // Wait for the process to exit
    info!("Waiting for process to complete...");
    let status = process.wait().await?;
    info!("Process exited with status: {}", status);

    // Clean up temporary directory
    drop(temp_dir);

    Ok(())
}
