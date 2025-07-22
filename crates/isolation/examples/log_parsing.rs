//! Example of parsing logs from isolated applications.
//!
//! This example demonstrates:
//! 1. Capturing stdout/stderr from an application
//! 2. Parsing log messages with different log levels
//! 3. Forwarding logs to appropriate tracing macros
//!
//! This can be useful for integrating legacy applications with modern
//! structured logging systems.

use std::path::PathBuf;

use async_trait::async_trait;
use proven_isolation::{Error, IsolatedApplication, Result};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{Level, debug, error, info, trace, warn};
use tracing_subscriber::FmtSubscriber;

/// A simple application that emits logs at different levels
#[derive(Clone)]
struct LoggerApp {
    /// Path to the executable
    executable_path: PathBuf,
    /// Directory to work in
    work_dir: PathBuf,
}

impl LoggerApp {
    /// Create a new logger application
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

        // Use the C program for log generation
        let logger_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/log_parsing/logger.c");

        // Compile the program
        let output = tokio::process::Command::new("gcc")
            .arg("-o")
            .arg(bin_dir.join("logger"))
            .arg(&logger_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile logger program", e))?;

        if !output.status.success() {
            return Err(Error::Io(
                "Failed to compile logger program",
                std::io::Error::other("Compilation failed"),
            ));
        }

        // Make the program executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let program_path = bin_dir.join("logger");
            let mut perms = tokio::fs::metadata(&program_path)
                .await
                .map_err(|e| Error::Io("Failed to get program metadata", e))?
                .permissions();

            perms.set_mode(0o755);
            tokio::fs::set_permissions(&program_path, perms)
                .await
                .map_err(|e| Error::Io("Failed to set program permissions", e))?;
        }

        Ok(Self {
            executable_path: bin_dir.join("logger"),
            work_dir,
        })
    }

    /// Parse a log line and forward to the appropriate tracing macro
    fn parse_log_line(&self, line: &str) {
        // Parse the log line based on its prefix and strip out the datetime portion
        if line.starts_with("INFO:") {
            let content = line.trim_start_matches("INFO:").trim();
            // Extract only the message portion after the timestamp
            if let Some(msg_start) = content.find(']') {
                info!(target: "isolated_app", "{}", content[msg_start+1..].trim());
            } else {
                info!(target: "isolated_app", "{}", content);
            }
        } else if line.starts_with("DEBUG:") {
            let content = line.trim_start_matches("DEBUG:").trim();
            if let Some(msg_start) = content.find(']') {
                debug!(target: "isolated_app", "{}", content[msg_start+1..].trim());
            } else {
                debug!(target: "isolated_app", "{}", content);
            }
        } else if line.starts_with("WARN:") {
            let content = line.trim_start_matches("WARN:").trim();
            if let Some(msg_start) = content.find(']') {
                warn!(target: "isolated_app", "{}", content[msg_start+1..].trim());
            } else {
                warn!(target: "isolated_app", "{}", content);
            }
        } else if line.starts_with("ERROR:") {
            let content = line.trim_start_matches("ERROR:").trim();
            if let Some(msg_start) = content.find(']') {
                error!(target: "isolated_app", "{}", content[msg_start+1..].trim());
            } else {
                error!(target: "isolated_app", "{}", content);
            }
        } else if line.starts_with("TRACE:") {
            let content = line.trim_start_matches("TRACE:").trim();
            if let Some(msg_start) = content.find(']') {
                trace!(target: "isolated_app", "{}", content[msg_start+1..].trim());
            } else {
                trace!(target: "isolated_app", "{}", content);
            }
        } else {
            // For unrecognized log formats, just log as info
            info!(target: "isolated_app", "{}", line);
        }
    }

    /// Handle stderr line
    fn parse_stderr_line(&self, line: &str) {
        // For stderr, we'll also try to extract the actual message without timestamp
        if let Some(bracket_pos) = line.find('[')
            && let Some(end_bracket_pos) = line.find(']')
            && bracket_pos < end_bracket_pos
        {
            warn!(target: "isolated_app_err", "{}", line[end_bracket_pos+1..].trim());
            return;
        }

        // If we couldn't parse it specially, just log the whole line
        warn!(target: "isolated_app_err", "{}", line);
    }

    /// Run the logger process directly
    async fn run_direct(&self) -> Result<()> {
        info!("Running logger process directly");

        // Create a command to run the C program
        let mut cmd = Command::new(&self.executable_path);

        // Set the working directory
        if let Some(parent) = self.executable_path.parent() {
            cmd.current_dir(parent);
        }

        // Set up pipes for stdout and stderr
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        // Run the process
        info!("Spawning process: {:?}", cmd);
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Io("Failed to spawn logger process", e))?;

        // Create tasks to handle stdout and stderr
        let stdout = child.stdout.take().expect("Failed to capture stdout");
        let stderr = child.stderr.take().expect("Failed to capture stderr");

        let stdout_app = self.clone();
        let stderr_app = self.clone();

        let stdout_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();

            info!("Started parsing stdout");
            while let Ok(Some(line)) = reader.next_line().await {
                stdout_app.parse_log_line(&line);
            }
            info!("Finished parsing stdout");
        });

        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();

            debug!("Started parsing stderr");
            while let Ok(Some(line)) = reader.next_line().await {
                stderr_app.parse_stderr_line(&line);
            }
            debug!("Finished parsing stderr");
        });

        // Wait for the process to complete
        info!("Waiting for logger process to complete...");
        let status = child
            .wait()
            .await
            .map_err(|e| Error::Io("Failed to wait for logger process", e))?;

        // Wait for the stdout and stderr tasks to complete
        let _ = stdout_task.await;
        let _ = stderr_task.await;

        info!("Logger process completed with status: {}", status);

        Ok(())
    }
}

#[async_trait]
impl IsolatedApplication for LoggerApp {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        self.executable_path.to_str().unwrap()
    }

    fn name(&self) -> &str {
        "log-parser-example"
    }

    fn working_dir(&self) -> Option<PathBuf> {
        Some(self.work_dir.clone())
    }

    /// Parse the stdout from the logger application and forward to appropriate tracing macros
    fn handle_stdout(&self, line: &str) {
        self.parse_log_line(line);
    }

    /// Handle stderr similarly to stdout but with a different target
    fn handle_stderr(&self, line: &str) {
        self.parse_stderr_line(line);
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
    // Initialize tracing with maximum verbosity
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global default subscriber");

    // Set up temporary directory
    let temp_dir =
        tempfile::tempdir().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

    let work_dir = temp_dir.path().to_path_buf();

    info!("Starting log parsing example");
    info!("Work directory: {}", work_dir.display());

    // Create the logger application
    let app = LoggerApp::new(&work_dir).await?;

    // Verify the logger program exists
    debug!(
        "Checking if logger program exists at {}",
        app.executable_path.display()
    );
    if app.executable_path.exists() {
        info!(
            "Logger program exists at: {}",
            app.executable_path.display()
        );
    } else {
        error!(
            "Logger program does not exist at: {}",
            app.executable_path.display()
        );
        return Err(Error::Io(
            "Logger program not found",
            std::io::Error::new(std::io::ErrorKind::NotFound, "File not found"),
        ));
    }

    // Run the logger process directly and parse its output
    app.run_direct().await?;

    // Clean up temporary directory
    info!("Cleaning up temporary directory");
    drop(temp_dir);

    Ok(())
}
