//! Configures and runs a Postgres server to provide storage for Radix Gateway.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use proven_isolation::{
    IsolatedApplication, IsolatedProcess, IsolationManager, Result as IsolationResult, VolumeMount,
};
use regex::Regex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Regex pattern for matching Postgres log lines
static LOG_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3} (?:UTC|[+-]\d{2}) \[\d+\] (\w+):  (.*)")
        .expect("Invalid regex pattern")
});

/// Application struct for running Postgres in isolation
struct PostgresApp {
    bin_path: String,
    executable: String,
    username: String,
    store_dir: String,
    port: u16,
}

#[async_trait]
impl IsolatedApplication for PostgresApp {
    fn args(&self) -> Vec<String> {
        vec![
            "-h".to_string(),
            "0.0.0.0".to_string(),
            "-p".to_string(),
            self.port.to_string(),
            "-D".to_string(),
            self.store_dir.clone(),
            "-c".to_string(),
            "maintenance_work_mem=1GB".to_string(),
        ]
    }

    fn chroot_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from("/tmp/postgres"))
    }

    fn executable(&self) -> &str {
        &self.executable
    }

    fn name(&self) -> &str {
        "postgres"
    }

    fn handle_stdout(&self, line: &str) {
        if let Some(caps) = LOG_PATTERN.captures(line) {
            let label = caps.get(1).map_or("UNKNOWN", |m| m.as_str());
            let message = caps.get(2).map_or(line, |m| m.as_str());
            match label {
                "DEBUG1" => debug!(target: "postgres", "{}", message),
                "DEBUG2" => debug!(target: "postgres", "{}", message),
                "DEBUG3" => debug!(target: "postgres", "{}", message),
                "DEBUG4" => debug!(target: "postgres", "{}", message),
                "DEBUG5" => debug!(target: "postgres", "{}", message),
                "INFO" => info!(target: "postgres", "{}", message),
                "NOTICE" => info!(target: "postgres", "{}", message),
                "WARNING" => warn!(target: "postgres", "{}", message),
                "ERROR" => error!(target: "postgres", "{}", message),
                "LOG" => info!(target: "postgres", "{}", message),
                "FATAL" => error!(target: "postgres", "{}", message),
                "PANIC" => error!(target: "postgres", "{}", message),
                _ => error!(target: "postgres", "{}", line),
            }
        } else {
            error!(target: "postgres", "{}", line);
        }
    }

    fn handle_stderr(&self, line: &str) {
        self.handle_stdout(line) // Postgres sends all logs to stderr, so handle them the same way
    }

    async fn is_ready_check(&self, process: &IsolatedProcess) -> IsolationResult<bool> {
        let ip_address = if let Some(ip) = process.container_ip() {
            &ip.to_string()
        } else {
            "127.0.0.1"
        };

        debug!(
            "Checking if Postgres is ready on {}:{}",
            ip_address, self.port
        );

        let cmd = Command::new(format!("{}/pg_isready", self.bin_path))
            .arg("-h")
            .arg(ip_address)
            .arg("-p")
            .arg(self.port.to_string())
            .arg("-U")
            .arg(&self.username)
            .arg("-d")
            .arg("postgres")
            .output()
            .await
            .map_err(|e| proven_isolation::Error::Application(e.to_string()))?;

        Ok(cmd.status.success())
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        5000 // Check every 5 seconds
    }

    fn memory_limit_mb(&self) -> usize {
        1024 * 5 // 5GB
    }

    async fn prepare_config(&self) -> IsolationResult<()> {
        // Create data directory if it doesn't exist
        let data_dir = Path::new(&self.store_dir);
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir).map_err(|e| {
                proven_isolation::Error::Application(format!(
                    "Failed to create data directory: {}",
                    e
                ))
            })?;
        }

        Ok(())
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![VolumeMount::new(&self.store_dir, &self.store_dir)]
    }
}

/// Runs a Postgres server to provide storage for Radix Gateway.
pub struct Postgres {
    bin_path: String,
    isolation_manager: IsolationManager,
    password: String,
    process: Option<IsolatedProcess>,
    username: String,
    skip_vacuum: bool,
    store_dir: String,
    port: u16,
}

/// Options for configuring `Postgres`.
pub struct PostgresOptions {
    /// The path to the directory containing the Postgres binaries.
    pub bin_path: String,

    /// The password for the Postgres user.
    pub password: String,

    /// The port to run Postgres on.
    pub port: u16,

    /// The username for the Postgres user.
    pub username: String,

    /// Whether to skip vacuuming the database.
    pub skip_vacuum: bool,

    /// The directory to store data in.
    pub store_dir: String,
}

impl Postgres {
    /// Creates a new instance of `Postgres`.
    #[must_use]
    pub fn new(
        PostgresOptions {
            bin_path,
            password,
            username,
            skip_vacuum,
            store_dir,
            port,
        }: PostgresOptions,
    ) -> Self {
        Self {
            bin_path,
            isolation_manager: IsolationManager::new(),
            password,
            port,
            process: None,
            username,
            skip_vacuum,
            store_dir,
        }
    }

    /// Starts the Postgres server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the server is already started, if the database
    /// initialization fails, or if the vacuuming process fails.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` to the spawned task that runs the Postgres server.
    pub async fn start(&mut self) -> Result<JoinHandle<()>> {
        if self.process.is_some() {
            return Err(Error::AlreadyStarted);
        }

        if !self.is_initialized() {
            self.initialize_database().await?;
        } else {
            // Ensure pg_hba.conf is properly configured even for existing databases
            self.configure_pg_hba().await?;
        }

        // ensure postmaster.pid does not exist
        let postmaster_pid = std::path::Path::new(&self.store_dir).join("postmaster.pid");
        if postmaster_pid.exists() {
            std::fs::remove_file(&postmaster_pid)
                .map_err(|e| Error::Io("failed to remove postmaster pid", e))?;
        }

        let app = PostgresApp {
            bin_path: self.bin_path.clone(),
            executable: format!("{}/postgres", self.bin_path),
            username: self.username.clone(),
            store_dir: self.store_dir.clone(),
            port: self.port,
        };

        let (process, join_handle) = self
            .isolation_manager
            .spawn(app)
            .await
            .map_err(Error::Isolation)?;

        self.process = Some(process);
        if !self.skip_vacuum {
            if let Err(e) = self.vacuum_database().await {
                let _ = self.shutdown().await;

                return Err(e);
            }
        }

        Ok(join_handle)
    }

    /// Shuts down the server.
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(process) = self.process.take() {
            info!("postgres shutting down...");
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("postgres shutdown");
        } else {
            debug!("no running Postgres server to shut down");
        }

        Ok(())
    }

    /// Returns the IP address of the Postgres server.
    #[must_use]
    pub fn ip_address(&self) -> IpAddr {
        self.process
            .as_ref()
            .map(|p| p.container_ip().unwrap())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
    }

    /// Returns the port of the Postgres server.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    async fn initialize_database(&self) -> Result<()> {
        // Write password to a file in tmp for use by initdb
        let password_file = std::path::Path::new("/tmp/pgpass");
        tokio::fs::write(password_file, self.password.clone())
            .await
            .map_err(|e| Error::Io("failed to write password file", e))?;

        let cmd = Command::new(format!("{}/initdb", self.bin_path))
            .arg("-D")
            .arg(&self.store_dir)
            .arg("-U")
            .arg(self.username.clone())
            .arg("--pwfile")
            .arg(password_file)
            .output()
            .await
            .map_err(|e| Error::Io("failed to spawn initdb", e))?;

        info!("stdout: {}", String::from_utf8_lossy(&cmd.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&cmd.stderr));

        if !cmd.status.success() {
            return Err(Error::InitDb);
        }

        // Configure pg_hba.conf to allow connections from all hosts
        self.configure_pg_hba().await?;

        Ok(())
    }

    async fn configure_pg_hba(&self) -> Result<()> {
        let pg_hba_path = std::path::Path::new(&self.store_dir).join("pg_hba.conf");

        // Create a pg_hba.conf that allows connections from anywhere
        let pg_hba_content = format!(
            "# TYPE  DATABASE        USER            ADDRESS                 METHOD\n\
             local   all             all                                     trust\n\
             host    all             all             0.0.0.0/0               md5\n\
             host    all             all             ::/0                    md5\n"
        );

        tokio::fs::write(&pg_hba_path, pg_hba_content)
            .await
            .map_err(|e| Error::Io("failed to write pg_hba.conf", e))?;

        info!("Updated pg_hba.conf to allow connections from all hosts");

        Ok(())
    }

    // check if the database is initialized
    fn is_initialized(&self) -> bool {
        std::path::Path::new(&self.store_dir).exists()
            && std::path::Path::new(&self.store_dir)
                .join("PG_VERSION")
                .exists()
    }

    async fn vacuum_database(&self) -> Result<()> {
        info!("vacuuming database...");

        let mut cmd = Command::new(format!("{}/vacuumdb", self.bin_path))
            .arg("-U")
            .arg(&self.username)
            .arg("--all")
            .arg("--verbose")
            .arg("--parallel=4")
            .arg("--buffer-usage-limit")
            .arg("4GB")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::Io("failed to spawn vacuumdb", e))?;

        let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
        let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

        let stdout_writer = tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                info!("{}", line);
            }
        });

        let stderr_writer = tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                info!("{}", line);
            }
        });

        tokio::select! {
            e = cmd.wait() => {
                let exit_status = e.map_err(|e| Error::Io("failed to get vacuum exit status", e))?;
                if !exit_status.success() {
                    return Err(Error::VacuumFailed);
                }
            }
            _ = stdout_writer => {},
            _ = stderr_writer => {},
        }

        Ok(())
    }
}
