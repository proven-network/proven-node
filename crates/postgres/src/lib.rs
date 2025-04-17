//! Configures and runs a Postgres server to provide storage for Radix Gateway.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use regex::Regex;
use tempfile::TempDir;
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
            "/data".to_string(),
            "-c".to_string(),
            "maintenance_work_mem=1GB".to_string(),
        ]
    }

    fn executable(&self) -> &str {
        "/apps/postgres/v17.4/bin/postgres"
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

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> Result<bool, Box<dyn StdError>> {
        let ip_address = info.ip_address.to_string();

        debug!(
            "Checking if Postgres is ready on {}:{}",
            ip_address, self.port
        );

        let cmd = Command::new("/apps/postgres/v17.4/bin/pg_isready")
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
            .map_err(|e| Error::Io("failed to spawn pg_isready", e))?;

        Ok(cmd.status.success())
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        5000 // Check every 5 seconds
    }

    fn memory_limit_mb(&self) -> usize {
        1024 * 5 // 5GB
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(&self.store_dir, &"/data".to_string()),
            VolumeMount::new("/apps/postgres/v17.4", "/apps/postgres/v17.4"),
        ]
    }
}

/// Isolated application for initializing Postgres database
struct PostgresInitApp {
    username: String,
    pass_dir: String,
    store_dir: String,
}

#[async_trait]
impl IsolatedApplication for PostgresInitApp {
    fn args(&self) -> Vec<String> {
        vec![
            "-D".to_string(),
            "/data".to_string(),
            "-U".to_string(),
            self.username.clone(),
            "--pwfile".to_string(),
            "/pass/pgpass".to_string(),
        ]
    }

    fn executable(&self) -> &str {
        "/apps/postgres/v17.4/bin/initdb"
    }

    fn name(&self) -> &str {
        "postgres-init"
    }

    fn handle_stdout(&self, line: &str) {
        info!(target: "postgres-init", "{}", line);
    }

    fn handle_stderr(&self, line: &str) {
        error!(target: "postgres-init", "{}", line);
    }

    fn memory_limit_mb(&self) -> usize {
        1024 // 1GB should be plenty for initialization
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(&self.pass_dir, &"/pass".to_string()),
            VolumeMount::new(&self.store_dir, &"/data".to_string()),
            VolumeMount::new("/apps/postgres/v17.4", "/apps/postgres/v17.4"),
        ]
    }
}

/// Isolated application for running vacuum on Postgres database
struct PostgresVacuumApp {
    host: IpAddr,
    password: String,
    port: u16,
    username: String,
}

#[async_trait]
impl IsolatedApplication for PostgresVacuumApp {
    fn args(&self) -> Vec<String> {
        vec![
            "-U".to_string(),
            self.username.clone(),
            "--host".to_string(),
            self.host.to_string(),
            "--port".to_string(),
            self.port.to_string(),
            "--no-password".to_string(),
            "--all".to_string(),
            "--parallel=4".to_string(),
            "--buffer-usage-limit".to_string(),
            "4GB".to_string(),
        ]
    }

    fn env(&self) -> Vec<(String, String)> {
        vec![("PGPASSWORD".to_string(), self.password.clone())]
    }

    fn executable(&self) -> &str {
        "/apps/postgres/v17.4/bin/vacuumdb"
    }

    fn name(&self) -> &str {
        "postgres-vacuum"
    }

    fn handle_stdout(&self, line: &str) {
        info!(target: "postgres-vacuum", "{}", line);
    }

    fn handle_stderr(&self, line: &str) {
        self.handle_stdout(line);
    }

    fn memory_limit_mb(&self) -> usize {
        1024 * 5 // 5GB to match main postgres process
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![VolumeMount::new(
            "/apps/postgres/v17.4",
            "/apps/postgres/v17.4",
        )]
    }
}

/// Runs a Postgres server to provide storage for Radix Gateway.
pub struct Postgres {
    password: String,
    process: Option<IsolatedProcess>,
    username: String,
    skip_vacuum: bool,
    store_dir: String,
    port: u16,
}

/// Options for configuring `Postgres`.
pub struct PostgresOptions {
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
            password,
            username,
            skip_vacuum,
            store_dir,
            port,
        }: PostgresOptions,
    ) -> Self {
        Self {
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
    pub async fn start(&mut self) -> Result<JoinHandle<()>, Error> {
        if self.process.is_some() {
            return Err(Error::AlreadyStarted);
        }

        self.prepare_config().await?;

        if !self.is_initialized() {
            info!("initializing database...");
            self.initialize_database().await?;
        }

        // Ensure pg_hba.conf is properly configured even for existing databases
        self.configure_pg_hba().await?;

        // ensure postmaster.pid does not exist
        let postmaster_pid = std::path::Path::new(&self.store_dir).join("postmaster.pid");
        if postmaster_pid.exists() {
            std::fs::remove_file(&postmaster_pid)
                .map_err(|e| Error::Io("failed to remove postmaster pid", e))?;
        }

        let app = PostgresApp {
            username: self.username.clone(),
            store_dir: self.store_dir.clone(),
            port: self.port,
        };

        let (process, join_handle) = proven_isolation::spawn(app)
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
    pub async fn shutdown(&mut self) -> Result<(), Error> {
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

    async fn initialize_database(&self) -> Result<(), Error> {
        // Create a temporary directory for pgpass file
        let pass_dir =
            TempDir::new().map_err(|e| Error::Io("failed to create temporary directory", e))?;

        // Write password to a file in the temporary directory
        let password_file = pass_dir.path().join("pgpass");
        tokio::fs::write(&password_file, &self.password)
            .await
            .map_err(|e| Error::Io("failed to write password file", e))?;

        let init_app = PostgresInitApp {
            username: self.username.clone(),
            pass_dir: pass_dir.path().to_string_lossy().into_owned(),
            store_dir: self.store_dir.clone(),
        };

        let (process, _join_handle) = proven_isolation::spawn(init_app)
            .await
            .map_err(Error::Isolation)?;

        let exit_status = process.wait().await;

        println!("exit status: {:?}", exit_status);

        if exit_status.success() {
            info!("database initialization completed");

            Ok(())
        } else {
            error!("database initialization failed");

            Err(Error::DatabaseInit)
        }
    }

    async fn configure_pg_hba(&self) -> Result<(), Error> {
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

    async fn prepare_config(&self) -> Result<(), Error> {
        // Create data directory if it doesn't exist
        let data_dir = Path::new(&self.store_dir);
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir)
                .map_err(|e| Error::Io("Failed to create data directory", e))?;
        }

        Ok(())
    }

    async fn vacuum_database(&self) -> Result<(), Error> {
        info!("vacuuming database...");

        let vacuum_app = PostgresVacuumApp {
            host: self.ip_address(),
            password: self.password.clone(),
            port: self.port,
            username: self.username.clone(),
        };

        let (process, _join_handle) = proven_isolation::spawn(vacuum_app)
            .await
            .map_err(Error::Isolation)?;

        let exit_status = process.wait().await;

        if exit_status.success() {
            info!("vacuuming database completed");

            Ok(())
        } else {
            error!("vacuuming database failed");

            Err(Error::Vacuum)
        }
    }
}
