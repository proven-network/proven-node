//! Configures and runs a Postgres server to provide storage for Radix Gateway.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use regex::Regex;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Regex pattern for matching Postgres log lines
static LOG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3} (?:UTC|[+-]\d{2}) \[\d+\] (\w+):  (.*)")
        .expect("Invalid regex pattern")
});

/// Application struct for running Postgres in isolation
struct PostgresApp {
    username: String,
    store_dir: PathBuf,
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

    fn executable(&self) -> &'static str {
        "/apps/postgres/v17.4/bin/postgres"
    }

    fn name(&self) -> &'static str {
        "postgres"
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_stdout(&self, line: &str) {
        if let Some(caps) = LOG_REGEX.captures(line) {
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
        self.handle_stdout(line); // Postgres sends all logs to stderr, so handle them the same way
    }

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> bool {
        let ip_address = info.ip_address.to_string();

        debug!(
            "Checking if Postgres is ready on {}:{}",
            ip_address, self.port
        );

        match Command::new("/apps/postgres/v17.4/bin/pg_isready")
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
        {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        5000 // Check every 5 seconds
    }

    fn memory_limit_mb(&self) -> usize {
        1024 * 5 // 5GB
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(&self.store_dir, &PathBuf::from("/data")),
            VolumeMount::new("/apps/postgres/v17.4", "/apps/postgres/v17.4"),
        ]
    }
}

/// Isolated application for initializing Postgres database
struct PostgresInitApp {
    username: String,
    pass_dir: PathBuf,
    store_dir: PathBuf,
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

    fn executable(&self) -> &'static str {
        "/apps/postgres/v17.4/bin/initdb"
    }

    fn name(&self) -> &'static str {
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
            VolumeMount::new(&self.pass_dir, &PathBuf::from("/pass")),
            VolumeMount::new(&self.store_dir, &PathBuf::from("/data")),
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

    fn executable(&self) -> &'static str {
        "/apps/postgres/v17.4/bin/vacuumdb"
    }

    fn name(&self) -> &'static str {
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
#[derive(Clone)]
pub struct Postgres {
    password: String,
    port: u16,
    process: Arc<Mutex<Option<IsolatedProcess>>>,
    skip_vacuum: bool,
    store_dir: PathBuf,
    username: String,
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
    pub store_dir: PathBuf,
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
            process: Arc::new(Mutex::new(None)),
            username,
            skip_vacuum,
            store_dir,
        }
    }

    /// Returns the IP address of the Postgres server.
    ///
    /// # Panics
    ///
    /// Panics if the Postgres server is not running.
    #[must_use]
    pub async fn ip_address(&self) -> IpAddr {
        self.process
            .lock()
            .await
            .as_ref()
            .map_or(IpAddr::V4(Ipv4Addr::LOCALHOST), |p| {
                p.container_ip().unwrap()
            })
    }

    /// Returns the port of the Postgres server.
    #[must_use]
    pub const fn port(&self) -> u16 {
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
            pass_dir: pass_dir.keep(),
            store_dir: self.store_dir.clone(),
        };

        let process = proven_isolation::spawn(init_app)
            .await
            .map_err(Error::Isolation)?;

        let exit_status = process.wait().await;

        println!("exit status: {exit_status:?}");

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
        let pg_hba_content =
            "# TYPE  DATABASE        USER            ADDRESS                 METHOD\n\
             local   all             all                                     trust\n\
             host    all             all             0.0.0.0/0               md5\n\
             host    all             all             ::/0                    md5\n"
                .to_string();

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

    fn prepare_config(&self) -> Result<(), Error> {
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
            host: self.ip_address().await,
            password: self.password.clone(),
            port: self.port,
            username: self.username.clone(),
        };

        let process = proven_isolation::spawn(vacuum_app)
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

#[async_trait]
impl Bootable for Postgres {
    type Error = Error;

    /// Starts the Postgres server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the server is already started, if the database
    /// initialization fails, or if the vacuuming process fails.
    ///
    /// # Returns
    ///
    /// Returns an error if postgres fails to start.
    async fn start(&self) -> Result<(), Error> {
        if self.process.lock().await.is_some() {
            return Err(Error::AlreadyStarted);
        }

        debug!("starting Postgres server...");

        self.prepare_config()?;

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
            port: self.port,
            store_dir: self.store_dir.clone(),
            username: self.username.clone(),
        };

        let process = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        self.process.lock().await.replace(process);

        if !self.skip_vacuum {
            if let Err(e) = self.vacuum_database().await {
                let _ = self.shutdown().await;

                return Err(e);
            }
        }

        Ok(())
    }

    /// Shuts down the server.
    async fn shutdown(&self) -> Result<(), Error> {
        let taken_process = self.process.lock().await.take();
        if let Some(process) = taken_process {
            info!("postgres shutting down...");
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("postgres shutdown");
        } else {
            debug!("no running Postgres server to shut down");
        }

        Ok(())
    }

    async fn wait(&self) {
        if let Some(process) = self.process.lock().await.as_ref() {
            process.wait().await;
        }
    }
}
