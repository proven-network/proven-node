//! Configures and runs Radix Gateway's data aggregator. Also manages migration
//! process as part of boot.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::path::PathBuf;
use std::process::Stdio;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use proven_isolation::{
    IsolatedApplication, IsolatedProcess, IsolationManager, Result as IsolationResult, VolumeMount,
};
use regex::Regex;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

static AGGREGATOR_CONFIG_PATH: &str = "/bin/DataAggregator/appsettings.Production.json";
static AGGREGATOR_DIR: &str = "/bin/DataAggregator";
static AGGREGATOR_PATH: &str = "/bin/DataAggregator/DataAggregator";
static MIGRATIONS_CONFIG_PATH: &str = "/bin/DatabaseMigrations/appsettings.Production.json";
static MIGRATIONS_DIR: &str = "/bin/DatabaseMigrations";
static MIGRATIONS_PATH: &str = "/bin/DatabaseMigrations/DatabaseMigrations";

/// Regex pattern for matching Radix Aggregator log lines
static LOG_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\w+): (.*)").unwrap());

/// Application struct for running the Radix Aggregator in isolation
struct RadixAggregatorApp {
    postgres_database: String,
    postgres_ip_address: String,
    postgres_port: u16,
    postgres_username: String,
    postgres_password: String,
    radix_node_ip_address: String,
    radix_node_port: u16,
}

#[async_trait]
impl IsolatedApplication for RadixAggregatorApp {
    fn chroot_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from("/tmp/radix-aggregator"))
    }

    fn executable(&self) -> &str {
        AGGREGATOR_PATH
    }

    fn name(&self) -> &str {
        "radix-aggregator"
    }

    fn handle_stdout(&self, line: &str) {
        // Use the static log pattern to parse log lines
        if let Some(caps) = LOG_PATTERN.captures(line) {
            let label = caps.get(1).map_or("unkw", |m| m.as_str());
            let message = caps.get(2).map_or(line, |m| m.as_str());
            match label {
                "trce" => trace!("{}", message),
                "dbug" => debug!("{}", message),
                "info" => info!("{}", message),
                "warn" => warn!("{}", message),
                "fail" => error!("{}", message),
                "crit" => error!("{}", message),
                _ => error!("{}", line),
            }
        } else {
            error!("{}", line);
        }
    }

    fn handle_stderr(&self, line: &str) {
        error!(target: "radix-aggregator", "{}", line);
    }

    async fn is_ready_check(&self, _process: &IsolatedProcess) -> IsolationResult<bool> {
        // Check if the service is responding on port 8080
        let client = reqwest::Client::new();
        match client.get("http://127.0.0.1:8080/health").send().await {
            Ok(response) => Ok(response.status().is_success()),
            Err(_) => Ok(false), // Not ready yet
        }
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        5000 // Check every 5 seconds
    }

    async fn prepare_config(&self) -> IsolationResult<()> {
        // Update aggregator config
        let connection_string = format!(
            "Host={};Port={};Database={};Username={};Password={}",
            self.postgres_ip_address,
            self.postgres_port,
            self.postgres_database,
            self.postgres_username,
            self.postgres_password
        );

        let core_api_address = format!(
            "http://{}:{}/core",
            self.radix_node_ip_address, self.radix_node_port
        );

        let config = json!({
            "urls": "http://0.0.0.0:8080",
            "Logging": {
                "LogLevel": {
                    "Default": "Information",
                    "Microsoft.AspNetCore": "Warning",
                    "Microsoft.Hosting.Lifetime": "Information",
                    "Microsoft.EntityFrameworkCore.Database.Command": "Warning",
                    "Microsoft.EntityFrameworkCore.Infrastructure": "Warning",
                    "Npgsql": "Warning",
                    "System.Net.Http.HttpClient.ICoreApiProvider.LogicalHandler": "Warning",
                    "System.Net.Http.HttpClient.ICoreApiProvider.ClientHandler": "Warning"
                },
                "Console": {
                    "FormatterName": "Simple",
                    "FormatterOptions": {
                        "SingleLine": true,
                        "IncludeScopes": false
                    }
                }
            },
            "PrometheusMetricsPort": 1234,
            "EnableSwagger": false,
            "ConnectionStrings": {
                "NetworkGatewayReadOnly": connection_string,
                "NetworkGatewayReadWrite": connection_string
            },
            "DataAggregator": {
                "Network": {
                    "NetworkName": "stokenet",
                    "DisableCoreApiHttpsCertificateChecks": true,
                    "CoreApiNodes": [
                        {
                            "Name": "babylon-node",
                            "CoreApiAddress": core_api_address,
                            "Enabled": true,
                            "RequestWeighting": 1
                        }
                    ]
                }
            }
        });

        // Write aggregator config
        let config_str = serde_json::to_string_pretty(&config)
            .map_err(|e| proven_isolation::Error::Application(e.to_string()))?;

        tokio::fs::write(AGGREGATOR_CONFIG_PATH, config_str)
            .await
            .map_err(|e| {
                proven_isolation::Error::Application(format!(
                    "Failed to write aggregator config: {}",
                    e
                ))
            })?;

        // Run migrations
        self.run_migrations()
            .await
            .map_err(|e| proven_isolation::Error::Application(e.to_string()))?;

        Ok(())
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(AGGREGATOR_DIR, AGGREGATOR_DIR),
            VolumeMount::new(MIGRATIONS_DIR, MIGRATIONS_DIR),
        ]
    }

    fn memory_limit_mb(&self) -> usize {
        2048 // 2GB should be sufficient for the aggregator
    }

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![8080, 1234] // Forward the API port and the Prometheus metrics port
    }

    fn working_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from(AGGREGATOR_DIR))
    }
}

impl RadixAggregatorApp {
    async fn run_migrations(&self) -> Result<()> {
        let connection_string = format!(
            "Host={};Port={};Database={};Username={};Password={};Include Error Detail=true",
            self.postgres_ip_address,
            self.postgres_port,
            self.postgres_database,
            self.postgres_username,
            self.postgres_password
        );

        let config = json!({
            "Logging": {
                "LogLevel": {
                    "Default": "Information",
                    "Microsoft.AspNetCore": "Warning",
                    "Microsoft.Hosting.Lifetime": "Information",
                    "Microsoft.EntityFrameworkCore.Database.Command": "Warning",
                    "Microsoft.EntityFrameworkCore.Infrastructure": "Warning",
                    "Npgsql": "Warning",
                    "System.Net.Http.HttpClient.ICoreApiProvider.LogicalHandler": "Warning",
                    "System.Net.Http.HttpClient.ICoreApiProvider.ClientHandler": "Warning"
                },
                "Console": {
                    "FormatterName": "Simple",
                    "FormatterOptions": {
                        "SingleLine": true,
                        "IncludeScopes": false
                    }
                }
            },
            "ConnectionStrings": {
                "NetworkGatewayMigrations": connection_string
            }
        });

        // Write migrations config
        let config_str = serde_json::to_string_pretty(&config)?;
        tokio::fs::write(MIGRATIONS_CONFIG_PATH, config_str)
            .await
            .map_err(|e| Error::Io("failed to write migrations config", e))?;

        let mut cmd = Command::new(MIGRATIONS_PATH)
            .current_dir(MIGRATIONS_DIR)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::Io("failed to spawn migrations runner", e))?;

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
                let exit_status = e.map_err(|e| Error::Io("failed to get migrations exit status", e))?;
                if !exit_status.success() {
                    return Err(Error::NonZeroExitCode(exit_status));
                }
            }
            _ = stdout_writer => {},
            _ = stderr_writer => {},
        }

        Ok(())
    }
}

/// Configures and runs Radix Gateway's data aggregator.
pub struct RadixAggregator {
    isolation_manager: IsolationManager,
    postgres_database: String,
    postgres_ip_address: String,
    postgres_port: u16,
    postgres_username: String,
    postgres_password: String,
    process: Option<IsolatedProcess>,
    radix_node_ip_address: String,
    radix_node_port: u16,
}

/// Options for configuring a `RadixAggregator`.
pub struct RadixAggregatorOptions {
    /// The name of the Postgres database.
    pub postgres_database: String,

    /// The IP address of the Postgres server.
    pub postgres_ip_address: String,

    /// The password to use when connecting to the Postgres database.
    pub postgres_password: String,

    /// The port of the Postgres server.
    pub postgres_port: u16,

    /// The username to use when connecting to the Postgres database.
    pub postgres_username: String,

    /// The IP address of the Radix Node.
    pub radix_node_ip_address: String,

    /// The port of the Radix Node.
    pub radix_node_port: u16,
}

impl RadixAggregator {
    /// Creates a new instance of `RadixAggregator`.
    #[must_use]
    pub fn new(
        RadixAggregatorOptions {
            postgres_database,
            postgres_ip_address,
            postgres_password,
            postgres_port,
            postgres_username,
            radix_node_ip_address,
            radix_node_port,
        }: RadixAggregatorOptions,
    ) -> Self {
        Self {
            isolation_manager: IsolationManager::new(),
            postgres_database,
            postgres_ip_address,
            postgres_password,
            postgres_port,
            postgres_username,
            process: None,
            radix_node_ip_address,
            radix_node_port,
        }
    }

    /// Starts the Radix Aggregator.
    ///
    /// # Errors
    ///
    /// This function will return an error if the server is already started or if there
    /// is an issue starting the isolated process.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` to the spawned task.
    pub async fn start(&mut self) -> Result<JoinHandle<()>> {
        if self.process.is_some() {
            return Err(Error::AlreadyStarted);
        }

        info!("Starting radix-aggregator...");

        let app = RadixAggregatorApp {
            postgres_database: self.postgres_database.clone(),
            postgres_ip_address: self.postgres_ip_address.clone(),
            postgres_port: self.postgres_port,
            postgres_username: self.postgres_username.clone(),
            postgres_password: self.postgres_password.clone(),
            radix_node_ip_address: self.radix_node_ip_address.clone(),
            radix_node_port: self.radix_node_port,
        };

        let (process, join_handle) = self
            .isolation_manager
            .spawn(app)
            .await
            .map_err(Error::Isolation)?;

        // Store the process for later shutdown
        self.process = Some(process);

        info!("radix-aggregator started");

        Ok(join_handle)
    }

    /// Shuts down the server.
    ///
    /// # Errors
    ///
    /// This function will return an error if there is an issue shutting down the isolated process.
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("radix-aggregator shutting down...");

        if let Some(process) = self.process.take() {
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("radix-aggregator shutdown");
        } else {
            debug!("No running radix-aggregator to shut down");
        }

        Ok(())
    }
}
