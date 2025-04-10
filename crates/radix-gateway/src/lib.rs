//! Configures and runs a local Radix Gateway.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::path::PathBuf;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use proven_isolation::{
    IsolatedApplication, IsolatedProcess, IsolationManager, Result as IsolationResult, VolumeMount,
};
use regex::Regex;
use serde_json::json;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

static GATEWAY_API_CONFIG_PATH: &str = "/bin/GatewayApi/appsettings.Production.json";
static GATEWAY_API_DIR: &str = "/bin/GatewayApi";
static GATEWAY_API_PATH: &str = "/bin/GatewayApi/GatewayApi";

/// Regex pattern for matching Gateway log lines
static LOG_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\w+): (.*)").unwrap());

/// Application struct for running the Radix Gateway in isolation
struct RadixGatewayApp {
    postgres_database: String,
    postgres_ip_address: String,
    postgres_password: String,
    postgres_port: u16,
    postgres_username: String,
    radix_node_ip_address: String,
    radix_node_port: u16,
}

#[async_trait]
impl IsolatedApplication for RadixGatewayApp {
    fn chroot_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from("/tmp/radix-gateway"))
    }

    fn executable(&self) -> &str {
        GATEWAY_API_PATH
    }

    fn name(&self) -> &str {
        "radix-gateway"
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
        error!(target: "radix-gateway", "{}", line);
    }

    async fn is_ready_check(&self, _process: &IsolatedProcess) -> IsolationResult<bool> {
        // Check if the service is responding on port 8081
        let client = reqwest::Client::new();
        match client.get("http://127.0.0.1:8081/health").send().await {
            Ok(response) => Ok(response.status().is_success()),
            Err(_) => Ok(false), // Not ready yet
        }
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        5000 // Check every 5 seconds
    }

    async fn prepare_config(&self) -> IsolationResult<()> {
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
            "urls": "http://0.0.0.0:8081",
            "Logging": {
                "LogLevel": {
                    "Default": "Information",
                    "Microsoft.Hosting.Lifetime": "Information",
                    "Microsoft.EntityFrameworkCore.Database.Command": "Warning",
                    "Microsoft.EntityFrameworkCore.Infrastructure": "Warning",
                    "Npgsql": "Warning",
                    "System.Net.Http.HttpClient.ICoreApiProvider.LogicalHandler": "Warning",
                    "System.Net.Http.HttpClient.ICoreApiProvider.ClientHandler": "Warning",
                    "System.Net.Http.HttpClient.ICoreNodeHealthChecker.LogicalHandler": "Warning",
                    "System.Net.Http.HttpClient.ICoreNodeHealthChecker.ClientHandler": "Warning"
                },
                "Console": {
                    "FormatterName": "Simple",
                    "FormatterOptions": {
                        "SingleLine": true,
                        "IncludeScopes": false
                    }
                }
            },
            "PrometheusMetricsPort": 1235,
            "EnableSwagger": false,
            "ConnectionStrings": {
                "NetworkGatewayReadOnly": connection_string,
                "NetworkGatewayReadWrite": connection_string
            },
            "GatewayApi": {
                "AcceptableLedgerLag": {
                    "ReadRequestAcceptableDbLedgerLagSeconds": 720,
                    "ConstructionRequestsAcceptableDbLedgerLagSeconds": 720,
                    "PreventReadRequestsIfDbLedgerIsBehind": true,
                    "PreventConstructionRequestsIfDbLedgerIsBehind": true
                },
                "Endpoint": {
                "MaxPageSize": 100,
                "DefaultPageSize": 100
                },
                "Network": {
                    "NetworkName": "stokenet",
                    "DisableCoreApiHttpsCertificateChecks": true,
                    "MaxAllowedStateVersionLagToBeConsideredSynced": 100,
                    "IgnoreNonSyncedNodes": true,
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

        // Write gateway config
        let config_str = serde_json::to_string_pretty(&config)
            .map_err(|e| proven_isolation::Error::Application(e.to_string()))?;

        tokio::fs::write(GATEWAY_API_CONFIG_PATH, config_str)
            .await
            .map_err(|e| {
                proven_isolation::Error::Application(format!(
                    "Failed to write gateway config: {}",
                    e
                ))
            })?;

        Ok(())
    }

    fn memory_limit_mb(&self) -> usize {
        1024 // 1GB should be sufficient for the gateway
    }

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![8081, 1235] // Forward the API port and the Prometheus metrics port
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![VolumeMount::new(GATEWAY_API_DIR, GATEWAY_API_DIR)]
    }

    fn working_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from(GATEWAY_API_DIR))
    }
}

/// Configures and runs a local Radix Gateway.
pub struct RadixGateway {
    isolation_manager: IsolationManager,
    postgres_database: String,
    postgres_ip_address: String,
    postgres_password: String,
    postgres_port: u16,
    postgres_username: String,
    process: Option<IsolatedProcess>,
    radix_node_ip_address: String,
    radix_node_port: u16,
}

/// Options for configuring a `RadixGateway`.
pub struct RadixGatewayOptions {
    /// The name of the Postgres database.
    pub postgres_database: String,

    /// The IP address of the Postgres server.
    pub postgres_ip_address: String,

    /// The password for the Postgres database.
    pub postgres_password: String,

    /// The port of the Postgres server.
    pub postgres_port: u16,

    /// The username for the Postgres database.
    pub postgres_username: String,

    /// The IP address of the Radix Node.
    pub radix_node_ip_address: String,

    /// The port of the Radix Node.
    pub radix_node_port: u16,
}

impl RadixGateway {
    /// Creates a new `RadixGateway` instance.
    #[must_use]
    pub fn new(
        RadixGatewayOptions {
            postgres_database,
            postgres_ip_address,
            postgres_password,
            postgres_port,
            postgres_username,
            radix_node_ip_address,
            radix_node_port,
        }: RadixGatewayOptions,
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

    /// Starts the Radix Gateway server.
    ///
    /// # Errors
    ///
    /// Returns an `Error::AlreadyStarted` if the server is already running.
    /// Returns an `Error::Isolation` if there is an error starting the isolated process.
    pub async fn start(&mut self) -> Result<JoinHandle<()>> {
        if self.process.is_some() {
            return Err(Error::AlreadyStarted);
        }

        info!("Starting radix-gateway...");

        let app = RadixGatewayApp {
            postgres_database: self.postgres_database.clone(),
            postgres_ip_address: self.postgres_ip_address.clone(),
            postgres_password: self.postgres_password.clone(),
            postgres_port: self.postgres_port,
            postgres_username: self.postgres_username.clone(),
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

        info!("radix-gateway started");

        Ok(join_handle)
    }

    /// Shuts down the server.
    ///
    /// # Errors
    ///
    /// This function will return an error if there is an issue shutting down the isolated process.
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("radix-gateway shutting down...");

        if let Some(process) = self.process.take() {
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("radix-gateway shutdown");
        } else {
            debug!("No running radix-gateway to shut down");
        }

        Ok(())
    }
}
