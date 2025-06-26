//! Configures and runs a local Radix Gateway.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use regex::Regex;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace, warn};

static GATEWAY_API_CONFIG_PATH: &str =
    "/apps/radix-gateway/v1.9.2/GatewayApi/appsettings.Production.json";
static GATEWAY_API_DIR: &str = "/apps/radix-gateway/v1.9.2/GatewayApi";
static GATEWAY_API_PATH: &str = "/apps/radix-gateway/v1.9.2/GatewayApi/GatewayApi";

/// Regex pattern for matching Gateway log lines
static LOG_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\w+): (.*)").unwrap());

/// Application struct for running the Radix Gateway in isolation
struct RadixGatewayApp;

#[async_trait]
impl IsolatedApplication for RadixGatewayApp {
    fn env(&self) -> Vec<(String, String)> {
        vec![("DOTNET_ROOT".to_string(), "/usr/share/dotnet".to_string())]
    }

    fn executable(&self) -> &str {
        GATEWAY_API_PATH
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_stdout(&self, line: &str) {
        // Use the static log pattern to parse log lines
        if let Some(caps) = LOG_REGEX.captures(line) {
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

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> bool {
        // Check if the service is responding on port 8081
        let client = reqwest::Client::new();
        (client
            .get(format!("http://{}:8081/health", info.ip_address))
            .send()
            .await)
            .is_ok_and(|response| response.status().is_success())
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        5000 // Check every 5 seconds
    }

    fn memory_limit_mb(&self) -> usize {
        1024 // 1GB should be sufficient for the gateway
    }

    fn name(&self) -> &'static str {
        "radix-gateway"
    }

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![8081, 1235] // Forward the API port and the Prometheus metrics port
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(GATEWAY_API_DIR, GATEWAY_API_DIR),
            VolumeMount::new("/usr/share/dotnet", "/usr/share/dotnet"),
        ]
    }

    fn working_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from(GATEWAY_API_DIR))
    }
}

/// Configures and runs a local Radix Gateway.
#[derive(Clone)]
pub struct RadixGateway {
    postgres_database: String,
    postgres_ip_address: String,
    postgres_password: String,
    postgres_port: u16,
    postgres_username: String,
    process: Arc<Mutex<Option<IsolatedProcess>>>,
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
            postgres_database,
            postgres_ip_address,
            postgres_password,
            postgres_port,
            postgres_username,
            process: Arc::new(Mutex::new(None)),
            radix_node_ip_address,
            radix_node_port,
        }
    }

    async fn prepare_config(&self) -> Result<(), Error> {
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
        let config_str = serde_json::to_string_pretty(&config)?;

        tokio::fs::write(GATEWAY_API_CONFIG_PATH, config_str)
            .await
            .map_err(|e| Error::Io("Failed to write gateway config", e))?;

        Ok(())
    }
}

#[async_trait]
impl Bootable for RadixGateway {
    /// Starts the Radix Gateway server.
    ///
    /// # Errors
    ///
    /// Returns an `Error::AlreadyStarted` if the server is already running.
    /// Returns an `Error::Isolation` if there is an error starting the isolated process.
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.process.lock().await.is_some() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        info!("Starting radix-gateway...");

        self.prepare_config().await?;

        let process = proven_isolation::spawn(RadixGatewayApp)
            .await
            .map_err(Error::Isolation)?;

        // Store the process for later shutdown
        self.process.lock().await.replace(process);

        info!("radix-gateway started");

        Ok(())
    }

    /// Shuts down the server.
    ///
    /// # Errors
    ///
    /// This function will return an error if there is an issue shutting down the isolated process.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("radix-gateway shutting down...");

        let taken_process = self.process.lock().await.take();
        if let Some(process) = taken_process {
            process
                .shutdown()
                .await
                .map_err(|e| Box::new(Error::Isolation(e)))?;

            info!("radix-gateway shutdown");
        } else {
            debug!("No running radix-gateway to shut down");
        }

        Ok(())
    }

    async fn wait(&self) {
        if let Some(process) = self.process.lock().await.as_ref() {
            process.wait().await;
        }
    }
}
