//! Configures and runs a local Radix Gateway.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use regex::Regex;
use serde_json::json;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

static GATEWAY_API_CONFIG_PATH: &str = "/bin/GatewayApi/appsettings.Production.json";
static GATEWAY_API_DIR: &str = "/bin/GatewayApi";
static GATEWAY_API_PATH: &str = "/bin/GatewayApi/GatewayApi";

/// Configures and runs a local Radix Gateway.
pub struct RadixGateway {
    postgres_database: String,
    postgres_ip_address: String,
    postgres_password: String,
    postgres_port: u16,
    postgres_username: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
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
        }: RadixGatewayOptions,
    ) -> Self {
        Self {
            postgres_database,
            postgres_ip_address,
            postgres_password,
            postgres_port,
            postgres_username,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Starts the Radix Gateway server.
    ///
    /// # Errors
    ///
    /// Returns an `Error::AlreadyStarted` if the server is already running.
    /// Returns an `Error::IoError` if there is an I/O error while spawning the process.
    /// Returns an `Error::OutputParse` if there is an error parsing the process output.
    /// Returns an `Error::NonZeroExitCode` if the process exits with a non-zero status.
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        self.update_config().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the radix-gateway process
            let mut cmd = Command::new(GATEWAY_API_PATH)
                .current_dir(GATEWAY_API_DIR)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| Error::Io("failed to spawn GatewayApi process", e))?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            let re = Regex::new(r"(\w+): (.*)")?;

            // Spawn a task to read and process the stdout output of the radix-gateway process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();


                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(caps) = re.captures(&line) {
                        let label = caps.get(1).map_or("unkw", |m| m.as_str());
                        let message = caps.get(2).map_or(line.as_str(), |m| m.as_str());
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
            });

            let re = Regex::new(r"(\w+): (.*)")?;

            // Spawn a task to read and process the stdout output of the radix-gateway process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();


                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(caps) = re.captures(&line) {
                        let label = caps.get(1).map_or("unkw", |m| m.as_str());
                        let message = caps.get(2).map_or(line.as_str(), |m| m.as_str());
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
            });

            // Wait for the radix-gateway process to exit or for the shutdown token to be cancelled
            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.map_err(|e| Error::Io("failed to wait for exit", e))?;

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                () = shutdown_token.cancelled() => {
                    let raw_pid: i32 = cmd.id().ok_or(Error::OutputParse)?.try_into().map_err(|_| Error::BadPid)?;
                    let pid = Pid::from_raw(raw_pid);
                    signal::kill(pid, Signal::SIGTERM)?;

                    let _ = cmd.wait().await;

                    Ok(())
                }
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        self.task_tracker.close();

        Ok(server_task)
    }

    /// Shuts down the server.
    pub async fn shutdown(&self) {
        info!("radix-gateway shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("radix-gateway shutdown");
    }

    async fn update_config(&self) -> Result<()> {
        let connection_string = format!(
            "Host={};Port={};Database={};Username={};Password={}",
            self.postgres_ip_address,
            self.postgres_port,
            self.postgres_database,
            self.postgres_username,
            self.postgres_password
        );

        let config = json!({
            "urls": "http://127.0.0.1:8081",
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
                            "CoreApiAddress": "http://127.0.0.1:3333/core",
                            "Enabled": true,
                            "RequestWeighting": 1
                        }
                    ]
                }
            }
        });

        let mut config_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(GATEWAY_API_CONFIG_PATH)
            .await
            .map_err(|e| Error::Io("failed to open gateway config", e))?;

        config_file
            .write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
            .await
            .map_err(|e| Error::Io("failed to write gateway config", e))?;

        Ok(())
    }
}
