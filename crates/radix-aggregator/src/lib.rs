//! Configures and runs Radix Gateway's data aggregator. Also manages migration
//! process as part of boot.
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

static AGGREGATOR_CONFIG_PATH: &str = "/bin/DataAggregator/appsettings.Production.json";
static AGGREGATOR_DIR: &str = "/bin/DataAggregator";
static AGGREGATOR_PATH: &str = "/bin/DataAggregator/DataAggregator";
static MIGRATIONS_CONFIG_PATH: &str = "/bin/DatabaseMigrations/appsettings.Production.json";
static MIGRATIONS_DIR: &str = "/bin/DatabaseMigrations";
static MIGRATIONS_PATH: &str = "/bin/DatabaseMigrations/DatabaseMigrations";

/// Configures and runs Radix Gateway's data aggregator.
pub struct RadixAggregator {
    postgres_database: String,
    postgres_username: String,
    postgres_password: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Options for configuring a `RadixAggregator`.
pub struct RadixAggregatorOptions {
    /// The name of the Postgres database.
    pub postgres_database: String,

    /// The username to use when connecting to the Postgres database.
    pub postgres_username: String,

    /// The password to use when connecting to the Postgres database.
    pub postgres_password: String,
}

impl RadixAggregator {
    /// Creates a new instance of `RadixAggregator`.
    #[must_use]
    pub fn new(
        RadixAggregatorOptions {
            postgres_database,
            postgres_username,
            postgres_password,
        }: RadixAggregatorOptions,
    ) -> Self {
        Self {
            postgres_database,
            postgres_username,
            postgres_password,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Starts the Radix Aggregator.
    ///
    /// # Errors
    ///
    /// This function will return an error if the task tracker is already closed,
    /// if there is an issue updating the configuration, or if the migrations fail to run.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` to the spawned task.
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        self.update_config().await?;
        self.run_migrations().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the radix-aggregator process
            let mut cmd = Command::new(AGGREGATOR_PATH)
                .current_dir(AGGREGATOR_DIR)
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|e| Error::IoError("failed to spawn radix-aggregator", e))?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;

            let re = Regex::new(r"(\w+): (.*)")?;
            // Spawn a task to read and process the stdout output of the radix-aggregator process
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

            // Wait for the radix-aggregator process to exit or for the shutdown token to be cancelled
            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.map_err(|e| Error::IoError("failed to wait for exit", e))?;

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
        info!("radix-aggregator shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("radix-aggregator shutdown");
    }

    async fn run_migrations(&self) -> Result<()> {
        let connection_string = format!(
            "Host=127.0.0.1:5432;Database={};Username={};Password={};Include Error Detail=true",
            self.postgres_database, self.postgres_username, self.postgres_password
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

        let mut config_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(MIGRATIONS_CONFIG_PATH)
            .await
            .map_err(|e| Error::IoError("failed to open migrations config", e))?;

        config_file
            .write_all(serde_json::to_string_pretty(&config)?.as_bytes())
            .await
            .map_err(|e| Error::IoError("failed to write migrations config", e))?;

        let mut cmd = Command::new(MIGRATIONS_PATH)
            .current_dir(MIGRATIONS_DIR)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::IoError("failed to spawn migrations runner", e))?;

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
                let exit_status = e.map_err(|e| Error::IoError("failed to get migrations exit status", e))?;
                if !exit_status.success() {
                    return Err(Error::NonZeroExitCode(exit_status));
                }
            }
            _ = stdout_writer => {},
            _ = stderr_writer => {},
        }

        Ok(())
    }

    async fn update_config(&self) -> Result<()> {
        let connection_string = format!(
            "Host=127.0.0.1:5432;Database={};Username={};Password={}",
            self.postgres_database, self.postgres_username, self.postgres_password
        );

        let config = json!({
            "urls": "http://127.0.0.1:8080",
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
            .open(AGGREGATOR_CONFIG_PATH)
            .await
            .map_err(|e| Error::IoError("failed to open config file", e))?;

        config_file
            .write_all(serde_json::to_string_pretty(&config)?.as_bytes())
            .await
            .map_err(|e| Error::IoError("failed to write config file", e))?;

        Ok(())
    }
}
