mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use regex::Regex;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

static DATA_AGGREGATOR_CONFIG_PATH: &str =
    "/bin/DataAggregator/appsettings.Production.overrides.json";
static DATA_AGGREGATOR_PATH: &str = "/bin/DataAggregator/DataAggregator.dll";
static DATABASE_MIGRATIONS_CONFIG_PATH: &str =
    "/bin/DatabaseMigrations/appsettings.Production.overrides.json";
static DATABASE_MIGRATIONS_PATH: &str = "/bin/DatabaseMigrations/DatabaseMigrations.dll";

pub struct BabylonAggregator {
    postgres_database: String,
    postgres_username: String,
    postgres_password: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl BabylonAggregator {
    pub fn new(
        postgres_database: String,
        postgres_username: String,
        postgres_password: String,
    ) -> Self {
        Self {
            postgres_database,
            postgres_username,
            postgres_password,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        self.update_config()?;
        self.run_migrations().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the babylon-aggregator process
            let mut cmd = Command::new("dotnet")
                .env("ASPNETCORE_ENVIRONMENT", "Production")
                .env("ASPNETCORE_URLS", "http://127.0.0.1.8080")
                .arg(DATA_AGGREGATOR_PATH)
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stdout output of the babylon-aggregator process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();

                let re = Regex::new(r"(\w+): (.*)").unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(caps) = re.captures(&line) {
                        let label = caps.get(1).unwrap().as_str();
                        let message = caps.get(2).unwrap().as_str();
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

            // Wait for the babylon-aggregator process to exit or for the shutdown token to be cancelled
            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.unwrap();

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                _ = shutdown_token.cancelled() => {
                    let pid = Pid::from_raw(cmd.id().unwrap() as i32);
                    signal::kill(pid, Signal::SIGTERM).unwrap();

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
        info!("babylon-aggregator shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("babylon-aggregator shutdown");
    }

    async fn run_migrations(&self) -> Result<()> {
        let cmd = Command::new("dotnet")
            .arg(DATABASE_MIGRATIONS_PATH)
            .output()
            .await
            .map_err(Error::Spawn)?;

        info!("stdout: {}", String::from_utf8_lossy(&cmd.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&cmd.stderr));

        if !cmd.status.success() {
            return Err(Error::NonZeroExitCode(cmd.status));
        }

        Ok(())
    }

    fn update_config(&self) -> Result<()> {
        let connection_string = format!(
            "Host=127.0.0.1:5432;Database={};Username={};Password={}",
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
            "PrometheusMetricsPort": 1234,
            "EnableSwagger": false,
            "ConnectionStrings": {
                "NetworkGatewayReadWrite": connection_string,
                "NetworkGatewayMigrations": connection_string
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

        let mut config_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(DATABASE_MIGRATIONS_CONFIG_PATH)
            .unwrap();

        std::io::Write::write_all(
            &mut config_file,
            serde_json::to_string_pretty(&config).unwrap().as_bytes(),
        )
        .map_err(Error::ConfigWrite)?;

        let mut config_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(DATA_AGGREGATOR_CONFIG_PATH)
            .unwrap();

        std::io::Write::write_all(
            &mut config_file,
            serde_json::to_string_pretty(&config).unwrap().as_bytes(),
        )
        .map_err(Error::ConfigWrite)?;

        Ok(())
    }
}
