mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
// use regex::Regex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

static DATA_AGGREGATOR_PATH: &str = "/bin/DataAggregator/DataAggregator.dll";
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

        self.run_migrations().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let postgres_database = self.postgres_database.clone();
        let postgres_username = self.postgres_username.clone();
        let postgres_password = self.postgres_password.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the babylon-aggregator process
            let mut cmd = Command::new("dotnet")
                .arg(DATA_AGGREGATOR_PATH)
                .env("ASPNETCORE_ENVIRONMENT", "Production")
                .env("Logging__LogLevel__Default", "Information")
                .env("Logging__LogLevel__Microsoft.AspNetCore", "Warning")
                .env(
                    "Logging__LogLevel__Microsoft.Hosting.Lifetime",
                    "Information",
                )
                .env(
                    "Logging__LogLevel__Microsoft.EntityFrameworkCore.Database.Command",
                    "Warning",
                )
                .env(
                    "Logging__LogLevel__Microsoft.EntityFrameworkCore.Infrastructure",
                    "Warning",
                )
                .env("Logging__LogLevel__Npgsql", "Warning")
                .env(
                    "Logging__LogLevel__System.Net.Http.HttpClient.ICoreApiProvider.LogicalHandler",
                    "Warning",
                )
                .env(
                    "Logging__LogLevel__System.Net.Http.HttpClient.ICoreApiProvider.ClientHandler",
                    "Warning",
                )
                .env("Logging__Console__FormatterName", "Simple")
                .env("Logging__Console__FormatterOptions__SingleLine", "true")
                .env("Logging__Console__FormatterOptions__IncludeScopes", "false")
                .env("ASPNETCORE_URLS", "http://127.0.0.1.8080")
                .env("PrometheusMetricsPort", "1234")
                .env(
                    "ConnectionStrings__NetworkGatewayReadWrite",
                    format!(
                        "Host=127.0.0.1:5432;Database={};Username={};Password={}",
                        postgres_database, postgres_username, postgres_password
                    ),
                )
                .env("DataAggregator__Network__NetworkName", "stokenet")
                .env(
                    "DataAggregator__Network__DisableCoreApiHttpsCertificateChecks",
                    "true",
                )
                .env(
                    "DataAggregator__Network__CoreApiNodes__0__Name",
                    "babylon-node",
                )
                .env(
                    "DataAggregator__Network__CoreApiNodes__0__CoreApiAddress",
                    "http://127.0.0.1:3333/core",
                )
                .env("DataAggregator__Network__CoreApiNodes__0__Enabled", "true")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stdout output of the babylon-aggregator process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();

                // let re = Regex::new(r"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]) (\[[A-Z]+\]) (.*)")
                //     .unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    info!("{}", line);
                    // if let Some(caps) = re.captures(&line) {
                    //     let label = caps.get(2).unwrap().as_str();
                    //     let message = caps.get(3).unwrap().as_str();
                    //     match label {
                    //         "[INFO]" => info!("{}", message),
                    //         "[NOTICE]" => info!("{}", message),
                    //         "[DEBUG]" => debug!("{}", message),
                    //         "[WARNING]" => warn!("{}", message),
                    //         "[CRITICAL]" => warn!("{}", message),
                    //         "[ERROR]" => error!("{}", message),
                    //         "[FATAL]" => error!("{}", message),
                    //         _ => error!("{}", line),
                    //     }
                    // } else {
                    //     error!("{}", line);
                    // }
                }
            });

            // Spawn a task to read and process the stderr output of the babylon-aggregator process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                // let re = Regex::new(r"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]) (\[[A-Z]+\]) (.*)")
                //     .unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    warn!("{}", line);
                    // if let Some(caps) = re.captures(&line) {
                    //     let label = caps.get(2).unwrap().as_str();
                    //     let message = caps.get(3).unwrap().as_str();
                    //     match label {
                    //         "[INFO]" => info!("{}", message),
                    //         "[NOTICE]" => info!("{}", message),
                    //         "[DEBUG]" => debug!("{}", message),
                    //         "[WARNING]" => warn!("{}", message),
                    //         "[CRITICAL]" => warn!("{}", message),
                    //         "[ERROR]" => error!("{}", message),
                    //         "[FATAL]" => error!("{}", message),
                    //         _ => error!("{}", line),
                    //     }
                    // } else {
                    //     error!("{}", line);
                    // }
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
            .env("ASPNETCORE_ENVIRONMENT", "Production")
            .env(
                "ConnectionStrings__NetworkGatewayMigrations",
                format!(
                    "Host=127.0.0.1:5432;Database={};Username={};Password={}",
                    self.postgres_database, self.postgres_username, self.postgres_password
                ),
            )
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
}
