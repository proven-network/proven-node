mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use regex::Regex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

static GATEWAY_API_PATH: &str = "/bin/GatewayApi/GatewayApi.dll";

pub struct BabylonGateway {
    postgres_database: String,
    postgres_username: String,
    postgres_password: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl BabylonGateway {
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

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let postgres_database = self.postgres_database.clone();
        let postgres_username = self.postgres_username.clone();
        let postgres_password = self.postgres_password.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the babylon-gateway process
            let mut cmd = Command::new("dotnet")
                .arg(GATEWAY_API_PATH)
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
                .env(
                    "Logging__LogLevel__System.Net.Http.HttpClient.ICoreNodeHealthChecker.LogicalHandler",
                    "Warning",
                )
                .env(
                    "Logging__LogLevel__System.Net.Http.HttpClient.ICoreNodeHealthChecker.ClientHandler",
                    "Warning",
                )
                .env("Logging__Console__FormatterName", "Simple")
                .env("Logging__Console__FormatterOptions__SingleLine", "true")
                .env("Logging__Console__FormatterOptions__IncludeScopes", "false")
                .env("ASPNETCORE_URLS", "http://127.0.0.1.8080")
                .env("PrometheusMetricsPort", "1235")
                .env("EnableSwagger", "false")
                .env(
                    "ConnectionStrings__NetworkGatewayReadWrite",
                    format!(
                        "Host=127.0.0.1:5432;Database={};Username={};Password={}",
                        postgres_database, postgres_username, postgres_password
                    ),
                )
                .env("GatewayApi__AcceptableLedgerLag__ReadRequestAcceptableDbLedgerLagSeconds", "720")
                .env("GatewayApi__AcceptableLedgerLag__ConstructionRequestsAcceptableDbLedgerLagSeconds", "720")
                .env("GatewayApi__Endpoint__MaxPageSize", "100")
                .env("GatewayApi__Endpoint__DefaultPageSize", "100")
                .env("GatewayApi__AcceptableLedgerLag__PreventReadRequestsIfDbLedgerIsBehind", "true")
                .env("GatewayApi__AcceptableLedgerLag__PreventConstructionRequestsIfDbLedgerIsBehind", "true")
                .env("GatewayApi__Network__NetworkName", "stokenet")
                .env(
                    "GatewayApi__Network__DisableCoreApiHttpsCertificateChecks",
                    "true",
                )
                .env(
                    "GatewayApi__Network__CoreApiNodes__0__Name",
                    "One",
                )
                .env(
                    "GatewayApi__Network__CoreApiNodes__0__CoreApiAddress",
                    "http://127.0.0.1:3333/core",
                )
                .env("GatewayApi__Network__CoreApiNodes__0__Enabled", "true")
                .env("GatewayApi__Network__CoreApiNodes__0__RequestWeighting", "1")
                .env(
                    "GatewayApi__Network__CoreApiNodes__1__Name",
                    "Two",
                )
                .env(
                    "GatewayApi__Network__CoreApiNodes__1__CoreApiAddress",
                    "",
                )
                .env("GatewayApi__Network__CoreApiNodes__1__Enabled", "false")
                .env("GatewayApi__Network__CoreApiNodes__1__RequestWeighting", "1")
                .env(
                    "GatewayApi__Network__CoreApiNodes__2__Name",
                    "Three",
                )
                .env(
                    "GatewayApi__Network__CoreApiNodes__2__CoreApiAddress",
                    "",
                )
                .env("GatewayApi__Network__CoreApiNodes__2__Enabled", "false")
                .env("GatewayApi__Network__CoreApiNodes__2__RequestWeighting", "1")
                .env(
                    "GatewayApi__Network__CoreApiNodes__3__Name",
                    "Four",
                )
                .env(
                    "GatewayApi__Network__CoreApiNodes__3__CoreApiAddress",
                    "",
                )
                .env("GatewayApi__Network__CoreApiNodes__3__Enabled", "false")
                .env("GatewayApi__Network__CoreApiNodes__3__RequestWeighting", "1")
                .env(
                    "GatewayApi__Network__CoreApiNodes__4__Name",
                    "Five",
                )
                .env(
                    "GatewayApi__Network__CoreApiNodes__4__CoreApiAddress",
                    "",
                )
                .env("GatewayApi__Network__CoreApiNodes__4__Enabled", "false")
                .env("GatewayApi__Network__CoreApiNodes__4__RequestWeighting", "1")
                .env("GatewayApi__Network__CoreApiNodes__MaxAllowedStateVersionLagToBeConsideredSynced", "100")
                .env("GatewayApi__Network__CoreApiNodes__IgnoreNonSyncedNodes", "true")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stdout output of the babylon-gateway process
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

            // Spawn a task to read and process the stdout output of the babylon-gateway process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
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

            // Wait for the babylon-gateway process to exit or for the shutdown token to be cancelled
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
        info!("babylon-gateway shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("babylon-gateway shutdown");
    }
}