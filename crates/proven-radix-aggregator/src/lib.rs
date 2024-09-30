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

pub struct RadixAggregator {
    postgres_database: String,
    postgres_username: String,
    postgres_password: String,
    skip_vacuum: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl RadixAggregator {
    pub fn new(
        postgres_database: String,
        postgres_username: String,
        postgres_password: String,
        skip_vacuum: bool,
    ) -> Self {
        Self {
            postgres_database,
            postgres_username,
            postgres_password,
            skip_vacuum,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        self.update_config().await?;
        self.run_migrations().await?;
        if !self.skip_vacuum {
            self.vacuum_database().await?;
        }

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the radix-aggregator process
            let mut cmd = Command::new(AGGREGATOR_PATH)
                .current_dir(AGGREGATOR_DIR)
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stdout output of the radix-aggregator process
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

            // Wait for the radix-aggregator process to exit or for the shutdown token to be cancelled
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
            .map_err(Error::ConfigWrite)?;

        config_file
            .write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
            .await
            .map_err(Error::ConfigWrite)?;

        let cmd = Command::new(MIGRATIONS_PATH)
            .current_dir(MIGRATIONS_DIR)
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
            .map_err(Error::ConfigWrite)?;

        config_file
            .write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes())
            .await
            .map_err(Error::ConfigWrite)?;

        Ok(())
    }

    async fn vacuum_database(&self) -> Result<()> {
        info!("vacuuming database...");

        let mut cmd = Command::new("/usr/local/pgsql/bin/vacuumdb")
            .arg("-U")
            .arg(&self.postgres_username)
            .arg("-d")
            .arg(&self.postgres_database)
            .arg("--analyze")
            .arg("--full")
            .arg("--jobs=4")
            .arg("--verbose")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(Error::Spawn)?;

        let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
        let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

        let stdout_writer = tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                info!("{}", line)
            }
        });

        let stderr_writer = tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                error!("{}", line)
            }
        });

        tokio::select! {
            e = cmd.wait() => {
                e.map_err(Error::Spawn)?;
            }
            _ = stdout_writer => {},
            _ = stderr_writer => {},
        }

        Ok(())
    }
}
