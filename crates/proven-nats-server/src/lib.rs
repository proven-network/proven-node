mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use async_nats::Client;
use regex::Regex;
use std::net::SocketAddrV4;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

pub struct NatsServer {
    listen_addr: SocketAddrV4,
    server_name: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl NatsServer {
    pub fn new(server_name: String, listen_addr: SocketAddrV4) -> Self {
        Self {
            listen_addr,
            server_name,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        self.update_nats_config().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            let mut cmd = Command::new("nats-server")
                .arg("--config")
                .arg("/etc/nats/nats-server.conf")
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                let re = Regex::new(
                    r"\[\d+\] (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6}) (\[[A-Z]+\]) (.*)",
                )
                .unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(caps) = re.captures(&line) {
                        let label = caps.get(2).unwrap().as_str();
                        let message = caps.get(3).unwrap().as_str();
                        match label {
                            "[INF]" => info!("{}", message),
                            "[DBG]" => debug!("{}", message),
                            "[WRN]" => warn!("{}", message),
                            "[ERR]" => error!("{}", message),
                            "[FTL]" => error!("{}", message),
                            "[TRC]" => trace!("{}", message),
                            _ => error!("{}", line),
                        }
                    } else {
                        error!("{}", line);
                    }
                }
            });

            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.unwrap();

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                _ = shutdown_token.cancelled() => {
                    cmd.kill().await.unwrap();

                    Ok(())
                }
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        self.task_tracker.close();

        Ok(server_task)
    }

    pub async fn shutdown(&self) {
        info!("nats server shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("nats server shutdown");
    }

    pub async fn build_client(&self) -> Result<Client> {
        let client = async_nats::connect(&format!("nats://{}", self.listen_addr))
            .await
            .map_err(Error::ClientFailedToConnect)?;

        Ok(client)
    }

    async fn update_nats_config(&self) -> Result<()> {
        let config = format!(
            r#"
            server_name: {}
            listen: {}
            jetstream: enabled

            jetstream {{
                store_dir: "/var/lib/nats/jetstream"
            }}
        "#,
            self.server_name, self.listen_addr
        );

        tokio::fs::write("/etc/nats/nats-server.conf", config)
            .await
            .map_err(Error::ConfigWrite)
    }
}
