mod error;

pub use error::{Error, Result};

use std::process::Stdio;
use std::sync::Arc;

use async_nats::Client;
use regex::Regex;
use std::net::SocketAddrV4;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

pub struct NatsServer {
    clients: Arc<Mutex<Vec<Client>>>,
    listen_addr: SocketAddrV4,
    server_name: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl NatsServer {
    /// Creates a new instance of `NatsServer`.
    ///
    /// # Arguments
    ///
    /// * `server_name` - The name of the server.
    /// * `listen_addr` - The address to listen on.
    pub fn new(server_name: String, listen_addr: SocketAddrV4) -> Self {
        Self {
            clients: Arc::new(Mutex::new(Vec::new())),
            listen_addr,
            server_name,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn add_peer(&self, peer_ip: SocketAddrV4) -> Result<()> {
        println!("Adding peer: {}", peer_ip);

        Ok(())
    }

    /// Starts the NATS server.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that can be used to await the completion of the server task.
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        self.mount_tmpfs().await?;
        self.update_nats_config().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the nats-server process
            let mut cmd = Command::new("nats-server")
                .arg("--config")
                .arg("/etc/nats/nats-server.conf")
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stderr output of the nats-server process
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

            // Wait for the nats-server process to exit or for the shutdown token to be cancelled
            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.unwrap();

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                _ = shutdown_token.cancelled() => {
                    let pid = cmd.id().unwrap().to_string();

                    let output = Command::new("kill")
                        .arg("-USR2")
                        .arg(pid)
                        .output()
                        .await
                        .expect("failed to execute process");

                    if !output.status.success() {
                        let e = String::from_utf8_lossy(&output.stderr);
                        error!("Failed to send SIGUSR2 signal: {}", e);
                    }

                    info!("nats server entered lame duck mode. waiting for connections to close...");

                    let _ = cmd.wait().await;

                    Ok(())
                }
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        self.task_tracker.close();

        Ok(server_task)
    }

    /// Shuts down the NATS server.
    pub async fn shutdown(&self) {
        info!("nats server shutting down...");

        info!("flushing existing clients...");
        let clients = self.clients.lock().await;
        for client in clients.iter() {
            if let Err(err) = client.flush().await {
                error!("failed to flush client: {}", err);
            }
        }

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("nats server shutdown");
    }

    /// Builds a NATS client.
    ///
    /// # Returns
    ///
    /// A `Result` containing the built `Client` if successful, or an `Error` if the client failed to connect.
    pub async fn build_client(&self) -> Result<Client> {
        let connect_options = async_nats::ConnectOptions::new()
            .name(format!("client-{}", self.server_name))
            .ignore_discovered_servers();

        let client = async_nats::connect_with_options(
            &format!("nats://{}", self.listen_addr),
            connect_options,
        )
        .await
        .map_err(Error::ClientFailedToConnect)?;

        let mut clients = self.clients.lock().await;
        clients.push(client.clone());

        Ok(client)
    }

    /// Mounts a tmpfs filesystem at `/var/lib/nats/jetstream`. This is required for some JetStream metadata even if streams are in-memory.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    async fn mount_tmpfs(&self) -> Result<()> {
        tokio::fs::create_dir_all("/var/lib/nats/jetstream")
            .await
            .unwrap();

        let cmd = tokio::process::Command::new("mount")
            .arg("-o")
            .arg("size=64m")
            .arg("-t")
            .arg("tmpfs")
            .arg("none")
            .arg("/var/lib/nats/jetstream")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .await;

        info!("{:?}", cmd);

        match cmd {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => Err(Error::NonZeroExitCode(output.status)),
            Err(e) => Err(Error::Spawn(e)),
        }
    }

    /// Updates the NATS server configuration.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    async fn update_nats_config(&self) -> Result<()> {
        let config = format!(
            r#"
            server_name: {}
            listen: {}
            jetstream: enabled
            http: localhost:8222

            jetstream {{
                store_dir: "/var/lib/nats/jetstream"
            }}
        "#,
            self.server_name, self.listen_addr
        );

        tokio::fs::write("/etc/nats/nats-server.conf", config)
            .await
            .map_err(Error::ConfigWrite)?;

        // Run "nats-server --signal reload" to reload the configuration if it is running (task_tracker closed)
        if self.task_tracker.is_closed() {
            let output = Command::new("nats-server")
                .arg("--signal")
                .arg("reload")
                .output()
                .await
                .map_err(Error::Spawn)?;

            if !output.status.success() {
                return Err(Error::NonZeroExitCode(output.status));
            }
        }

        Ok(())
    }
}
