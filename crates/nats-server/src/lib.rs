//! Configures and runs a NATS server for inter-node communication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::process::Stdio;
use std::sync::Arc;

use async_nats::Client;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use regex::Regex;
use std::net::SocketAddrV4;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

static CONFIG_TEMPLATE: &str = include_str!("../templates/nats-server.conf");

/// Runs a NATS server for inter-node communication.
pub struct NatsServer {
    clients: Arc<Mutex<Vec<Client>>>,
    debug: bool,
    listen_addr: SocketAddrV4,
    server_name: String,
    store_dir: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Options for configuring a `NatsServer`.
pub struct NatsServerOptions {
    /// Whether to enable debug logging.
    pub debug: bool,

    /// The address to listen on.
    pub listen_addr: SocketAddrV4,

    /// The name of the server.
    pub server_name: String,

    /// The directory to store data in.
    pub store_dir: String,
}

impl NatsServer {
    /// Creates a new instance of `NatsServer`.
    #[must_use]
    pub fn new(
        NatsServerOptions {
            debug,
            listen_addr,
            server_name,
            store_dir,
        }: NatsServerOptions,
    ) -> Self {
        Self {
            clients: Arc::new(Mutex::new(Vec::new())),
            debug,
            listen_addr,
            server_name,
            store_dir,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Adds a peer to the NATS server.
    ///
    /// # Arguments
    ///
    /// * `peer_ip` - The IP address of the peer to add.
    ///
    /// # Errors
    ///
    /// This function will return an error if the peer cannot be added.
    pub fn add_peer(&self, peer_ip: SocketAddrV4) -> Result<()> {
        println!("Adding peer: {peer_ip}");

        Ok(())
    }

    /// Starts the NATS server.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that can be used to await the completion of the server task.
    ///
    /// # Errors
    ///
    /// This function will return an error if the server is already started, if there is an issue
    /// with spawning the server process, or if it fails to create the necessary directories.
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        tokio::fs::create_dir_all(format!("{}/jetstream", self.store_dir.as_str()))
            .await
            .map_err(|e| Error::Io("failed to create jetstream directory", e))?;
        self.update_nats_config().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let debug = self.debug;

        let server_task = self.task_tracker.spawn(async move {
            let mut args = vec!["--config", "/etc/nats/nats-server.conf"];

            if debug {
                args.extend_from_slice(&["-DV"]);
            }

            let mut cmd = Command::new("nats-server");
            for arg in args {
                cmd.arg(arg);
            }

            // Start the nats-server process
            let mut cmd = cmd
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| Error::Io("failed to spawn nats-server", e))?;

            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            let re = Regex::new(
                r"\[\d+\] \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} (\[[A-Z]+\]) (.*)",
            )?;

            // Spawn a task to read and process the stderr output of the nats-server process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(caps) = re.captures(&line) {
                        let label = caps.get(1).map_or("[UKW]", |m| m.as_str());
                        let message = caps.get(2).map_or(line.as_str(), |m| m.as_str());
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
                status = cmd.wait() => {
                    let status = status.map_err(|e| Error::Io("failed to get exit status", e))?;

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                () = shutdown_token.cancelled() => {
                    let raw_pid: i32 = cmd.id().ok_or(Error::OutputParse)?.try_into().map_err(|_| Error::BadPid)?;
                    let pid = Pid::from_raw(raw_pid);

                    if let Err(e) = signal::kill(pid, Signal::SIGUSR2) {
                        error!("Failed to send SIGUSR2 signal: {}", e);
                    } else {
                        info!("nats server entered lame duck mode. waiting for connections to close...");
                    }

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
        let clients = self.clients.lock().await.clone();
        for client in &clients {
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
    ///
    /// # Errors
    ///
    /// This function will return an error if the client fails to connect to the NATS server.
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

        self.clients.lock().await.push(client.clone());

        Ok(client)
    }

    /// Updates the NATS server configuration.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    async fn update_nats_config(&self) -> Result<()> {
        let config = CONFIG_TEMPLATE
            .replace("{server_name}", &self.server_name)
            .replace("{listen_addr}", &self.listen_addr.to_string())
            .replace("{store_dir}", &self.store_dir);

        tokio::fs::create_dir_all("/etc/nats")
            .await
            .map_err(|e| Error::Io("failed to create /etc/nats", e))?;

        tokio::fs::write("/etc/nats/nats-server.conf", config)
            .await
            .map_err(|e| Error::Io("failed to write nats-server.conf", e))?;

        // Run "nats-server --signal reload" to reload the configuration if it is running (task_tracker closed)
        if self.task_tracker.is_closed() {
            let output = Command::new("nats-server")
                .arg("--signal")
                .arg("reload")
                .output()
                .await
                .map_err(|e| Error::Io("failed to reload nats-server", e))?;

            if !output.status.success() {
                return Err(Error::NonZeroExitCode(output.status));
            }
        }

        Ok(())
    }
}
