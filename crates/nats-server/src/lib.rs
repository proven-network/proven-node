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
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_network::{Peer, ProvenNetwork};
use regex::Regex;
use std::net::SocketAddrV4;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace, warn};

static CONFIG_TEMPLATE: &str = include_str!("../templates/nats-server.conf");
static CLUSTER_CONFIG_TEMPLATE: &str = include_str!("../templates/cluster.conf");

const PEER_DISCOVERY_INTERVAL: u64 = 300; // 5 minutes

/// Runs a NATS server for inter-node communication.
#[derive(Clone)]
pub struct NatsServer<G, A>
where
    G: Governance,
    A: Attestor,
{
    bin_dir: Option<String>,
    client_listen_addr: SocketAddrV4,
    clients: Arc<Mutex<Vec<Client>>>,
    config_dir: String,
    debug: bool,
    network: ProvenNetwork<G, A>,
    server_name: String,
    store_dir: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Options for configuring a `NatsServer`.
pub struct NatsServerOptions<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Optional path to the NATS server binary if it is not in the PATH.
    pub bin_dir: Option<String>,

    /// The address to listen on.
    pub client_listen_addr: SocketAddrV4,

    /// The directory to store configuration in.
    pub config_dir: String,

    /// Whether to enable debug logging.
    pub debug: bool,

    /// Network for peer discovery.
    pub network: ProvenNetwork<G, A>,

    /// The name of the server.
    pub server_name: String,

    /// The directory to store data in.
    pub store_dir: String,
}

impl<G, A> NatsServer<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Creates a new instance of `NatsServer`.
    #[must_use]
    pub fn new(
        NatsServerOptions {
            bin_dir,
            client_listen_addr,
            config_dir,
            debug,
            network,
            server_name,
            store_dir,
        }: NatsServerOptions<G, A>,
    ) -> Self {
        Self {
            bin_dir,
            clients: Arc::new(Mutex::new(Vec::new())),
            client_listen_addr,
            config_dir,
            debug,
            network,
            server_name,
            store_dir,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
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

        // Initialize topology from network
        self.update_topology().await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let debug = self.debug;

        let config_dir = format!("{}/nats-server.conf", self.config_dir);
        let bin_dir = self
            .bin_dir
            .as_deref()
            .map(|d| format!("{}/", d))
            .unwrap_or_default();
        let server_task = self.task_tracker.spawn(async move {
            let mut args = vec!["--config", &config_dir];

            if debug {
                args.extend_from_slice(&["-DV"]);
            }

            let mut cmd = Command::new(format!("{}nats-server", bin_dir));
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

        // TODO: Do a better ready check here
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Start periodic topology update task
        self.start_topology_update_task()?;

        self.task_tracker.close();

        Ok(server_task)
    }

    /// Updates topology from network and updates NATS configuration.
    ///
    /// # Errors
    ///
    /// This function will return an error if it fails to get the topology or update the configuration.
    async fn update_topology(&self) -> Result<()> {
        info!("Fetching peers from network...");
        match self.network.get_peers().await {
            Ok(peers) => {
                info!("Retrieved {} peers from network", peers.len());
                self.update_nats_config_with_peers(&peers).await
            }
            Err(e) => {
                error!("Failed to get peers from network: {}", e);
                Err(Error::ProvenNetwork(e))
            }
        }
    }

    /// Starts a task that periodically updates the topology from network.
    ///
    /// # Errors
    ///
    /// This function will return an error if it fails to start the task.
    fn start_topology_update_task(&self) -> Result<()> {
        let server_name = self.server_name.clone();
        let shutdown_token = self.shutdown_token.clone();

        // We need self reference for updating config
        let self_clone = self.clone();

        self.task_tracker.spawn(async move {
            let update_interval = Duration::from_secs(PEER_DISCOVERY_INTERVAL);
            let mut interval = tokio::time::interval(update_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        info!("[{}] Checking for topology updates...", server_name);
                        match self_clone.network.get_peers().await {
                            Ok(peers) => {
                                info!("[{}] Retrieved {} peers from network", server_name, peers.len());
                                // Update config with new peers
                                if let Err(e) = self_clone.update_nats_config_with_peers(&peers).await {
                                    error!("[{}] Failed to update NATS config: {}", server_name, e);
                                }
                            }
                            Err(e) => {
                                error!("[{}] Failed to get peers from network: {}", server_name, e);
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        info!("[{}] Stopping topology update task", server_name);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Updates the NATS server configuration with peer nodes from the topology.
    ///
    /// # Arguments
    ///
    /// * `peers` - The peer nodes in the topology.
    ///
    /// # Errors
    ///
    /// This function will return an error if it fails to update the configuration.
    async fn update_nats_config_with_peers(&self, peers: &[Peer]) -> Result<()> {
        // Build routes for cluster configuration
        let mut routes = String::new();
        let mut valid_peer_count = 0;

        for peer in peers {
            // Try to get the NATS cluster endpoint
            match peer.nats_cluster_endpoint().await {
                Ok(endpoint) => {
                    routes.push_str(&format!("{}\n    ", endpoint));
                    valid_peer_count += 1;
                }
                Err(e) => {
                    // Log the error but continue with other peers
                    warn!("Failed to get NATS cluster endpoint for peer: {}", e);
                }
            }
        }

        // Log the number of valid peers found
        info!(
            "Found {} valid peers for NATS cluster configuration",
            valid_peer_count
        );

        let nats_cluster_endpoint = self.network.nats_cluster_endpoint().await?;

        // Start with the basic configuration
        let mut config = CONFIG_TEMPLATE
            .replace("{server_name}", &self.server_name)
            .replace("{client_listen_addr}", &self.client_listen_addr.to_string())
            .replace("{store_dir}", &self.store_dir);

        // Only add cluster.routes configuration if there are valid peers
        if valid_peer_count > 0 {
            config.push_str(
                &CLUSTER_CONFIG_TEMPLATE
                    .replace(
                        "{cluster_port}",
                        &nats_cluster_endpoint.port().unwrap().to_string(),
                    )
                    .replace("{cluster_node_user}", &nats_cluster_endpoint.username())
                    .replace(
                        "{cluster_node_password}",
                        &nats_cluster_endpoint.password().unwrap(),
                    )
                    .replace("{cluster_routes}", &routes.trim()),
            );
        }

        info!("{}", config);

        tokio::fs::create_dir_all(&self.config_dir)
            .await
            .map_err(|e| Error::Io("failed to create /etc/nats", e))?;

        tokio::fs::write(format!("{}/nats-server.conf", self.config_dir), config)
            .await
            .map_err(|e| Error::Io("failed to write nats-server.conf", e))?;

        // Run "nats-server --signal reload" to reload the configuration if it is running (task_tracker closed)
        if self.task_tracker.is_closed() {
            let bin_dir = self
                .bin_dir
                .as_deref()
                .map(|d| format!("{}/", d))
                .unwrap_or_default();
            let output = Command::new(format!("{}nats-server", bin_dir))
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
            &format!("nats://{}", self.client_listen_addr),
            connect_options,
        )
        .await
        .map_err(Error::ClientFailedToConnect)?;

        self.clients.lock().await.push(client.clone());

        Ok(client)
    }
}
