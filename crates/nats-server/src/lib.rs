//! Configures and runs a NATS server for inter-node communication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::sync::Arc;

use async_nats::Client;
use async_trait::async_trait;
use nix::sys::signal::Signal;
use once_cell::sync::Lazy;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use proven_network::{Peer, ProvenNetwork};
use regex::Regex;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, trace, warn};

static CONFIG_TEMPLATE: &str = include_str!("../templates/nats-server.conf");
static CLUSTER_CONFIG_TEMPLATE: &str = include_str!("../templates/cluster.conf");

const PEER_DISCOVERY_INTERVAL: u64 = 300; // 5 minutes

/// Regex pattern for matching NATS server log lines
static LOG_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\[\d+\] \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} (\[[A-Z]+\]) (.*)")
        .expect("Invalid regex pattern")
});

/// Options for configuring a `NatsServer`.
pub struct NatsServerOptions<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Optional path to the NATS server binary if it is not in the PATH.
    pub bin_dir: Option<String>,

    /// The port to listen for client connections on.
    pub client_listen_port: u16,

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

/// NATS server application implementing the IsolatedApplication trait
struct NatsServerApp {
    /// The path to the nats-server executable
    bin_dir: String,

    /// The configuration directory inside the container
    config_dir: String,

    /// The client listen port
    client_listen_port: u16,

    /// Whether to enable debug logging
    debug: bool,

    /// The path to the nats-server executable
    executable_path: String,

    /// The store directory
    store_dir: String,
}

#[async_trait]
impl IsolatedApplication for NatsServerApp {
    fn args(&self) -> Vec<String> {
        let mut args = vec![
            "--config".to_string(),
            format!("{}/nats-server.conf", self.config_dir),
        ];

        if self.debug {
            args.extend_from_slice(&["-DV".to_string()]);
        }

        args
    }

    fn executable(&self) -> &str {
        &self.executable_path
    }

    fn handle_stderr(&self, line: &str) {
        if let Some(caps) = LOG_PATTERN.captures(line) {
            let label = caps.get(1).map_or("[UKW]", |m| m.as_str());
            let message = caps.get(2).map_or(line, |m| m.as_str());
            match label {
                "[INF]" => info!(target: "nats-server", "{}", message),
                "[DBG]" => debug!(target: "nats-server", "{}", message),
                "[WRN]" => warn!(target: "nats-server", "{}", message),
                "[ERR]" => error!(target: "nats-server", "{}", message),
                "[FTL]" => error!(target: "nats-server", "{}", message),
                "[TRC]" => trace!(target: "nats-server", "{}", message),
                _ => error!(target: "nats-server", "{}", line),
            }
        } else {
            error!(target: "nats-server", "{}", line);
        }
    }

    fn handle_stdout(&self, line: &str) {
        self.handle_stderr(line)
    }

    fn name(&self) -> &str {
        "nats-server"
    }

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> Result<bool, Box<dyn StdError>> {
        // Try to connect to the NATS server with async-nats
        let server_url = format!("nats://{}:{}", info.ip_address, self.client_listen_port);
        match async_nats::connect(&server_url).await {
            Ok(client) => {
                // Connection successful, server is ready
                // We can drop the client right away as we only wanted to test the connection
                drop(client);
                Ok(true)
            }
            Err(_) => {
                // Connection failed, server not ready yet
                Ok(false)
            }
        }
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        1000 // Check every second
    }

    fn memory_limit_mb(&self) -> usize {
        512 // 512MB should be enough for a NATS server
    }

    fn shutdown_signal(&self) -> Signal {
        Signal::SIGUSR2 // SIGUSR2 puts server into "lame duck" mode
    }

    fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(40) // "lame duck" mode might take 30 seconds to evict clients
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(self.config_dir.clone(), self.config_dir.clone()),
            VolumeMount::new(self.store_dir.clone(), self.store_dir.clone()),
            VolumeMount::new(self.bin_dir.clone(), "/apps/nats/v2.11.0".to_string()),
        ]
    }
}

/// Represents an isolated NATS server with network discovery.
#[derive(Clone)]
pub struct NatsServer<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Path to the directory containing the NATS server binary
    bin_dir: String,

    /// The client listen port
    client_listen_port: u16,

    /// The config directory
    config_dir: String,

    /// Enable debug logging
    debug: bool,

    /// Network for peer discovery
    network: ProvenNetwork<G, A>,

    /// The isolated process running NATS server
    process: Arc<Mutex<Option<IsolatedProcess>>>,

    /// The server name
    server_name: String,

    /// The store directory
    store_dir: String,
}

impl<G, A> NatsServer<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Creates a new `NatsServer` with the specified options.
    #[must_use]
    pub fn new(
        NatsServerOptions {
            bin_dir,
            client_listen_port,
            config_dir,
            debug,
            network,
            server_name,
            store_dir,
        }: NatsServerOptions<G, A>,
    ) -> Result<Self, Error> {
        let bin_dir = match bin_dir {
            Some(dir) => dir,
            None => match which::which("nats-server") {
                Ok(path) => path.parent().unwrap().to_str().unwrap().to_string(),
                Err(_) => return Err(Error::BinaryNotFound),
            },
        };

        Ok(Self {
            bin_dir,
            client_listen_port,
            config_dir,
            debug,
            network,
            process: Arc::new(Mutex::new(None)),
            server_name,
            store_dir,
        })
    }

    /// Starts the NATS server in an isolated environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to start.
    pub async fn start(&self) -> Result<JoinHandle<()>, Error> {
        if self.process.lock().await.is_some() {
            return Err(Error::AlreadyStarted);
        }

        debug!("Starting isolated NATS server...");

        // Create necessary directories
        tokio::fs::create_dir_all(format!("{}/jetstream", self.store_dir.as_str()))
            .await
            .map_err(|e| Error::Io("failed to create jetstream directory", e))?;

        // Initialize topology from network
        self.update_topology().await?;

        // Prepare the NATS server application
        let app = NatsServerApp {
            bin_dir: self.bin_dir.clone(),
            client_listen_port: self.client_listen_port,
            debug: self.debug,
            config_dir: self.config_dir.clone(),
            executable_path: format!("{}/nats-server", self.bin_dir),
            store_dir: self.store_dir.clone(),
        };

        // Spawn the isolated process
        let (process, join_handle) = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        // Store the process for later access
        *self.process.lock().await = Some(process);

        // Start periodic topology update task
        self.start_topology_update_task()?;

        info!("NATS server started");

        Ok(join_handle)
    }

    /// Updates topology from network and updates NATS configuration.
    ///
    /// # Errors
    ///
    /// This function will return an error if it fails to get the topology or update the configuration.
    async fn update_topology(&self) -> Result<(), Error> {
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
    fn start_topology_update_task(&self) -> Result<(), Error> {
        let server_name = self.server_name.clone();

        // We need self reference for updating config
        let self_clone = self.clone();
        let process = self.process.clone();

        tokio::spawn(async move {
            let update_interval = Duration::from_secs(PEER_DISCOVERY_INTERVAL);
            let mut interval = tokio::time::interval(update_interval);

            loop {
                if let Some(process) = &*process.lock().await {
                    if !process.running().await {
                        break;
                    }
                }

                info!("[{}] Checking for topology updates...", server_name);
                match self_clone.network.get_peers().await {
                    Ok(peers) => {
                        info!(
                            "[{}] Retrieved {} peers from network",
                            server_name,
                            peers.len()
                        );
                        // Update config with new peers
                        if let Err(e) = self_clone.update_nats_config_with_peers(&peers).await {
                            error!("[{}] Failed to update NATS config: {}", server_name, e);
                        }
                    }
                    Err(e) => {
                        error!("[{}] Failed to get peers from network: {}", server_name, e);
                    }
                }

                interval.tick().await;
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
    async fn update_nats_config_with_peers(&self, peers: &[Peer]) -> Result<(), Error> {
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
            .replace("{client_listen_addr}", "0.0.0.0:4222") // Listen on all interfaces inside the container
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
            .map_err(|e| Error::Io("failed to create config directory", e))?;

        tokio::fs::write(format!("{}/nats-server.conf", self.config_dir), config)
            .await
            .map_err(|e| Error::Io("failed to write nats-server.conf", e))?;

        // Reload the configuration if the server is running
        let process_guard = self.process.lock().await;
        if let Some(process) = &*process_guard {
            // Send SIGHUP to reload configuration
            if let Err(e) = process.signal(Signal::SIGHUP) {
                warn!(
                    "Failed to send SIGHUP signal to reload NATS configuration: {}",
                    e
                );
            } else {
                info!("NATS configuration reload signal sent");
            }
        }

        Ok(())
    }

    /// Shuts down the NATS server.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to shutdown.
    pub async fn shutdown(&self) -> Result<(), Error> {
        info!("Shutting down isolated NATS server...");

        // Get the process and shut it down
        let mut process_guard = self.process.lock().await;
        if let Some(process) = process_guard.take() {
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("NATS server shut down successfully");
        } else {
            debug!("No running NATS server to shut down");
        }

        Ok(())
    }

    /// Returns the client URL for the NATS server.
    #[must_use]
    pub async fn get_client_url(&self) -> String {
        match &*self.process.lock().await {
            Some(process) => {
                if let Some(container_ip) = process.container_ip() {
                    return format!("nats://{}:{}", container_ip, self.client_listen_port);
                } else {
                    return format!("nats://127.0.0.1:{}", self.client_listen_port);
                }
            }
            None => format!("nats://127.0.0.1:{}", self.client_listen_port),
        }
    }

    /// Builds a NATS client.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to connect.
    pub async fn build_client(&self) -> Result<Client, Error> {
        let connect_options = async_nats::ConnectOptions::new()
            .name(format!("client-{}", self.server_name))
            .ignore_discovered_servers();

        let client = async_nats::connect_with_options(self.get_client_url().await, connect_options)
            .await
            .map_err(Error::ClientFailedToConnect)?;

        Ok(client)
    }
}
