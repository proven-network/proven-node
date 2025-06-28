//! Configures and runs a NATS server for inter-node communication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;
use proven_bootable::Bootable;
use proven_cert_store::CertStore;
use proven_nats_monitor::NatsMonitor;
use proven_store::Store;

use std::convert::Infallible;
use std::fmt::Write;
use std::sync::{Arc, LazyLock};
use std::{net::SocketAddr, path::PathBuf};

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use nix::sys::signal::Signal;
use pem::{Pem, encode};
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use proven_network::{Peer, ProvenNetwork};
use regex::Regex;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_rustls_acme::acme::LETS_ENCRYPT_PRODUCTION_DIRECTORY;
use tokio_rustls_acme::{AccountCache, CertCache};
use tracing::{debug, error, info, trace, warn};

static CONFIG_TEMPLATE: &str = include_str!("../templates/nats-server.conf");
static CLUSTER_CONFIG_TEMPLATE: &str = include_str!("../templates/cluster.conf");
static CLUSTER_NO_TLS_CONFIG_TEMPLATE: &str = include_str!("../templates/cluster-no-tls.conf");

const PEER_DISCOVERY_INTERVAL: u64 = 300; // 5 minutes

/// Regex pattern for matching NATS server log lines
static LOG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[\d+\] \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} (\[[A-Z]+\]) (.*)")
        .expect("Invalid regex pattern")
});

/// Options for configuring a `NatsServer`.
pub struct NatsServerOptions<G, A, S>
where
    G: Governance,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    /// Optional path to the NATS server binary if it is not in the PATH.
    pub bin_dir: Option<PathBuf>,

    /// The store for certificates. Set to `None` if the server is not using TLS.
    pub cert_store: Option<CertStore<S>>,

    /// The port to listen for client connections on.
    pub client_port: u16,

    /// The directory to store configuration in.
    pub config_dir: PathBuf,

    /// Whether to enable debug logging.
    pub debug: bool,

    /// The port to listen for http connections on.
    pub http_port: u16,

    /// Network for peer discovery.
    pub network: ProvenNetwork<G, A>,

    /// The name of the server.
    pub server_name: String,

    /// The directory to store data in.
    pub store_dir: PathBuf,
}

/// NATS server application implementing the `IsolatedApplication` trait
struct NatsServerApp {
    /// The path to the nats-server executable
    bin_dir: PathBuf,

    /// The configuration directory inside the container
    config_dir: PathBuf,

    /// The client listen port
    client_port: u16,

    /// The cluster listen port
    cluster_port: u16,

    /// Whether to enable debug logging
    debug: bool,

    /// The path to the nats-server executable
    executable_path: String,

    /// The http listen port
    http_port: u16,

    /// The store directory
    store_dir: PathBuf,

    /// Whether to wait for a cluster to be formed
    wait_for_cluster: bool,
}

#[async_trait]
impl IsolatedApplication for NatsServerApp {
    fn args(&self) -> Vec<String> {
        let mut args = vec![
            "--config".to_string(),
            self.config_dir
                .join("nats-server.conf")
                .to_string_lossy()
                .to_string(),
        ];

        if self.debug {
            args.extend_from_slice(&["-DV".to_string()]);
        }

        args
    }

    fn executable(&self) -> &str {
        &self.executable_path
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_stderr(&self, line: &str) {
        // Ignore any lines contains "rid:" as it's a route message and are typically spammy
        if line.contains("rid:") {
            return;
        }

        if let Some(caps) = LOG_REGEX.captures(line) {
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
        self.handle_stderr(line);
    }

    fn name(&self) -> &'static str {
        "nats-server"
    }

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> bool {
        // Try to connect to the NATS server with async-nats
        let server_url = format!("nats://{}:{}", info.ip_address, self.client_port);
        match async_nats::connect(&server_url).await {
            Ok(client) => {
                // // We can drop the client right away as we only wanted to test the connection
                drop(client);

                if self.wait_for_cluster {
                    // Check route information (/routez) endpoints to see if cluster is ready
                    let monitor =
                        NatsMonitor::new(SocketAddr::new(info.ip_address, self.http_port));

                    match monitor.get_routez().await {
                        Ok(routez) => {
                            // Check there's at least one route with an uptime of more than 5s
                            routez
                                .routes
                                .iter()
                                .any(|route| route.uptime.unwrap() > Duration::from_secs(5))
                        }
                        Err(e) => {
                            error!("Failed to get route info: {}", e);

                            false
                        }
                    }
                } else {
                    // If we're not using a cluster, we can just return based on the client connecting
                    true
                }
            }
            Err(_) => {
                // Connection failed, server not ready yet
                false
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

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![self.cluster_port]
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(self.bin_dir.clone(), self.bin_dir.clone()),
            VolumeMount::new(self.config_dir.clone(), self.config_dir.clone()),
            VolumeMount::new(self.store_dir.clone(), self.store_dir.clone()),
        ]
    }
}

/// Represents an isolated NATS server with network discovery.
#[derive(Clone)]
pub struct NatsServer<G, A, S>
where
    G: Governance,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    /// Path to the directory containing the NATS server binary
    bin_dir: PathBuf,

    /// The store for certificates. Set to `None` if the server is not using TLS.
    cert_store: Option<CertStore<S>>,

    /// The client listen port
    client_port: u16,

    /// The config directory
    config_dir: PathBuf,

    /// Enable debug logging
    debug: bool,

    /// The http listen port
    http_port: u16,

    /// Network for peer discovery
    network: ProvenNetwork<G, A>,

    /// The isolated process running NATS server
    process: Arc<Mutex<Option<IsolatedProcess>>>,

    /// The server name
    server_name: String,

    /// The store directory
    store_dir: PathBuf,

    /// The topology update task
    topology_update_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl<G, A, S> NatsServer<G, A, S>
where
    G: Governance,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    /// Creates a new `NatsServer` with the specified options.
    ///
    /// # Errors
    ///
    /// Returns an error if the NATS server binary is not found.
    ///
    /// # Panics
    ///
    /// Panics if the NATS server binary is not found and `bin_dir` is `None`.
    pub fn new(
        NatsServerOptions {
            bin_dir,
            cert_store,
            client_port,
            config_dir,
            debug,
            http_port,
            network,
            server_name,
            store_dir,
        }: NatsServerOptions<G, A, S>,
    ) -> Result<Self, Error> {
        let bin_dir = match bin_dir {
            Some(dir) => dir,
            None => match which::which("nats-server") {
                Ok(path) => path.parent().unwrap().to_path_buf(),
                Err(_) => return Err(Error::BinaryNotFound),
            },
        };

        Ok(Self {
            bin_dir,
            cert_store,
            client_port,
            config_dir,
            debug,
            http_port,
            network,
            process: Arc::new(Mutex::new(None)),
            server_name,
            store_dir,
            topology_update_task: Arc::new(Mutex::new(None)),
        })
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
    fn start_topology_update_task(&self) -> JoinHandle<()> {
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
        })
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
    async fn update_nats_config_with_peers(&self, peers: &[Peer<A>]) -> Result<(), Error> {
        // Build routes for cluster configuration
        let mut routes = String::new();
        let mut valid_peer_count = 0;

        for peer in peers {
            // Try to get the NATS cluster endpoint
            match peer.nats_cluster_endpoint().await {
                Ok(endpoint) => {
                    write!(routes, "\"{endpoint}\"\n    ").unwrap();
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

        // Create the config directory if it doesn't exist
        tokio::fs::create_dir_all(&self.config_dir)
            .await
            .map_err(|e| Error::Io("failed to create config directory", e))?;

        let nats_cluster_endpoint = self.network.nats_cluster_endpoint().await?;

        // Start with the basic configuration
        let mut config = CONFIG_TEMPLATE
            .replace("{server_name}", &self.server_name)
            .replace("{client_addr}", &format!("0.0.0.0:{}", self.client_port))
            .replace("{http_addr}", &format!("0.0.0.0:{}", self.http_port))
            .replace("{store_dir}", &self.store_dir.to_string_lossy());

        // Only add cluster configuration if there are valid peers
        if valid_peer_count > 0 {
            // If we have a cert store, we should apply TLS configuration
            // TODO: Needs testing against production ACME server
            let cluster_config = if let Some(cert_store) = &self.cert_store {
                if let (Some(cert_bytes), Some(pkcs8_bytes)) = (
                    cert_store
                        .load_cert(
                            &[nats_cluster_endpoint.host_str().unwrap().to_string()],
                            LETS_ENCRYPT_PRODUCTION_DIRECTORY,
                        )
                        .await?,
                    cert_store
                        .load_account(
                            &[nats_cluster_endpoint.host_str().unwrap().to_string()],
                            LETS_ENCRYPT_PRODUCTION_DIRECTORY,
                        )
                        .await?,
                ) {
                    let cert_file = self.config_dir.join("cert.pem");
                    let key_file = self.config_dir.join("key.pem");

                    tokio::fs::write(&cert_file, cert_bytes)
                        .await
                        .map_err(|e| Error::Io("failed to write cert file", e))?;

                    // Convert the loaded account bytes (PKCS#8) to PEM format
                    let pem = Pem::new(String::from("PRIVATE KEY"), pkcs8_bytes);
                    let key_pem_string = encode(&pem);
                    tokio::fs::write(&key_file, key_pem_string.as_bytes())
                        .await
                        .map_err(|e| Error::Io("failed to write key file", e))?;

                    #[allow(clippy::literal_string_with_formatting_args)]
                    CLUSTER_CONFIG_TEMPLATE
                        .replace("{cert_file}", &cert_file.to_string_lossy())
                        .replace("{key_file}", &key_file.to_string_lossy())
                } else {
                    warn!(
                        "Failed to load certificate for NATS cluster endpoint: {}",
                        nats_cluster_endpoint
                    );

                    // TODO: This is a temporary solution to allow the server to start without TLS
                    // Should probably just retry until the HTTPS server populates the cert store
                    CLUSTER_NO_TLS_CONFIG_TEMPLATE.to_string()
                }
            } else {
                CLUSTER_NO_TLS_CONFIG_TEMPLATE.to_string()
            };

            #[allow(clippy::literal_string_with_formatting_args)]
            config.push_str(
                &cluster_config
                    .replace(
                        "{cluster_port}",
                        &nats_cluster_endpoint.port().unwrap().to_string(),
                    )
                    .replace("{cluster_node_user}", nats_cluster_endpoint.username())
                    .replace(
                        "{cluster_node_password}",
                        nats_cluster_endpoint.password().unwrap(),
                    )
                    .replace("{cluster_routes}", routes.trim()),
            );
        }

        tokio::fs::write(self.config_dir.join("nats-server.conf"), config)
            .await
            .map_err(|e| Error::Io("failed to write nats-server.conf", e))?;

        // Reload the configuration if the server is running
        if let Some(process) = &*self.process.lock().await {
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

    /// Returns the client URL for the NATS server.
    #[must_use]
    pub async fn get_client_url(&self) -> String {
        (*self.process.lock().await).as_ref().map_or_else(
            || format!("nats://127.0.0.1:{}", self.client_port),
            |process| {
                process.container_ip().map_or_else(
                    || format!("nats://127.0.0.1:{}", self.client_port),
                    |container_ip| format!("nats://{}:{}", container_ip, self.client_port),
                )
            },
        )
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

#[async_trait]
impl<G, A, S> Bootable for NatsServer<G, A, S>
where
    G: Governance,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    /// Starts the NATS server in an isolated environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to start.
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.process.lock().await.is_some() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        debug!("Starting isolated NATS server...");

        // Create necessary directories
        tokio::fs::create_dir_all(self.store_dir.join("jetstream"))
            .await
            .map_err(|e| Error::Io("failed to create jetstream directory", e))?;

        // Initialize topology from network
        self.update_topology().await?;

        let cluster_port = self.network.nats_cluster_endpoint().await?.port().unwrap();
        let wait_for_cluster = self.network.get_peers().await?.len() > 1;

        // Prepare the NATS server application
        let app = NatsServerApp {
            bin_dir: self.bin_dir.clone(),
            client_port: self.client_port,
            cluster_port,
            debug: self.debug,
            config_dir: self.config_dir.clone(),
            executable_path: self
                .bin_dir
                .join("nats-server")
                .to_string_lossy()
                .to_string(),
            http_port: self.http_port,
            store_dir: self.store_dir.clone(),
            wait_for_cluster,
        };

        // Spawn the isolated process
        let process = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        // Store the process for later access
        self.process.lock().await.replace(process);

        // // Start periodic topology update task
        let topology_update_task = self.start_topology_update_task();
        self.topology_update_task
            .lock()
            .await
            .replace(topology_update_task);

        info!("NATS server started");

        Ok(())
    }

    /// Shuts down the NATS server.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to shutdown.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down isolated NATS server...");

        // Stop the topology update task
        let taken_task = self.topology_update_task.lock().await.take();
        if let Some(task) = taken_task {
            task.abort();
        }

        // Get the process and shut it down
        let taken_process = self.process.lock().await.take();
        if let Some(process) = taken_process {
            process
                .shutdown()
                .await
                .map_err(|e| Box::new(Error::Isolation(e)))?;

            info!("NATS server shut down successfully");
        } else {
            debug!("No running NATS server to shut down");
        }

        Ok(())
    }

    async fn wait(&self) {
        if let Some(process) = self.process.lock().await.as_ref() {
            process.wait().await;
        }
    }
}
