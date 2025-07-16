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
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use proven_messaging::subject::{PublishableSubject, Subject};
use proven_messaging::subscription_handler::SubscriptionHandler;
use proven_messaging::subscription_responder::SubscriptionResponder;
use proven_messaging_nats::subject::{NatsSubject, NatsSubjectOptions};
use proven_messaging_nats::subscription::NatsSubscription;
use proven_network::{Peer, ProvenNetwork};
use proven_topology::TopologyAdaptor;
use regex::Regex;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_rustls_acme::acme::LETS_ENCRYPT_PRODUCTION_DIRECTORY;
use tokio_rustls_acme::{AccountCache, CertCache};
use tracing::{debug, error, info, trace, warn};

const PEER_REMOVAL_SUBJECT: &str = "NATS_SERVER_PEER_REMOVAL";

/// Message sent to notify other NATS servers to remove this server as a peer
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerRemovalNotification {
    /// The server name to remove from the cluster
    pub server_name: String,
    /// Timestamp when the notification was sent
    pub timestamp: u64,
}

impl TryFrom<Bytes> for PeerRemovalNotification {
    type Error = serde_json::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&bytes)
    }
}

impl TryInto<Bytes> for PeerRemovalNotification {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let json = serde_json::to_vec(&self)?;
        Ok(Bytes::from(json))
    }
}

static CONFIG_TEMPLATE: &str = include_str!("../templates/nats-server.conf");
static CLUSTER_CONFIG_TEMPLATE: &str = include_str!("../templates/cluster.conf");
static CLUSTER_NO_TLS_CONFIG_TEMPLATE: &str = include_str!("../templates/cluster-no-tls.conf");

const PEER_DISCOVERY_INTERVAL: u64 = 300; // 5 minutes

/// Type alias for peer removal subscription
type PeerRemovalSubscription<G, A, S> = NatsSubscription<
    PeerRemovalHandler<G, A, S>,
    PeerRemovalNotification,
    serde_json::Error,
    serde_json::Error,
>;

/// Regex pattern for matching NATS server log lines
static LOG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[\d+\] \d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} (\[[A-Z]+\]) (.*)")
        .expect("Invalid regex pattern")
});

/// Options for configuring a `NatsServer`.
pub struct NatsServerOptions<G, A, S>
where
    G: TopologyAdaptor,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    /// The store for certificates. Set to `None` if the server is not using TLS.
    pub cert_store: Option<CertStore<S>>,

    /// Optional path to the NATS CLI binary if it is not in the PATH.
    pub cli_bin_dir: Option<PathBuf>,

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

    /// Optional path to the NATS server binary if it is not in the PATH.
    pub server_bin_dir: Option<PathBuf>,

    /// The name of the server.
    pub server_name: String,

    /// The directory to store data in.
    pub store_dir: PathBuf,
}

/// NATS CLI application implementing the `IsolatedApplication` trait
struct NatsPeerRemoveApp {
    /// The path to the nats-server executable
    bin_dir: PathBuf,

    /// The client URL
    client_url: String,

    /// The path to the nats-server executable
    executable_path: String,

    /// The replica name to remove from the cluster
    replica_name: String,

    /// The system password
    system_password: String,

    /// The system username
    system_username: String,
}

#[async_trait]
impl IsolatedApplication for NatsPeerRemoveApp {
    fn args(&self) -> Vec<String> {
        vec![
            "-s".to_string(),
            self.client_url.to_string(),
            format!("--user={}", self.system_username),
            format!("--password={}", self.system_password),
            "server".to_string(),
            "raft".to_string(),
            "peer-remove".to_string(),
            "--force".to_string(),
            self.replica_name.to_string(),
        ]
    }

    fn executable(&self) -> &str {
        &self.executable_path
    }

    fn handle_stdout(&self, line: &str) {
        info!(target: "nats-peer-remove", "{}", line);
    }

    fn handle_stderr(&self, line: &str) {
        error!(target: "nats-peer-remove", "{}", line);
    }

    fn name(&self) -> &'static str {
        "nats-peer-remove"
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![VolumeMount::new(self.bin_dir.clone(), self.bin_dir.clone())]
    }
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
        // Switch spammy messages to trace level
        if line.contains("rid:")
            || line.contains("Reloaded")
            || line.contains("Trapped")
            || line.contains("i/o timeout")
            || line.contains("connection refused")
        {
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

/// Empty response for peer removal notifications
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerRemovalResponse;

impl TryFrom<Bytes> for PeerRemovalResponse {
    type Error = serde_json::Error;

    fn try_from(_bytes: Bytes) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

impl TryInto<Bytes> for PeerRemovalResponse {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from("{}"))
    }
}

/// Subscription handler for peer removal notifications
#[derive(Clone)]
struct PeerRemovalHandler<G, A, S>
where
    G: TopologyAdaptor,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    server: NatsServer<G, A, S>,
    /// Cached client URL to avoid mutex contention
    client_url: String,
}

impl<G, A, S> std::fmt::Debug for PeerRemovalHandler<G, A, S>
where
    G: TopologyAdaptor,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerRemovalHandler")
            .field("server_name", &self.server.server_name)
            .field("client_url", &self.client_url)
            .finish()
    }
}

#[async_trait]
impl<G, A, S> SubscriptionHandler<PeerRemovalNotification, serde_json::Error, serde_json::Error>
    for PeerRemovalHandler<G, A, S>
where
    G: TopologyAdaptor,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    type Error = Error;
    type ResponseType = PeerRemovalResponse;
    type ResponseDeserializationError = serde_json::Error;
    type ResponseSerializationError = serde_json::Error;

    async fn handle<R>(
        &self,
        message: PeerRemovalNotification,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: SubscriptionResponder<
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
    {
        info!(
            "Received peer removal notification for server: {}",
            message.server_name
        );

        // Don't remove ourselves from the cluster
        if message.server_name == self.server.server_name {
            debug!("Ignoring peer removal notification for ourselves");
            return Ok(responder.no_reply().await);
        }

        // Give a moment for replica to enter lame duck mode
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        // Create the peer removal application
        let remove_peer_app = NatsPeerRemoveApp {
            bin_dir: self.server.cli_bin_dir.clone(),
            client_url: self.client_url.clone(),
            executable_path: self
                .server
                .cli_bin_dir
                .join("nats")
                .to_string_lossy()
                .to_string(),
            replica_name: message.server_name.clone(),
            system_password: self.server.system_password.clone(),
            system_username: self.server.system_username.clone(),
        };

        // Attempt to remove the peer from the cluster
        match proven_isolation::spawn(remove_peer_app).await {
            Ok(process) => {
                let exit_status = process.wait().await;

                if exit_status.success() {
                    info!(
                        "successfully removed peer {} from cluster",
                        message.server_name
                    );
                } else {
                    info!(
                        "failed to remove peer {} - likely another node has already removed it",
                        message.server_name
                    );
                }
            }
            Err(e) => {
                error!(
                    "Failed to spawn peer removal process for {}: {}",
                    message.server_name, e
                );
            }
        }

        Ok(responder.no_reply().await)
    }
}

/// Represents an isolated NATS server with network discovery.
///
/// This server automatically listens for peer removal notifications on the
/// "nats.cluster.peer-removal" subject and removes peers from the cluster when
/// they announce their shutdown. It also sends its own removal notification
/// before shutting down.
#[derive(Clone)]
pub struct NatsServer<G, A, S>
where
    G: TopologyAdaptor,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    /// The store for certificates. Set to `None` if the server is not using TLS.
    cert_store: Option<CertStore<S>>,

    /// Path to the directory containing the NATS CLI binary
    cli_bin_dir: PathBuf,

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

    /// The peer removal subscription
    peer_removal_subscription: Arc<Mutex<Option<PeerRemovalSubscription<G, A, S>>>>,

    /// The isolated process running NATS server
    process: Arc<Mutex<Option<IsolatedProcess>>>,

    /// Path to the directory containing the NATS server binary
    server_bin_dir: PathBuf,

    /// The server name
    server_name: String,

    /// The store directory
    store_dir: PathBuf,

    /// The system password
    system_password: String,

    /// The system username
    system_username: String,

    /// The topology update task
    topology_update_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl<G, A, S> NatsServer<G, A, S>
where
    G: TopologyAdaptor,
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
            cli_bin_dir,
            cert_store,
            client_port,
            config_dir,
            debug,
            http_port,
            network,
            server_bin_dir,
            server_name,
            store_dir,
        }: NatsServerOptions<G, A, S>,
    ) -> Result<Self, Error> {
        let cli_bin_dir = match cli_bin_dir {
            Some(dir) => dir,
            None => match which::which("nats") {
                Ok(path) => path.parent().unwrap().to_path_buf(),
                Err(_) => return Err(Error::BinaryNotFound),
            },
        };

        let server_bin_dir = match server_bin_dir {
            Some(dir) => dir,
            None => match which::which("nats-server") {
                Ok(path) => path.parent().unwrap().to_path_buf(),
                Err(_) => return Err(Error::BinaryNotFound),
            },
        };

        // Generate random system username and password for this run
        let system_username = format!("sys-{}", uuid::Uuid::new_v4());
        let system_password = format!("pwd-{}", uuid::Uuid::new_v4());

        Ok(Self {
            cert_store,
            cli_bin_dir,
            client_port,
            config_dir,
            debug,
            http_port,
            network,
            peer_removal_subscription: Arc::new(Mutex::new(None)),
            process: Arc::new(Mutex::new(None)),
            server_bin_dir,
            server_name,
            store_dir,
            system_password,
            system_username,
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
            .replace("{system_username}", &self.system_username)
            .replace("{system_password}", &self.system_password)
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

    /// Sends a peer removal notification to other NATS servers before shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the notification fails to send, but errors are logged and not propagated
    /// to avoid failing the shutdown process.
    async fn send_peer_removal_notification(&self) -> Result<(), Error> {
        info!("Sending peer removal notification to other NATS servers...");

        // Create a client to send the notification
        let client = match self.build_client().await {
            Ok(client) => client,
            Err(e) => {
                error!(
                    "Failed to create NATS client for peer removal notification: {}",
                    e
                );
                return Err(e);
            }
        };

        // Create the notification message
        let notification = PeerRemovalNotification {
            server_name: self.server_name.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Create the subject for peer removal notifications
        let subject_options = NatsSubjectOptions {
            client: client.clone(),
        };

        let subject = match NatsSubject::<
            PeerRemovalNotification,
            serde_json::Error,
            serde_json::Error,
        >::new(PEER_REMOVAL_SUBJECT, subject_options)
        {
            Ok(subject) => subject,
            Err(e) => {
                error!("Failed to create peer removal notification subject: {}", e);
                return Ok(()); // Don't fail shutdown on subject creation error
            }
        };

        // Send the notification
        match subject.publish(notification).await {
            Ok(()) => {
                info!("Peer removal notification sent successfully");
                // Give a moment for the message to be processed
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                Ok(())
            }
            Err(e) => {
                error!("Failed to send peer removal notification: {}", e);
                Ok(()) // Don't fail shutdown on message send error
            }
        }
    }

    /// Starts listening for peer removal notifications
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription fails to start.
    async fn start_peer_removal_listener(&self) -> Result<(), Error> {
        info!("Starting peer removal notification listener...");

        // Create a client for the subscription
        let client = self.build_client().await?;

        // Create the subject for peer removal notifications
        let subject_options = NatsSubjectOptions {
            client: client.clone(),
        };

        let subject =
            NatsSubject::<PeerRemovalNotification, serde_json::Error, serde_json::Error>::new(
                PEER_REMOVAL_SUBJECT,
                subject_options,
            )
            .map_err(|e| {
                error!("Failed to create peer removal subscription subject: {}", e);
                Error::RemovePeer
            })?;

        // Create the handler
        let handler = PeerRemovalHandler {
            server: self.clone(),
            client_url: self.get_client_url().await,
        };

        // Subscribe to the subject
        let subscription = subject.subscribe(handler).await.map_err(|e| {
            error!("Failed to subscribe to peer removal notifications: {}", e);
            Error::RemovePeer
        })?;

        // Store the subscription
        self.peer_removal_subscription
            .lock()
            .await
            .replace(subscription);

        info!("Peer removal notification listener started successfully");
        Ok(())
    }
}

#[async_trait]
impl<G, A, S> Bootable for NatsServer<G, A, S>
where
    G: TopologyAdaptor,
    A: Attestor,
    S: Store<Bytes, Infallible, Infallible>,
{
    fn bootable_name(&self) -> &'static str {
        "nats-server"
    }

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
            bin_dir: self.server_bin_dir.clone(),
            client_port: self.client_port,
            cluster_port,
            debug: self.debug,
            config_dir: self.config_dir.clone(),
            executable_path: self
                .server_bin_dir
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

        // Start peer removal listener (allow it to fail without stopping server startup)
        if let Err(e) = self.start_peer_removal_listener().await {
            warn!("Failed to start peer removal listener: {}", e);
        }

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

        // Send peer removal notification to other servers before going into lame duck mode
        if let Err(e) = self.send_peer_removal_notification().await {
            warn!(
                "Failed to send peer removal notification during shutdown: {}",
                e
            );
            // Continue with shutdown even if notification fails
        }

        // Give a moment for the notification to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Stop the topology update task
        let taken_task = self.topology_update_task.lock().await.take();
        if let Some(task) = taken_task {
            task.abort();
        }

        // Stop the peer removal subscription
        let taken_subscription = self.peer_removal_subscription.lock().await.take();
        if taken_subscription.is_some() {
            info!("Peer removal subscription stopped");
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
