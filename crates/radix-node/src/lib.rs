//! Configures and runs a local Radix Babylon Node.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;
use proven_bootable::Bootable;
use tokio::sync::Mutex;

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use radix_common::network::NetworkDefinition;
use regex::Regex;
use reqwest::Client;
use strip_ansi_escapes::strip_str;
use tokio::process::Command;
use tracing::{debug, error, info, trace, warn};

use async_trait::async_trait;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};

// Default filenames
static CONFIG_FILENAME: &str = "radix-node.config";
static KEYSTORE_FILENAME: &str = "radix-keystore.ks";
static KEYSTORE_PASS: &str = "notarealpassword"; // Irrelevant as keyfile never leaves TEE

static MAINNET_SEED_NODES: &[&str] = &[
    "radix://node_rdx1qf2x63qx4jdaxj83kkw2yytehvvmu6r2xll5gcp6c9rancmrfsgfw0vnc65@babylon-mainnet-eu-west-1-node0.radixdlt.com",
    "radix://node_rdx1qgxn3eeldj33kd98ha6wkjgk4k77z6xm0dv7mwnrkefknjcqsvhuu4gc609@babylon-mainnet-ap-southeast-2-node0.radixdlt.com",
    "radix://node_rdx1qwrrnhzfu99fg3yqgk3ut9vev2pdssv7hxhff80msjmmcj968487uugc0t2@babylon-mainnet-ap-south-1-node0.radixdlt.com",
    "radix://node_rdx1q0gnmwv0fmcp7ecq0znff7yzrt7ggwrp47sa9pssgyvrnl75tvxmvj78u7t@babylon-mainnet-us-east-1-node0.radixdlt.com",
];

static STOKENET_SEED_NODES: &[&str] = &[
    "radix://node_tdx_2_1qv89yg0la2jt429vqp8sxtpg95hj637gards67gpgqy2vuvwe4s5ss0va2y@babylon-stokenet-ap-south-1-node0.radixdlt.com",
    "radix://node_tdx_2_1qvtd9ffdhxyg7meqggr2ezsdfgjre5aqs6jwk5amdhjg86xhurgn5c79t9t@babylon-stokenet-ap-southeast-2-node0.radixdlt.com",
    "radix://node_tdx_2_1qwfh2nn0zx8cut5fqfz6n7pau2f7vdyl89mypldnn4fwlhaeg2tvunp8s8h@babylon-stokenet-eu-west-1-node0.radixdlt.com",
    "radix://node_tdx_2_1qwz237kqdpct5l3yjhmna66uxja2ymrf3x6hh528ng3gtvnwndtn5rsrad4@babylon-stokenet-us-east-1-node1.radixdlt.com",
];

static JAVA_OPTS: &[&str] = &[
    "-Djava.library.path=/apps/radix-node/v1.3.0.2",
    "--enable-preview",
    "-server",
    "-Xms2g",
    "-Xmx12g",
    "-XX:MaxDirectMemorySize=2048m",
    "-XX:+HeapDumpOnOutOfMemoryError",
    "-XX:+UseCompressedOops",
    "-Djavax.net.ssl.trustStore=/etc/ssl/certs/java/cacerts",
    "-Djavax.net.ssl.trustStoreType=jks",
    "-Djava.security.egd=file:/dev/urandom",
    "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
];

// Java log regexp
static JAVA_LOG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2},\d{3} \[(\w+).+\] - (.*)").unwrap()
});

// Rust log regexp
static RUST_LOG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z\s+(\w+) .+: (.*)").unwrap()
});

/// Application struct for running Radix Node in isolation
struct RadixNodeApp {
    config_dir: PathBuf,
    http_port: u16,
    network_definition: NetworkDefinition,
    p2p_port: u16,
    store_dir: PathBuf,
}

#[async_trait]
impl IsolatedApplication for RadixNodeApp {
    fn args(&self) -> Vec<String> {
        let config_path = Path::new("/config").join(CONFIG_FILENAME);

        vec![
            "-config".to_string(),
            config_path.to_string_lossy().to_string(),
        ]
    }

    fn env(&self) -> Vec<(String, String)> {
        vec![
            (
                "RADIX_NODE_KEYSTORE_PASSWORD".to_string(),
                KEYSTORE_PASS.to_string(),
            ),
            ("JAVA_OPTS".to_string(), JAVA_OPTS.join(" ")),
            (
                "JAVA_HOME".to_string(),
                "/usr/lib/jvm/java-17-openjdk-arm64".to_string(),
            ),
            (
                "PATH".to_string(),
                "/usr/lib/jvm/java-17-openjdk-arm64/bin:/usr/bin:/bin".to_string(),
            ),
            (
                "LD_LIBRARY_PATH".to_string(),
                "/usr/lib/jvm/java-17-openjdk-arm64/lib".to_string(),
            ),
        ]
    }

    fn executable(&self) -> &'static str {
        "/apps/radix-node/v1.3.0.2/core-v1.3.0.2/bin/core"
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_stdout(&self, line: &str) {
        if let Some(caps) = JAVA_LOG_REGEX.captures(line) {
            let label = caps.get(1).map_or("UNKNW", |m| m.as_str());
            let message = caps.get(2).map_or(line, |m| m.as_str());
            match label {
                "OFF" => info!("{}", message),
                "FATAL" => error!("{}", message),
                "ERROR" => error!("{}", message),
                "WARN" => warn!("{}", message),
                "INFO" => info!("{}", message),
                "DEBUG" => debug!("{}", message),
                "TRACE" => trace!("{}", message),
                _ => error!("{}", line),
            }
        } else if let Some(caps) = RUST_LOG_REGEX.captures(&strip_str(line)) {
            let label = caps.get(1).map_or("UNKNW", |m| m.as_str());
            let message = caps.get(2).map_or(line, |m| m.as_str());
            match label {
                "DEBUG" => debug!("{}", message),
                "ERROR" => error!("{}", message),
                "INFO" => info!("{}", message),
                "TRACE" => trace!("{}", message),
                "WARN" => warn!("{}", message),
                _ => error!("{}", line),
            }
        } else {
            error!("{}", line);
        }
    }

    fn handle_stderr(&self, line: &str) {
        self.handle_stdout(line);
    }

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> bool {
        let client = Client::new();
        let payload = format!(
            "{{\"network\":\"{}\"}}",
            self.network_definition.logical_name
        );

        let url = format!(
            "http://{}:{}/core/status/network-status",
            info.ip_address, self.http_port
        );

        matches!(client
            .post(&url)
            .body(payload)
            .header("Content-Type", "application/json")
            .send()
            .await, Ok(resp) if resp.status().is_success())
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        5000 // Check every 5 seconds
    }

    fn name(&self) -> &'static str {
        "radix-node"
    }

    fn memory_limit_mb(&self) -> usize {
        1024 * 12 // 12GB
    }

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![self.p2p_port] // Forward P2P
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(&self.store_dir, &PathBuf::from("/data")),
            VolumeMount::new(&self.config_dir, &PathBuf::from("/config")),
            VolumeMount::new("/apps/radix-node/v1.3.0.2", "/apps/radix-node/v1.3.0.2"),
            VolumeMount::new(
                "/etc/java-17-openjdk/security",
                "/etc/java-17-openjdk/security",
            ),
        ]
    }
}

/// Runs a Radix Babylon Node.
#[derive(Clone)]
pub struct RadixNode {
    config_dir: PathBuf,
    host_ip: String,
    http_port: u16,
    network_definition: NetworkDefinition,
    p2p_port: u16,
    process: Arc<Mutex<Option<IsolatedProcess>>>,
    store_dir: PathBuf,
}

/// Options for configuring a `RadixNode`.
pub struct RadixNodeOptions {
    /// The directory to store configuration and keystore files in.
    pub config_dir: PathBuf,

    /// The host IP address.
    pub host_ip: String,

    /// The HTTP port to listen on.
    pub http_port: u16,

    /// The network definition.
    pub network_definition: NetworkDefinition,

    /// The port to listen on.
    pub p2p_port: u16,

    /// The directory to store data in.
    pub store_dir: PathBuf,
}

impl RadixNode {
    /// Creates a new instance of `RadixNode`.
    #[must_use]
    pub fn new(
        RadixNodeOptions {
            host_ip,
            http_port,
            network_definition,
            p2p_port,
            store_dir,
            config_dir,
        }: RadixNodeOptions,
    ) -> Self {
        Self {
            host_ip,
            http_port,
            network_definition,
            p2p_port,
            process: Arc::new(Mutex::new(None)),
            store_dir,
            config_dir,
        }
    }

    /// Returns the HTTP port of the Radix Node.
    #[must_use]
    pub const fn http_port(&self) -> u16 {
        self.http_port
    }

    /// Returns the IP address of the Radix Node.
    ///
    /// # Panics
    ///
    /// Panics if the IP address cannot be determined.
    #[must_use]
    pub async fn ip_address(&self) -> IpAddr {
        self.process
            .lock()
            .await
            .as_ref()
            .map_or(IpAddr::V4(Ipv4Addr::LOCALHOST), |p| {
                p.container_ip().unwrap()
            })
    }

    /// Returns the port of the Radix Node.
    #[must_use]
    pub const fn p2p_port(&self) -> u16 {
        self.p2p_port
    }

    async fn prepare_config(&self) -> Result<(), Error> {
        // Ensure store dir created
        std::fs::create_dir_all(&self.store_dir)
            .map_err(|e| Error::Io("Failed to create store dir", e))?;

        // Ensure config dir created
        std::fs::create_dir_all(&self.config_dir)
            .map_err(|e| Error::Io("Failed to create config dir", e))?;

        let config_path = Path::new(&self.config_dir).join(CONFIG_FILENAME);
        let keystore_path = Path::new("/config").join(KEYSTORE_FILENAME);

        let seed_nodes = match self.network_definition.id {
            1 => MAINNET_SEED_NODES,
            2 => STOKENET_SEED_NODES,
            _ => unreachable!(),
        }
        .join(",");

        let config = format!(
            r"network.host_ip={}
network.id={}
network.p2p.listen_port={}
network.p2p.broadcast_port={}
network.p2p.seed_nodes={}
node.key.path={}
db.location=/data
api.core.bind_address=0.0.0.0
api.core.port={}",
            self.host_ip,
            self.network_definition.id,
            self.p2p_port,
            self.p2p_port,
            seed_nodes,
            keystore_path.to_string_lossy(),
            self.http_port
        );

        let mut config_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&config_path)
            .map_err(|e| Error::Io("Failed to open config file", e))?;

        std::io::Write::write_all(&mut config_file, config.as_bytes())
            .map_err(|e| Error::Io("Failed to write config file", e))?;

        // Generate the node key
        let keystore_path_local = Path::new(&self.config_dir).join(KEYSTORE_FILENAME);
        let output = Command::new("/apps/radix-node/v1.3.0.2/core-v1.3.0.2/bin/keygen")
            .arg("-k")
            .arg(keystore_path_local.to_string_lossy().as_ref())
            .arg("-p")
            .arg(KEYSTORE_PASS)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to generate node key", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::KeyGeneration(stderr.to_string()));
        }

        Ok(())
    }
}

#[async_trait]
impl Bootable for RadixNode {
    fn name(&self) -> &'static str {
        "radix-node"
    }

    /// Starts the Radix Babylon Node.
    ///
    /// # Errors
    ///
    /// This function will return an error if the node is already started, if there is an I/O error,
    /// or if the node process exits with a non-zero status code.
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.process.lock().await.is_some() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        info!("Starting Radix Node...");

        self.prepare_config().await?;

        let app = RadixNodeApp {
            http_port: self.http_port,
            network_definition: self.network_definition.clone(),
            p2p_port: self.p2p_port,
            store_dir: self.store_dir.clone(),
            config_dir: self.config_dir.clone(),
        };

        let process = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        // Store the process for later shutdown
        self.process.lock().await.replace(process);

        info!("Radix Node started");

        Ok(())
    }

    /// Shuts down the server.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("radix-node shutting down...");

        let taken_process = self.process.lock().await.take();
        if let Some(process) = taken_process {
            process
                .shutdown()
                .await
                .map_err(|e| Box::new(Error::Isolation(e)))?;

            info!("radix-node shutdown");
        } else {
            debug!("No running radix-node to shut down");
        }

        Ok(())
    }

    async fn wait(&self) {
        if let Some(process) = self.process.lock().await.as_ref() {
            process.wait().await;
        }
    }
}
