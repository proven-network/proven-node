//! Binary to bootstrap other components locally.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;
mod hosts;

use error::Result;
use hosts::check_hosts_file;

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use clap::Parser;
use console::style;
use ed25519_dalek::SigningKey;
use hex;
use hickory_resolver::Resolver;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation_dev::DevAttestor;
use proven_core::{Core, CoreOptions};
use proven_governance::{Node, NodeSpecialization};
use proven_governance_mock::MockGovernance;
use proven_http_insecure::InsecureHttpServer;
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::{RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_direct::{DirectSqlStore1, DirectSqlStore2, DirectSqlStore3};
use proven_store_fs::{FsStore1, FsStore2, FsStore3};
use proven_store_memory::{MemoryStore, MemoryStore2};
use radix_common::network::NetworkDefinition;
use serde::{Deserialize, Serialize};
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

/// Node definition in the topology
#[derive(Debug, Serialize, Deserialize)]
struct TopologyNode {
    endpoint: String,
    fqdn: String,
    public_key: String,
    specializations: Vec<String>,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = 3200)]
    port: u16,

    /// Path to the topology file
    #[arg(long, default_value = "/etc/proven/topology.json")]
    topology_file: PathBuf,

    /// Path to the private key file
    #[arg(long, default_value = "/etc/proven/private-key.pem")]
    private_key_file: PathBuf,

    /// Private key provided directly as an environment variable
    /// This takes precedence over the private key file if both are provided
    #[arg(long, env = "PRIVATE_KEY")]
    private_key: Option<String>,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish(),
    )?;

    let args = Args::parse();

    // Load the topology file
    info!("Loading topology file from: {:?}", args.topology_file);
    let mut topology_file = File::open(&args.topology_file)
        .map_err(|e| error::Error::Io(format!("Failed to open topology file: {}", e)))?;

    let mut content = String::new();
    topology_file
        .read_to_string(&mut content)
        .map_err(|e| error::Error::Io(format!("Failed to read topology file: {}", e)))?;

    let topology_nodes: Vec<TopologyNode> = serde_json::from_str(&content)
        .map_err(|e| error::Error::Io(format!("Failed to parse topology file: {}", e)))?;

    // Load the private key - first check if it was provided as an env var
    let private_key = if let Some(key) = &args.private_key {
        info!("Using private key from environment variable");
        key.clone()
    } else {
        // Otherwise, load from file
        info!("Loading private key from: {:?}", args.private_key_file);
        let mut private_key_file = File::open(&args.private_key_file)
            .map_err(|e| error::Error::Io(format!("Failed to open private key file: {}", e)))?;

        let mut private_key = String::new();
        private_key_file
            .read_to_string(&mut private_key)
            .map_err(|e| error::Error::Io(format!("Failed to read private key file: {}", e)))?;

        private_key
    };

    // Find our node in the topology by deriving the public key from the private key
    // First, parse the private key
    let private_key_bytes = hex::decode(private_key.trim())
        .map_err(|e| error::Error::Io(format!("Failed to decode private key as hex: {}", e)))?;

    // We need exactly 32 bytes for ed25519 private key
    let signing_key = SigningKey::try_from(private_key_bytes.as_slice()).map_err(|_| {
        error::Error::Io("Failed to create SigningKey: invalid key length".to_string())
    })?;

    // Derive the public key
    let verifying_key = signing_key.verifying_key();
    let public_key_hex = hex::encode(verifying_key.as_bytes());

    info!("Derived public key: {}", public_key_hex);

    // Find our node in the topology
    let node_config = topology_nodes
        .iter()
        .find(|n| n.public_key == public_key_hex)
        .ok_or_else(|| {
            error::Error::Io(format!(
                "Node with public key {} not found in topology",
                public_key_hex
            ))
        })?;

    info!("Found node in topology: {}", node_config.fqdn);

    // Convert to Governance Nodes
    let governance_nodes: Vec<Node> = topology_nodes
        .iter()
        .map(|n| {
            let mut specializations: HashSet<NodeSpecialization> = HashSet::new();
            for spec in &n.specializations {
                if spec == "radix-mainnet" {
                    specializations.insert(NodeSpecialization::RadixMainnet);
                } else if spec == "radix-stokenet" {
                    specializations.insert(NodeSpecialization::RadixStokenet);
                }
            }

            Node {
                availability_zone: "local".to_string(),
                fqdn: n.endpoint.clone(),
                public_key: n.public_key.clone(),
                region: "local".to_string(),
                specializations,
            }
        })
        .collect();

    // Check /etc/hosts to ensure the node's FQDN is properly configured
    // First try DNS resolution using Hickory resolver
    let resolver = match Resolver::tokio_from_system_conf() {
        Ok(resolver) => resolver,
        Err(e) => {
            error!("Failed to create DNS resolver: {}", e);
            return Err(error::Error::Io(format!(
                "Failed to create DNS resolver: {}",
                e
            )));
        }
    };
    let dns_lookup_result = resolver.lookup_ip(node_config.fqdn.as_str()).await;

    if let Ok(lookup_result) = dns_lookup_result {
        if !lookup_result.iter().collect::<Vec<_>>().is_empty() {
            info!(
                "Hostname {} can be resolved via DNS: {:?}",
                node_config.fqdn,
                lookup_result.iter().collect::<Vec<_>>()
            );
        } else {
            error!(
                "DNS resolution for {} returned no addresses",
                node_config.fqdn
            );
            if !check_hosts_file(node_config.fqdn.as_str()) {
                error!(
                    "{} is not configured in hosts file or DNS",
                    node_config.fqdn
                );
                show_hosts_file_instructions(&node_config.fqdn);
                std::process::exit(1);
            }
        }
    } else {
        error!(
            "Could not resolve {} via DNS: {:?}",
            node_config.fqdn,
            dns_lookup_result.err()
        );
        if !check_hosts_file(node_config.fqdn.as_str()) {
            error!(
                "{} is not configured in hosts file either",
                node_config.fqdn
            );
            show_hosts_file_instructions(&node_config.fqdn);
            std::process::exit(1);
        }
    }

    // Create the governance mock
    let governance = MockGovernance::new(governance_nodes, vec![]);

    let challenge_store = MemoryStore2::new();
    let sessions_store = FsStore1::new("/tmp/proven/sessions");

    // Determine network definition based on specializations
    let radix_network_definition = if node_config
        .specializations
        .contains(&"RadixStokenet".to_string())
    {
        info!("Configuring for RadixStokenet");
        NetworkDefinition::stokenet()
    } else if node_config
        .specializations
        .contains(&"RadixMainnet".to_string())
    {
        info!("Configuring for RadixMainnet");
        NetworkDefinition::mainnet()
    } else {
        info!("No specialization found, defaulting to stokenet");
        NetworkDefinition::stokenet()
    };

    // Determine the Radix Gateway origin based on network
    let radix_gateway_origin = if radix_network_definition.id == 1 {
        "https://mainnet.radixdlt.com".to_string()
    } else {
        "https://stokenet.radixdlt.com".to_string()
    };

    let attestor = DevAttestor;

    let session_manager = SessionManager::new(SessionManagerOptions {
        attestor,
        challenge_store,
        sessions_store,
        radix_gateway_origin: &radix_gateway_origin,
        radix_network_definition: &radix_network_definition,
    });

    let application_manager = ApplicationManager::new(
        MemoryStore::new(),
        DirectSqlStore1::new("/tmp/proven/application_manager.db"),
    );

    let application_store = FsStore2::new("/tmp/proven/kv/application");
    let personal_store = FsStore3::new("/tmp/proven/kv/personal");
    let nft_store = FsStore3::new("/tmp/proven/kv/nft");

    let application_sql_store = DirectSqlStore2::new("/tmp/proven/sql/application");
    let personal_sql_store = DirectSqlStore3::new("/tmp/proven/sql/personal");
    let nft_sql_store = DirectSqlStore3::new("/tmp/proven/sql/nft");

    let file_system_store = MemoryStore::new();

    let radix_nft_verifier = GatewayRadixNftVerifier::new(&radix_gateway_origin);

    let runtime_pool_manager = RuntimePoolManager::new(RuntimePoolManagerOptions {
        application_sql_store,
        application_store,
        file_system_store,
        max_workers: 10,
        nft_sql_store,
        nft_store,
        personal_sql_store,
        personal_store,
        radix_gateway_origin,
        radix_network_definition,
        radix_nft_verifier,
    })
    .await;

    let core = Core::new(CoreOptions {
        application_manager,
        attestor: DevAttestor,
        governance,
        primary_hostnames: vec![format!("proven.local:{}", args.port)]
            .into_iter()
            .collect(),
        runtime_pool_manager,
        session_manager,
    });

    let http_sock_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));
    let http_server = InsecureHttpServer::new(http_sock_addr);

    // Start the HTTP server and core
    let core_handle = core.start(http_server).await?;

    info!(
        "Node {} started with specializations: {:?}",
        node_config.public_key, node_config.specializations
    );
    info!("Listening on http://{}:{}", node_config.fqdn, args.port);

    // Create a /config endpoint response for test script
    tokio::fs::create_dir_all("/tmp/proven").await.ok();
    tokio::fs::write(
        "/tmp/proven/config.json",
        serde_json::to_string(node_config).unwrap(),
    )
    .await
    .ok();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down");
            let () = core.shutdown().await;
        }
        _ = core_handle => {
            error!("Core exited");
        }
    }

    Ok(())
}

fn show_hosts_file_instructions(hostname: &str) {
    #[cfg(target_family = "unix")]
    error!(
        "Please add {} to {} or configure DNS properly",
        style(format!("127.0.0.1 {}", hostname)).cyan(),
        style("/etc/hosts").blue(),
    );
    #[cfg(target_family = "windows")]
    error!(
        "Please add {} to {} or configure DNS properly",
        style(format!("127.0.0.1 {}", hostname)).cyan(),
        style(r"C:\Windows\System32\drivers\etc\hosts").blue()
    );
}
