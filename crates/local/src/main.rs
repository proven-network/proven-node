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

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use clap::Parser;
use console::style;
use hickory_resolver::Resolver;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation_dev::DevAttestor;
use proven_core::{Core, CoreOptions};
use proven_governance::{Governance, NodeSpecialization};
use proven_governance_mock::MockGovernance;
use proven_http_insecure::InsecureHttpServer;
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::{RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_direct::{DirectSqlStore1, DirectSqlStore2, DirectSqlStore3};
use proven_store_fs::{FsStore1, FsStore2, FsStore3};
use proven_store_memory::{MemoryStore, MemoryStore2};
use radix_common::network::NetworkDefinition;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = 3200)]
    port: u16,

    /// Path to the topology file
    #[arg(long, default_value = "/etc/proven/topology.json")]
    topology_file: PathBuf,

    /// Private key provided directly as an environment variable
    #[arg(long, env = "PRIVATE_KEY", required = true)]
    private_key: String,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish(),
    )?;

    let args = Args::parse();

    info!("Using private key from environment variable");

    // Create the governance mock and initialize it with the private key
    info!("Loading topology file from: {:?}", args.topology_file);
    let governance = MockGovernance::from_topology_file(args.topology_file, vec![])
        .map_err(|e| error::Error::Io(format!("Failed to load topology: {}", e)))?
        .with_private_key(&args.private_key)
        .map_err(|e| error::Error::Io(format!("Failed to initialize governance: {}", e)))?;

    // Get our node from the governance
    let node_config = governance
        .get_self()
        .await
        .map_err(|e| error::Error::Io(format!("Failed to find node in topology: {}", e)))?;

    info!("Found node in topology: {}", node_config.fqdn);

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
    let dns_lookup_result = resolver.lookup_ip(&node_config.fqdn).await;

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
            if !check_hosts_file(&node_config.fqdn) {
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
        if !check_hosts_file(&node_config.fqdn) {
            error!(
                "{} is not configured in hosts file either",
                node_config.fqdn
            );
            show_hosts_file_instructions(&node_config.fqdn);
            std::process::exit(1);
        }
    }

    let challenge_store = MemoryStore2::new();
    let sessions_store = FsStore1::new("/tmp/proven/sessions");

    // Determine network definition based on specializations
    let radix_network_definition = if node_config
        .specializations
        .contains(&NodeSpecialization::RadixStokenet)
    {
        info!("Configuring for RadixStokenet");
        NetworkDefinition::stokenet()
    } else if node_config
        .specializations
        .contains(&NodeSpecialization::RadixMainnet)
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
        primary_hostnames: vec![node_config.fqdn.clone()].into_iter().collect(),
        runtime_pool_manager,
        session_manager,
    });

    let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, args.port));
    let http_server = InsecureHttpServer::new(http_sock_addr);

    // Start the HTTP server and core
    let core_handle = core.start(http_server).await?;

    info!(
        "Node {} started with specializations: {:?}",
        node_config.public_key, node_config.specializations
    );
    info!("Listening on http://{}:{}", node_config.fqdn, args.port);

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
        style(format!("0.0.0.0 {}", hostname)).cyan(),
        style("/etc/hosts").blue(),
    );
    #[cfg(target_family = "windows")]
    error!(
        "Please add {} to {} or configure DNS properly",
        style(format!("0.0.0.0 {}", hostname)).cyan(),
        style(r"C:\Windows\System32\drivers\etc\hosts").blue()
    );
}
