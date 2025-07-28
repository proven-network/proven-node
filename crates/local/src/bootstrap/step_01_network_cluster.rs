//! Bootstrap Step 1: Network Cluster Initialization
//!
//! This step handles the initialization of the network cluster, including:
//! - Private key parsing and validation
//! - Governance setup (single node or multi-node)
//! - Network creation and peer discovery
//! - Light core HTTP server setup
//! - Hostname resolution validation

use super::Bootstrap;
use crate::error::Error;
use crate::hosts::check_hostname_resolution;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::any;
use http::StatusCode;
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_core::{Core, CoreOptions};
use proven_engine::{
    EngineBuilder, EngineConfig,
    config::{
        ConsensusConfig, GlobalConsensusConfig, GroupConsensusConfig,
        NetworkConfig as ConsensusNetworkConfig, ServiceConfig, StorageConfig,
    },
};
use proven_http_insecure::InsecureHttpServer;
use proven_network::NetworkManager;
use proven_network::connection_pool::ConnectionPoolConfig;
use proven_storage::StorageManager;
use proven_storage_rocksdb::RocksDbStorage;
use proven_topology::{NodeId, TopologyManager};
use proven_topology::{TopologyAdaptor, Version};
use proven_transport_ws::WebSocketTransport;
use tower_http::cors::CorsLayer;
use url::Url;

#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_lines)]
pub async fn execute<G: TopologyAdaptor>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    // Just use single version based on mock attestation pcrs (deterministic hashes on cargo version)
    let pcrs = bootstrap
        .attestor
        .pcrs()
        .await
        .map_err(|e| Error::Attestation(format!("failed to get PCRs: {e}")))?;
    let version = Version::from_pcrs(pcrs);

    // Check that governance contains the version from attestor
    if !bootstrap
        .config
        .governance
        .get_active_versions()
        .await
        .map_err(|e| Error::Governance(e.to_string()))?
        .contains(&version)
    {
        return Err(Error::Governance(format!(
            "governance does not contain version from attestor: {version:?}"
        )));
    }

    // Simplified setup with new constructor pattern
    let node_id = NodeId::from(bootstrap.config.node_key.verifying_key());
    let governance = Arc::new(bootstrap.config.governance.clone());

    let topology_manager = Arc::new(TopologyManager::new(governance.clone(), node_id.clone()));
    topology_manager
        .start()
        .await
        .map_err(|e| Error::Topology(e.to_string()))?;

    let transport = Arc::new(WebSocketTransport::new());

    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
        transport.clone(),
        topology_manager.clone(),
        bootstrap.config.node_key.clone(),
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(bootstrap.attestor.clone()),
    ));

    // Start network manager first - this creates the listener
    network_manager
        .start()
        .await
        .map_err(|e| Error::Bootable(Box::new(e)))?;

    // Now mount the WebSocket endpoint - this will use the existing listener
    let engine_router = transport
        .mount_into_router(Router::new())
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;

    let my_node = topology_manager
        .get_node_by_id(&node_id)
        .await
        .ok_or(Error::Topology(format!("node not found: {node_id}")))?;

    let origin = my_node.origin().to_string();
    bootstrap.node = Some(my_node);
    let origin_url =
        Url::parse(&origin).map_err(|e| Error::Topology(format!("invalid origin: {e}")))?;

    // Check /etc/hosts to ensure the node's FQDN is properly configured
    check_hostname_resolution(origin_url.host_str().unwrap()).await?;

    // Create engine configuration
    let engine_config = EngineConfig {
        node_name: format!("node-{node_id}"),
        services: ServiceConfig::default(),
        consensus: ConsensusConfig {
            global: GlobalConsensusConfig {
                election_timeout_min: Duration::from_millis(1500),
                election_timeout_max: Duration::from_millis(3000),
                heartbeat_interval: Duration::from_millis(500),
                snapshot_interval: 1000,
                max_entries_per_append: 64,
            },
            group: GroupConsensusConfig {
                election_timeout_min: Duration::from_millis(150),
                election_timeout_max: Duration::from_millis(300),
                heartbeat_interval: Duration::from_millis(50),
                snapshot_interval: 1000,
                max_entries_per_append: 64,
            },
        },
        network: ConsensusNetworkConfig {
            listen_addr: format!("0.0.0.0:{}", bootstrap.config.port),
            public_addr: origin.clone(),
            connection_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        },
        storage: StorageConfig {
            path: "./data".to_string(),
            max_log_size: 1024 * 1024 * 1024,
            compaction_interval: Duration::from_secs(3600),
            cache_size: 1000,
        },
    };

    // Create RocksDB storage
    let storage_adaptor = RocksDbStorage::new(&bootstrap.config.rocksdb_store_dir)
        .await
        .map_err(|e| Error::Storage(format!("Failed to create RocksDB storage: {e}")))?;

    let storage_manager = Arc::new(StorageManager::new(storage_adaptor));

    // Create HTTP server with a base router
    let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, bootstrap.config.port));
    let http_server = InsecureHttpServer::new(
        http_sock_addr,
        Router::new()
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive()),
    );

    let core = Core::new(CoreOptions {
        attestor: bootstrap.attestor.clone(),
        engine_router,
        governance: bootstrap.config.governance.clone(),
        http_server,
        origin: origin.to_string(),
    });

    // Start the Core first to ensure HTTP server with WebSocket routes is ready
    core.start().await.map_err(Error::Bootable)?;

    // Give the HTTP server time to fully start and bind
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now build and start the engine
    let mut engine = EngineBuilder::new(node_id.clone())
        .with_config(engine_config)
        .with_network(network_manager.clone())
        .with_topology(topology_manager.clone())
        .with_storage(storage_manager)
        .build()
        .await
        .map_err(|e| Error::Consensus(format!("failed to build engine: {e}")))?;

    engine
        .start()
        .await
        .map_err(|e| Error::Bootable(Box::new(e)))?;

    // Save the engine client for use in later steps
    bootstrap.engine_client = Some(Arc::new(engine.client()));

    // TODO: Add components to bootable vec

    bootstrap.bootstrapping_core = Some(core);

    // TODO: Do better cluster formation check
    // Sleep to let cluster initialize
    tokio::time::sleep(Duration::from_secs(3)).await;

    Ok(())
}
