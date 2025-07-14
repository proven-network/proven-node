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
use openraft::Config as RaftConfig;
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_consensus::EngineBuilder;
use proven_consensus::config::{ClusterJoinRetryConfig, StorageConfig};
use proven_core::{Core, CoreOptions};
use proven_governance::{Governance, Version};
use proven_http_insecure::InsecureHttpServer;
use proven_network::{NetworkManager, TopologyManager};
use proven_topology::NodeId;
use proven_transport::HttpIntegratedTransport;
use proven_transport_ws::{WebsocketConfig, WebsocketTransport};
use proven_verification::{AttestationVerifier, ConnectionVerifier, CoseHandler};
use tower_http::cors::CorsLayer;
use url::Url;

#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_lines)]
pub async fn execute<G: Governance>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
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

    // TODO: simplify setup of all these components
    let node_id = NodeId::from(bootstrap.config.node_key.verifying_key());
    let governance = Arc::new(bootstrap.config.governance.clone());

    let topology_manager = Arc::new(
        TopologyManager::new(governance.clone(), node_id.clone())
            .await
            .map_err(|e| Error::Topology(e.to_string()))?,
    );
    let cose_handler = Arc::new(CoseHandler::new(bootstrap.config.node_key.clone()));

    let attestation_verifier = Arc::new(AttestationVerifier::new(
        bootstrap.config.governance.clone(),
        bootstrap.attestor.clone(),
    ));

    let connection_verifier = Arc::new(ConnectionVerifier::new(
        attestation_verifier,
        cose_handler,
        node_id.clone(),
    ));

    let websocket_config = WebsocketConfig::default();

    let transport = WebsocketTransport::new(
        websocket_config,
        bootstrap.config.node_key.clone(),
        connection_verifier,
        topology_manager.clone(),
    );

    let engine_router = transport
        .create_router_integration()
        .map_err(|e| Error::Transport(e.to_string()))?;

    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
        transport,
        topology_manager.clone(),
    ));

    // TODO: fix bootable trait for network manager so we can start here

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

    let (engine, _engine_client) = EngineBuilder::new()
        .governance(governance)
        .signing_key(bootstrap.config.node_key.clone())
        .network_manager(network_manager)
        .topology_manager(topology_manager)
        .raft_config(RaftConfig {
            heartbeat_interval: 500,    // 500ms
            election_timeout_min: 1500, // 1.5s
            election_timeout_max: 3000, // 3s
            ..RaftConfig::default()
        })
        .storage_config(StorageConfig::Memory)
        .cluster_discovery_timeout(Duration::from_secs(30))
        .cluster_join_retry_config(ClusterJoinRetryConfig {
            max_attempts: 20,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            request_timeout: Duration::from_secs(5),
        })
        .build()
        .await
        .map_err(|e| Error::Consensus(format!("failed to build engine: {e}")))?;

    engine
        .start()
        .await
        .map_err(|e| Error::Bootable(Box::new(e)))?;

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

    core.start().await.map_err(Error::Bootable)?;

    // TODO: Add components to bootable vec

    bootstrap.bootstrapping_core = Some(core);

    Ok(())
}
