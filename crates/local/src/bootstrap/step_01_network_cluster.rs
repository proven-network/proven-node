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
use proven_consensus::Consensus;
use proven_consensus::config::ClusterJoinRetryConfig;
use proven_core::{Core, CoreOptions};
use proven_governance::{Governance, Version};
use proven_http_insecure::InsecureHttpServer;
use proven_network::{ProvenNetwork, ProvenNetworkOptions};
use tower_http::cors::CorsLayer;
use tracing::info;

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

    // TODO: possibly remove this at some point
    let network = ProvenNetwork::new(ProvenNetworkOptions {
        governance: bootstrap.config.governance.clone(),
        attestor: bootstrap.attestor.clone(),
        nats_cluster_port: bootstrap.config.nats_cluster_port,
        private_key: bootstrap.config.node_key.clone(),
    })
    .await?;

    // Check /etc/hosts to ensure the node's FQDN is properly configured
    check_hostname_resolution(network.fqdn().await?.as_str()).await?;

    if !bootstrap.config.allow_single_node {
        // Get peers in a loop until we have at least two others with an Ok nats cluster endpoint
        info!(
            "waiting for at least two other nodes to be started so NATS can boot in cluster mode"
        );
        loop {
            let peers = network.get_peers().await?;
            if peers.len() >= 2 {
                break;
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        info!("Peer nodes are ready");
    }

    let consensus = Arc::new(
        Consensus::new(proven_consensus::ConsensusConfig {
            governance: Arc::new(bootstrap.config.governance.clone()),
            attestor: Arc::new(bootstrap.attestor.clone()),
            signing_key: bootstrap.config.node_key.clone(),
            raft_config: RaftConfig {
                heartbeat_interval: 500,    // 500ms
                election_timeout_min: 1500, // 1.5s
                election_timeout_max: 3000, // 3s
                ..RaftConfig::default()
            },
            transport_config: proven_consensus::TransportConfig::WebSocket,
            storage_config: proven_consensus::StorageConfig::Memory,
            cluster_discovery_timeout: Some(Duration::from_secs(30)),
            cluster_join_retry_config: ClusterJoinRetryConfig {
                max_attempts: 20,
                initial_delay: Duration::from_secs(1),
                max_delay: Duration::from_secs(10),
                request_timeout: Duration::from_secs(5),
            },
        })
        .await
        .unwrap(),
    );

    let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, bootstrap.config.port));
    let http_server = InsecureHttpServer::new(
        http_sock_addr,
        Router::new()
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive()),
    );

    let core = Core::new(CoreOptions {
        consensus: consensus.clone(),
        http_server,
        network: network.clone(),
    });
    core.start().await.map_err(Error::Bootable)?;

    bootstrap.consensus = Some(consensus);

    bootstrap.network = Some(network);
    bootstrap.bootstrapping_core = Some(core);

    // Sleep to allow NATS to boot in cluster mode
    tokio::time::sleep(Duration::from_secs(5)).await;

    Ok(())
}
