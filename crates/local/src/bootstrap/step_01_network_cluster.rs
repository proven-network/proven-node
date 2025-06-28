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
use std::time::Duration;

use axum::Router;
use axum::routing::any;
use http::StatusCode;
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_core::{LightCore, LightCoreOptions};
use proven_governance::{Governance, Version};
use proven_http_insecure::InsecureHttpServer;
use proven_network::{ProvenNetwork, ProvenNetworkOptions};
use tower_http::cors::CorsLayer;

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

    let network = ProvenNetwork::new(ProvenNetworkOptions {
        governance: bootstrap.config.governance.clone(),
        attestor: bootstrap.attestor.clone(),
        nats_cluster_port: bootstrap.config.nats_cluster_port,
        private_key: bootstrap.config.node_key.clone(),
    })
    .await?;

    let peer_count = network.get_peers().await?.len();

    // Check /etc/hosts to ensure the node's FQDN is properly configured
    check_hostname_resolution(network.fqdn().await?.as_str()).await?;

    let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, bootstrap.config.port));
    let http_server = InsecureHttpServer::new(
        http_sock_addr,
        Router::new()
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive()),
    );

    let light_core = LightCore::new(LightCoreOptions {
        http_server,
        network: network.clone(),
    });
    light_core.start().await.map_err(Error::Bootable)?;

    bootstrap.network = Some(network);
    bootstrap.light_core = Some(light_core);

    if peer_count > 0 {
        // TODO: Wait for at least one other node to be started so NATS can boot in cluster mode
        // Just sleep to simulate for now
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    Ok(())
}
