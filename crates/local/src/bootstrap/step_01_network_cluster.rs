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
use ed25519_dalek::SigningKey;
use http::StatusCode;
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_core::{LightCore, LightCoreOptions};
use proven_governance::{Governance, Version};
use proven_governance_mock::MockGovernance;
use proven_http_insecure::InsecureHttpServer;
use proven_network::{ProvenNetwork, ProvenNetworkOptions};
use tower_http::cors::CorsLayer;
use tracing::info;

pub async fn execute(bootstrap: &mut Bootstrap) -> Result<(), Error> {
    // Parse the private key and calculate public key
    let private_key_bytes = hex::decode(bootstrap.config.node_key.trim())
        .map_err(|e| Error::PrivateKey(format!("Failed to decode private key as hex: {e}")))?;

    // We need exactly 32 bytes for ed25519 private key
    let private_key = SigningKey::try_from(private_key_bytes.as_slice()).map_err(|_| {
        Error::PrivateKey("Failed to create SigningKey: invalid key length".to_string())
    })?;

    // Just use single version based on mock attestation pcrs (deterministic hashes on cargo version)
    let pcrs = bootstrap
        .attestor
        .pcrs()
        .await
        .map_err(|e| Error::Attestation(format!("failed to get PCRs: {e}")))?;
    let version = Version::from_pcrs(pcrs);

    let governance = if let Some(ref network_config_path) = bootstrap.config.network_config_path {
        info!(
            "using replication factor 3 with network config from file: {}",
            network_config_path.display()
        );
        MockGovernance::from_network_config_file(network_config_path)
            .map_err(|e| Error::Io(format!("Failed to load network config: {e}")))?
    } else {
        info!("using replication factor 1 as no network config file provided");
        bootstrap.num_replicas = 1;
        MockGovernance::for_single_node(
            format!("http://localhost:{}", bootstrap.config.port),
            &private_key,
            version.clone(),
        )
    };

    // Check that governance contains the version from attestor
    if !governance
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
        governance: governance.clone(),
        attestor: bootstrap.attestor.clone(),
        nats_cluster_port: bootstrap.config.nats_cluster_port,
        private_key,
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

    bootstrap.governance = Some(governance);
    bootstrap.network = Some(network);
    bootstrap.light_core = Some(light_core);

    if peer_count > 0 {
        // TODO: Wait for at least one other node to be started so NATS can boot in cluster mode
        // Just sleep to simulate for now
        tokio::time::sleep(Duration::from_secs(20)).await;
    }

    Ok(())
}
