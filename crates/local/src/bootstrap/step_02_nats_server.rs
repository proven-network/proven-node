//! Bootstrap Step 2: NATS Server Initialization
//!
//! This step handles the initialization of the NATS server, including:
//! - NATS server configuration and startup
//! - NATS client creation and connection
//! - Cluster synchronization for multi-node setups

use super::Bootstrap;
use crate::error::Error;

use std::path::PathBuf;
use std::time::Duration;

use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_governance_mock::MockGovernance;
use proven_nats_server::{NatsServer, NatsServerOptions};
use tracing::info;

pub async fn execute(bootstrap: &mut Bootstrap) -> Result<(), Error> {
    let network = bootstrap.network.as_ref().unwrap_or_else(|| {
        panic!("network not set before nats server step");
    });

    let peer_count = network.get_peers().await?.len();

    let nats_server: NatsServer<
        MockGovernance,
        MockAttestor,
        proven_store_fs::FsStore<bytes::Bytes, std::convert::Infallible, std::convert::Infallible>,
    > = NatsServer::new(NatsServerOptions {
        bin_dir: bootstrap.config.nats_bin_dir.clone(),
        cert_store: None,
        client_port: bootstrap.config.nats_client_port,
        config_dir: PathBuf::from("/tmp/nats-config"),
        debug: bootstrap.config.testnet,
        http_port: bootstrap.config.nats_http_port,
        network: network.clone(),
        server_name: network.fqdn().await?,
        store_dir: bootstrap.config.nats_store_dir.clone(),
    })?;

    nats_server.start().await.map_err(Error::Bootable)?;
    let nats_client = nats_server.build_client().await?;

    // Add NATS server to bootables collection
    bootstrap.add_bootable(Box::new(nats_server));
    bootstrap.nats_client = Some(nats_client);

    info!("nats server started");

    if peer_count > 0 {
        // TODO: Wait for cluster to reach consensus
        // Just sleep to simulate for now
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    Ok(())
}
