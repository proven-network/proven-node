//! Bootstrap Step 10: Radix Stokenet Node Initialization
//!
//! This step handles the initialization of Radix stokenet services, including:
//! - Conditional Radix stokenet node startup based on specializations
//! - Radix node configuration and startup
//! - Radix aggregator configuration and startup
//! - Radix gateway configuration and startup
//! - `PostgreSQL` integration for data persistence

use super::Bootstrap;
use crate::error::{Error, Result};

use std::path::PathBuf;

use proven_bootable::Bootable;
use proven_governance::NodeSpecialization;
use proven_radix_aggregator::{RadixAggregator, RadixAggregatorOptions};
use proven_radix_gateway::{RadixGateway, RadixGatewayOptions};
use proven_radix_node::{RadixNode, RadixNodeOptions};
use radix_common::prelude::NetworkDefinition;
use tracing::info;

static POSTGRES_USERNAME: &str = "your-username";
static POSTGRES_PASSWORD: &str = "your-password";
static POSTGRES_RADIX_STOKENET_DATABASE: &str = "radix-stokenet-db";

pub async fn execute(bootstrap: &mut Bootstrap) -> Result<()> {
    let network = bootstrap.network.as_ref().unwrap_or_else(|| {
        panic!("network not set before radix stokenet node step");
    });

    if network
        .specializations()
        .await?
        .contains(&NodeSpecialization::RadixStokenet)
    {
        let postgres_ip_address = bootstrap.postgres_ip_address.unwrap_or_else(|| {
            panic!("postgres ip address not set before radix stokenet node step");
        });

        let postgres_port = bootstrap.postgres_port.unwrap_or_else(|| {
            panic!("postgres port not set before radix stokenet node step");
        });

        let radix_stokenet_node = RadixNode::new(RadixNodeOptions {
            config_dir: PathBuf::from("/tmp/radix-node-stokenet"),
            host_ip: bootstrap.external_ip.to_string(),
            http_port: bootstrap.args.radix_stokenet_http_port,
            network_definition: NetworkDefinition::stokenet(),
            p2p_port: bootstrap.args.radix_stokenet_p2p_port,
            store_dir: bootstrap.args.radix_stokenet_store_dir.clone(),
        });

        radix_stokenet_node.start().await.map_err(Error::Bootable)?;

        let radix_node_ip_address = radix_stokenet_node.ip_address().await.to_string();
        let radix_node_port = radix_stokenet_node.http_port();

        info!("radix stokenet node started");

        let radix_stokenet_aggregator = RadixAggregator::new(RadixAggregatorOptions {
            postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
            postgres_ip_address: postgres_ip_address.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_port,
            postgres_username: POSTGRES_USERNAME.to_string(),
            radix_node_ip_address: radix_node_ip_address.clone(),
            radix_node_port,
        });

        radix_stokenet_aggregator
            .start()
            .await
            .map_err(Error::Bootable)?;

        info!("radix-aggregator for radix stokenet started");

        let radix_stokenet_gateway = RadixGateway::new(RadixGatewayOptions {
            postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
            postgres_ip_address: postgres_ip_address.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_port,
            postgres_username: POSTGRES_USERNAME.to_string(),
            radix_node_ip_address,
            radix_node_port,
        });

        radix_stokenet_gateway
            .start()
            .await
            .map_err(Error::Bootable)?;

        // Add all Radix stokenet services to bootables collection
        bootstrap.add_bootable(Box::new(radix_stokenet_node));
        bootstrap.add_bootable(Box::new(radix_stokenet_aggregator));
        bootstrap.add_bootable(Box::new(radix_stokenet_gateway));

        info!("radix stokenet services started");
    }

    Ok(())
}
