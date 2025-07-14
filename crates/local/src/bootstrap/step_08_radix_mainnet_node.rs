//! Bootstrap Step 9: Radix Mainnet Node Initialization
//!
//! This step handles the initialization of Radix mainnet services, including:
//! - Conditional Radix mainnet node startup based on specializations
//! - Radix node configuration and startup
//! - Radix aggregator configuration and startup
//! - Radix gateway configuration and startup
//! - `PostgreSQL` integration for data persistence

use super::Bootstrap;
use crate::error::Error;

use std::path::PathBuf;

use proven_bootable::Bootable;
use proven_governance::{Governance, NodeSpecialization};
use proven_radix_aggregator::{RadixAggregator, RadixAggregatorOptions};
use proven_radix_gateway::{RadixGateway, RadixGatewayOptions};
use proven_radix_node::{RadixNode, RadixNodeOptions};
use radix_common::prelude::NetworkDefinition;
use tracing::info;

static POSTGRES_USERNAME: &str = "your-username";
static POSTGRES_PASSWORD: &str = "your-password";
static POSTGRES_RADIX_MAINNET_DATABASE: &str = "radix-mainnet-db";

#[allow(clippy::cognitive_complexity)]
pub async fn execute<G: Governance>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    let node = bootstrap.node.as_ref().unwrap_or_else(|| {
        panic!("node not set before radix mainnet node step");
    });

    if node
        .specializations()
        .contains(&NodeSpecialization::RadixMainnet)
    {
        let postgres_ip_address = bootstrap.postgres_ip_address.unwrap_or_else(|| {
            panic!("postgres ip address not set before radix mainnet node step");
        });

        let postgres_port = bootstrap.postgres_port.unwrap_or_else(|| {
            panic!("postgres port not set before radix mainnet node step");
        });

        let radix_mainnet_node = RadixNode::new(RadixNodeOptions {
            config_dir: PathBuf::from("/tmp/radix-node-mainnet"),
            host_ip: bootstrap.external_ip.to_string(),
            http_port: bootstrap.config.radix_mainnet_http_port,
            network_definition: NetworkDefinition::mainnet(),
            p2p_port: bootstrap.config.radix_mainnet_p2p_port,
            store_dir: bootstrap.config.radix_mainnet_store_dir.clone(),
        });

        radix_mainnet_node.start().await.map_err(Error::Bootable)?;

        let radix_node_ip_address = radix_mainnet_node.ip_address().await.to_string();
        let radix_node_port = radix_mainnet_node.http_port();

        info!("radix mainnet node started");

        let radix_mainnet_aggregator = RadixAggregator::new(RadixAggregatorOptions {
            postgres_database: POSTGRES_RADIX_MAINNET_DATABASE.to_string(),
            postgres_ip_address: postgres_ip_address.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_port,
            postgres_username: POSTGRES_USERNAME.to_string(),
            radix_node_ip_address: radix_node_ip_address.clone(),
            radix_node_port,
        });

        radix_mainnet_aggregator
            .start()
            .await
            .map_err(Error::Bootable)?;

        info!("radix-aggregator for radix mainnet started");

        let radix_mainnet_gateway = RadixGateway::new(RadixGatewayOptions {
            postgres_database: POSTGRES_RADIX_MAINNET_DATABASE.to_string(),
            postgres_ip_address: postgres_ip_address.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_port,
            postgres_username: POSTGRES_USERNAME.to_string(),
            radix_node_ip_address,
            radix_node_port,
        });

        radix_mainnet_gateway
            .start()
            .await
            .map_err(Error::Bootable)?;

        // Add all Radix mainnet services to bootables collection
        bootstrap.add_bootable(Box::new(radix_mainnet_node));
        bootstrap.add_bootable(Box::new(radix_mainnet_aggregator));
        bootstrap.add_bootable(Box::new(radix_mainnet_gateway));

        info!("radix mainnet services started");
    }

    Ok(())
}
