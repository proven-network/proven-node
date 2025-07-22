//! Bootstrap Step 3: `PostgreSQL` Initialization
//!
//! This step handles the initialization of the `PostgreSQL` server, including:
//! - Conditional startup based on node specializations
//! - `PostgreSQL` configuration and database initialization
//! - Support for both Radix mainnet and stokenet databases

use super::Bootstrap;
use crate::error::Error;

use proven_bootable::Bootable;
use proven_postgres::{Postgres, PostgresOptions};
use proven_topology::{NodeSpecialization, TopologyAdaptor};
use tracing::info;

static POSTGRES_USERNAME: &str = "your-username";
static POSTGRES_PASSWORD: &str = "your-password";

pub async fn execute<G: TopologyAdaptor>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    let node = bootstrap.node.as_ref().unwrap_or_else(|| {
        panic!("node not set before postgres step");
    });

    if node
        .specializations()
        .contains(&NodeSpecialization::RadixMainnet)
        || node
            .specializations()
            .contains(&NodeSpecialization::RadixStokenet)
    {
        let postgres = Postgres::new(PostgresOptions {
            password: POSTGRES_PASSWORD.to_string(),
            port: bootstrap.config.postgres_port,
            username: POSTGRES_USERNAME.to_string(),
            skip_vacuum: bootstrap.config.postgres_skip_vacuum,
            store_dir: bootstrap.config.postgres_store_dir.clone(),
        });

        postgres.start().await.map_err(Error::Bootable)?;

        // Set postgres ip address and port for later use by other steps
        bootstrap.postgres_ip_address = Some(postgres.ip_address().await);
        bootstrap.postgres_port = Some(postgres.port());

        // Add Postgres to bootables collection
        bootstrap.add_bootable(Box::new(postgres));

        info!("postgres for radix stokenet started");
    }

    Ok(())
}
