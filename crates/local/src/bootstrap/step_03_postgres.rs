//! Bootstrap Step 3: `PostgreSQL` Initialization
//!
//! This step handles the initialization of the `PostgreSQL` server, including:
//! - Conditional startup based on node specializations
//! - `PostgreSQL` configuration and database initialization
//! - Support for both Radix mainnet and stokenet databases

use super::Bootstrap;
use crate::error::{Error, Result};

use proven_bootable::Bootable;
use proven_governance::NodeSpecialization;
use proven_postgres::{Postgres, PostgresOptions};
use tracing::info;

static POSTGRES_USERNAME: &str = "your-username";
static POSTGRES_PASSWORD: &str = "your-password";

pub async fn execute(bootstrap: &mut Bootstrap) -> Result<()> {
    let network = bootstrap.network.as_ref().unwrap_or_else(|| {
        panic!("network not set before postgres step");
    });

    if network
        .specializations()
        .await?
        .contains(&NodeSpecialization::RadixMainnet)
        || network
            .specializations()
            .await?
            .contains(&NodeSpecialization::RadixStokenet)
    {
        let postgres = Postgres::new(PostgresOptions {
            password: POSTGRES_PASSWORD.to_string(),
            port: bootstrap.args.postgres_port,
            username: POSTGRES_USERNAME.to_string(),
            skip_vacuum: bootstrap.args.skip_vacuum,
            store_dir: bootstrap.args.postgres_store_dir.clone(),
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
