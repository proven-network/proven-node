//! Bootstrap Step 11: Gateway Bootstrapping
//!
//! This step handles the bootstrapping of the gateway services, including:
//! - Lock manager setup
//! - Identity manager configuration
//! - Passkey manager setup
//! - Session manager configuration
//! - Application manager setup
//! - Runtime pool manager configuration
//! - Various store and SQL store configurations
//! - Full gateway bootstrapping

use super::Bootstrap;
use crate::error::Error;

use proven_applications::{ApplicationManagement, ApplicationManager, ApplicationManagerConfig};

use proven_gateway::BootstrapUpgrade;
use proven_identity::{IdentityManager, IdentityManagerConfig};
use proven_passkeys::{PasskeyManagement, PasskeyManager, PasskeyManagerOptions};
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::{
    RpcEndpoints, RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions,
};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_engine::{SqlEngineStore2, SqlEngineStore3};
use proven_store_engine::{EngineStore, EngineStore1, EngineStore2, EngineStore3};
use proven_store_fs::FsStore;
use proven_topology::TopologyAdaptor;
use tracing::info;

static GATEWAY_URL: &str = "http://127.0.0.1:8081";

#[allow(clippy::too_many_lines)]
pub async fn execute<G: TopologyAdaptor>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    // Get the engine client from bootstrap
    let engine_client = bootstrap
        .engine_client
        .as_ref()
        .expect("Engine client not available")
        .clone();

    // Create IdentityManager with engine client
    let identity_manager_config = IdentityManagerConfig {
        stream_prefix: "identity".to_string(),
        leadership_lease_duration: std::time::Duration::from_secs(30),
        leadership_renewal_interval: std::time::Duration::from_secs(10),
        command_timeout: std::time::Duration::from_secs(30),
    };

    let identity_manager =
        IdentityManager::new(engine_client.clone(), identity_manager_config).await?;

    let passkey_manager = PasskeyManager::new(PasskeyManagerOptions {
        passkeys_store: EngineStore::new(engine_client.clone()),
    });

    let sessions_manager = SessionManager::new(SessionManagerOptions {
        attestor: bootstrap.attestor.clone(),
        sessions_store: EngineStore1::new(engine_client.clone()),
    });

    let application_manager_config = ApplicationManagerConfig {
        stream_prefix: "applications".to_string(),
        leadership_lease_duration: std::time::Duration::from_secs(30),
        leadership_renewal_interval: std::time::Duration::from_secs(10),
        command_timeout: std::time::Duration::from_secs(30),
    };

    let application_manager =
        ApplicationManager::new(engine_client.clone(), application_manager_config).await?;

    let applications = application_manager.list_all_applications().await.unwrap();
    info!("current application count: {}", applications.len());

    let application_store = EngineStore2::new(engine_client.clone());
    let application_sql_store = SqlEngineStore2::new(engine_client.clone(), "APPLICATION_SQL");
    let personal_store = EngineStore3::new(engine_client.clone());
    let personal_sql_store = SqlEngineStore3::new(engine_client.clone(), "PERSONAL_SQL");
    let nft_store = EngineStore3::new(engine_client.clone());
    let nft_sql_store = SqlEngineStore3::new(engine_client.clone(), "NFT_SQL");
    let file_system_store = FsStore::new("/tmp/proven/file_systems");

    let radix_nft_verifier = GatewayRadixNftVerifier::new(GATEWAY_URL);

    let runtime_pool_manager = RuntimePoolManager::new(RuntimePoolManagerOptions {
        application_sql_store,
        application_store,
        file_system_store,
        max_workers: 10,
        nft_sql_store,
        nft_store,
        personal_sql_store,
        personal_store,
        radix_nft_verifier,
        rpc_endpoints: RpcEndpoints {
            bitcoin_mainnet: bootstrap.bitcoin_mainnet_node_rpc_endpoint.clone(),
            bitcoin_testnet: bootstrap.bitcoin_testnet_node_rpc_endpoint.clone(),
            ethereum_holesky: bootstrap.ethereum_holesky_rpc_endpoint.clone(),
            ethereum_mainnet: bootstrap.ethereum_mainnet_rpc_endpoint.clone(),
            ethereum_sepolia: bootstrap.ethereum_sepolia_rpc_endpoint.clone(),
            radix_mainnet: bootstrap.radix_mainnet_rpc_endpoint.clone(),
            radix_stokenet: bootstrap.radix_stokenet_rpc_endpoint.clone(),
        },
    })
    .await;

    let core = bootstrap.bootstrapping_gateway.take().unwrap();

    // Bootstrap the core (this modifies the core in-place)
    core.bootstrap(BootstrapUpgrade {
        application_manager,
        identity_manager,
        passkey_manager,
        runtime_pool_manager,
        sessions_manager,
    })
    .await
    .unwrap();

    // Add core to bootables collection
    bootstrap.add_bootable(Box::new(core));

    info!("core started");

    Ok(())
}
