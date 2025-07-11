//! Bootstrap Step 11: Core Initialization
//!
//! This step handles the initialization of the core services, including:
//! - Lock manager setup
//! - Identity manager configuration
//! - Passkey manager setup
//! - Session manager configuration
//! - Application manager setup
//! - Runtime pool manager configuration
//! - Various store and SQL store configurations
//! - Full core startup and light core shutdown

use super::Bootstrap;
use crate::error::Error;

use proven_applications::{ApplicationManagement, ApplicationManager};

use proven_core::BootstrapUpgrade;
use proven_governance::Governance;
use proven_identity::IdentityManager;
use proven_locks_memory::MemoryLockManager;
use proven_messaging::stream::Stream;
use proven_messaging_memory::client::MemoryClientOptions;
use proven_messaging_memory::consumer::MemoryConsumerOptions;
use proven_messaging_memory::service::MemoryServiceOptions;
use proven_messaging_memory::stream::{
    MemoryStream, MemoryStream2, MemoryStream3, MemoryStreamOptions,
};
use proven_passkeys::{PasskeyManagement, PasskeyManager, PasskeyManagerOptions};
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::{
    RpcEndpoints, RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions,
};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_streamed::{StreamedSqlStore2, StreamedSqlStore3};
use proven_store_fs::{FsStore, FsStore2, FsStore3};
use proven_store_memory::{MemoryStore, MemoryStore1, MemoryStore2, MemoryStore3};
use tracing::info;

static GATEWAY_URL: &str = "http://127.0.0.1:8081";

#[allow(clippy::too_many_lines)]
pub async fn execute<G: Governance>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    let lock_manager = MemoryLockManager::default();

    let identity_manager = IdentityManager::new(
        // Command stream for processing identity commands
        MemoryStream::new("IDENTITY_COMMANDS", MemoryStreamOptions),
        // Event stream for publishing identity events
        MemoryStream::new("IDENTITY_EVENTS", MemoryStreamOptions),
        // Service options for the command processing service
        MemoryServiceOptions,
        // Client options for the command client
        MemoryClientOptions,
        // Consumer options for the event consumer
        MemoryConsumerOptions,
        // Lock manager for distributed leadership
        lock_manager.clone(),
    )
    .await?;

    let passkey_manager = PasskeyManager::new(PasskeyManagerOptions {
        passkeys_store: MemoryStore::new(),
    });

    let sessions_manager = SessionManager::new(SessionManagerOptions {
        attestor: bootstrap.attestor.clone(),
        sessions_store: MemoryStore1::new(),
    });

    let application_manager = ApplicationManager::new(
        // Command stream for processing application commands
        MemoryStream::new("APPLICATION_COMMANDS", MemoryStreamOptions),
        // Event stream for publishing application events
        MemoryStream::new("APPLICATION_EVENTS", MemoryStreamOptions),
        // Service options for the command processing service
        MemoryServiceOptions,
        // Client options for the command client
        MemoryClientOptions,
        // Consumer options for the event consumer
        MemoryConsumerOptions,
        // Lock manager for distributed leadership
        lock_manager,
    )
    .await?;

    let applications = application_manager.list_all_applications().await.unwrap();
    info!("current application count: {}", applications.len());

    let application_store = MemoryStore2::new();

    let application_sql_store = StreamedSqlStore2::new(
        MemoryStream2::new("APPLICATION_SQL", MemoryStreamOptions),
        MemoryServiceOptions,
        MemoryClientOptions,
        FsStore2::new("/tmp/proven/application_snapshots"),
    );

    let personal_store = MemoryStore3::new();

    let personal_sql_store = StreamedSqlStore3::new(
        MemoryStream3::new("PERSONAL_SQL", MemoryStreamOptions),
        MemoryServiceOptions,
        MemoryClientOptions,
        FsStore3::new("/tmp/proven/personal_snapshots"),
    );

    let nft_store = MemoryStore3::new();

    let nft_sql_store = StreamedSqlStore3::new(
        MemoryStream3::new("NFT_SQL", MemoryStreamOptions),
        MemoryServiceOptions,
        MemoryClientOptions,
        FsStore3::new("/tmp/proven/nft_snapshots"),
    );

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

    let core = bootstrap.bootstrapping_core.take().unwrap();

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
