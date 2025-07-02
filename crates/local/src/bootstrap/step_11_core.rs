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

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::any;
use http::StatusCode;
use openraft::Config as RaftConfig;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_bootable::Bootable;
use proven_consensus::Consensus;
use proven_core::{Core, CoreOptions};
use proven_governance::Governance;
use proven_http_insecure::InsecureHttpServer;
use proven_identity::IdentityManager;
use proven_locks_nats::{NatsLockManager, NatsLockManagerConfig};
use proven_messaging::stream::Stream;
use proven_messaging_consensus::ConsensusConfig;
use proven_messaging_consensus::consumer::ConsensusConsumerOptions;
use proven_messaging_consensus::stream::{ConsensusStream, ConsensusStreamOptions};
use proven_messaging_nats::client::NatsClientOptions;
use proven_messaging_nats::consumer::NatsConsumerOptions;
use proven_messaging_nats::service::NatsServiceOptions;
use proven_messaging_nats::stream::{NatsStream, NatsStream2, NatsStream3, NatsStreamOptions};
use proven_passkeys::{PasskeyManagement, PasskeyManager, PasskeyManagerOptions};
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::{
    RpcEndpoints, RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions,
};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_streamed::{StreamedSqlStore2, StreamedSqlStore3};
use proven_store_fs::{FsStore, FsStore2, FsStore3};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3, NatsStoreOptions};
use tower_http::cors::CorsLayer;
use tracing::info;
use uuid::Uuid;

static GATEWAY_URL: &str = "http://127.0.0.1:8081";
const PORT_RELEASE_MAX_RETRIES: u32 = 20;
const PORT_RELEASE_RETRY_DELAY: Duration = Duration::from_secs(1);

#[allow(clippy::too_many_lines)]
pub async fn execute<G: Governance>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    let nats_client = bootstrap.nats_client.as_ref().unwrap_or_else(|| {
        panic!("nats client not fetched before core");
    });

    let network = bootstrap.network.as_ref().unwrap_or_else(|| {
        panic!("network not set before core");
    });

    let light_core = bootstrap.light_core.as_ref().unwrap_or_else(|| {
        panic!("light core not fetched before core");
    });

    let lock_manager = NatsLockManager::new(NatsLockManagerConfig {
        bucket: "GLOBAL_LOCKS".to_string(),
        client: nats_client.clone(),
        local_identifier: format!("node-{}", Uuid::new_v4()),
        num_replicas: bootstrap.num_replicas,
        persist: true,
        ttl: Duration::from_secs(30),
        operation_timeout: None,
        max_retries: None,
        retry_base_delay: None,
        retry_max_delay: None,
    });

    let consensus = Consensus::new(
        bootstrap.config.port.to_string(), // TODO: replace with a real id
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, bootstrap.config.p2p_port)),
        Arc::new(bootstrap.config.governance.clone()),
        Arc::new(bootstrap.attestor.clone()),
        bootstrap.config.node_key.clone(),
        ConsensusConfig {
            consensus_timeout: Duration::from_secs(30),
            require_all_nodes: false,
            raft_config: Arc::new(RaftConfig {
                heartbeat_interval: 500,    // 500ms
                election_timeout_min: 1500, // 1.5s
                election_timeout_max: 3000, // 3s
                ..RaftConfig::default()
            }),
            storage_dir: None, // Default to None, will use temporary directory
        },
    )
    .await
    .unwrap();

    consensus.start().await.unwrap();

    let identity_manager = IdentityManager::new(
        // Command stream for processing identity commands
        NatsStream::new(
            "IDENTITY_COMMANDS",
            NatsStreamOptions {
                client: nats_client.clone(),
                num_replicas: bootstrap.num_replicas,
            },
        ),
        // Event stream for publishing identity events
        ConsensusStream::new(
            "IDENTITY_EVENTS",
            ConsensusStreamOptions {
                consensus: Arc::new(consensus),
                stream_config: None,
            },
        ),
        // Service options for the command processing service
        NatsServiceOptions {
            client: nats_client.clone(),
            durable_name: None,
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        // Client options for the command client
        NatsClientOptions {
            client: nats_client.clone(),
        },
        // Consumer options for the event consumer
        ConsensusConsumerOptions {
            start_sequence: None,
        },
        // Lock manager for distributed leadership
        lock_manager.clone(),
    )
    .await?;

    let passkey_manager = PasskeyManager::new(PasskeyManagerOptions {
        passkeys_store: NatsStore::new(NatsStoreOptions {
            bucket: "passkeys".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            num_replicas: bootstrap.num_replicas,
            persist: true,
        }),
    });

    let sessions_manager = SessionManager::new(SessionManagerOptions {
        attestor: bootstrap.attestor.clone(),
        sessions_store: NatsStore1::new(NatsStoreOptions {
            bucket: "sessions".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            num_replicas: bootstrap.num_replicas,
            persist: true,
        }),
    });

    let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, bootstrap.config.port));
    let http_server = InsecureHttpServer::new(
        http_sock_addr,
        Router::new()
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive()),
    );

    let application_manager = ApplicationManager::new(
        // Command stream for processing application commands
        NatsStream::new(
            "APPLICATION_COMMANDS",
            NatsStreamOptions {
                client: nats_client.clone(),
                num_replicas: bootstrap.num_replicas,
            },
        ),
        // Event stream for publishing application events
        NatsStream::new(
            "APPLICATION_EVENTS",
            NatsStreamOptions {
                client: nats_client.clone(),
                num_replicas: bootstrap.num_replicas,
            },
        ),
        // Service options for the command processing service
        NatsServiceOptions {
            client: nats_client.clone(),
            durable_name: None,
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        // Client options for the command client
        NatsClientOptions {
            client: nats_client.clone(),
        },
        // Consumer options for the event consumer
        NatsConsumerOptions {
            client: nats_client.clone(),
            durable_name: None,
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        // Lock manager for distributed leadership
        lock_manager,
    )
    .await?;

    let applications = application_manager.list_all_applications().await.unwrap();
    info!("current application count: {}", applications.len());

    let application_store = NatsStore2::new(NatsStoreOptions {
        bucket: "APPLICATION_KV".to_string(),
        client: nats_client.clone(),
        max_age: Duration::ZERO,
        num_replicas: bootstrap.num_replicas,
        persist: true,
    });

    let application_sql_store = StreamedSqlStore2::new(
        NatsStream2::new(
            "APPLICATION_SQL",
            NatsStreamOptions {
                client: nats_client.clone(),
                num_replicas: bootstrap.num_replicas,
            },
        ),
        NatsServiceOptions {
            client: nats_client.clone(),
            durable_name: None,
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        NatsClientOptions {
            client: nats_client.clone(),
        },
        FsStore2::new("/tmp/proven/application_snapshots"),
    );

    let personal_store = NatsStore3::new(NatsStoreOptions {
        bucket: "PERSONAL_KV".to_string(),
        client: nats_client.clone(),
        max_age: Duration::ZERO,
        num_replicas: bootstrap.num_replicas,
        persist: true,
    });

    let personal_sql_store = StreamedSqlStore3::new(
        NatsStream3::new(
            "PERSONAL_SQL",
            NatsStreamOptions {
                client: nats_client.clone(),
                num_replicas: bootstrap.num_replicas,
            },
        ),
        NatsServiceOptions {
            client: nats_client.clone(),
            durable_name: None,
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        NatsClientOptions {
            client: nats_client.clone(),
        },
        FsStore3::new("/tmp/proven/personal_snapshots"),
    );

    let nft_store = NatsStore3::new(NatsStoreOptions {
        bucket: "NFT_KV".to_string(),
        client: nats_client.clone(),
        max_age: Duration::ZERO,
        num_replicas: bootstrap.num_replicas,
        persist: true,
    });

    let nft_sql_store = StreamedSqlStore3::new(
        NatsStream3::new(
            "NFT_SQL",
            NatsStreamOptions {
                client: nats_client.clone(),
                num_replicas: bootstrap.num_replicas,
            },
        ),
        NatsServiceOptions {
            client: nats_client.clone(),
            durable_name: None,
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        NatsClientOptions {
            client: nats_client.clone(),
        },
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

    let core = Core::new(CoreOptions {
        application_manager,
        attestor: bootstrap.attestor.clone(),
        http_server,
        identity_manager,
        network: network.clone(),
        passkey_manager,
        runtime_pool_manager,
        sessions_manager,
    });

    // Shutdown the light core and free the port before starting the full core
    let _ = light_core.shutdown().await;
    bootstrap.light_core = None;

    // Wait for the port to be fully released
    let port = bootstrap.config.port;
    let mut retry_count = 0;

    loop {
        // Test if we can bind to the port
        if let Ok(listener) = tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
            // Port is available, close the test listener
            drop(listener);
            break;
        }
        retry_count += 1;
        if retry_count >= PORT_RELEASE_MAX_RETRIES {
            return Err(Error::Io(format!(
                "Port {port} still in use after {PORT_RELEASE_MAX_RETRIES} attempts"
            )));
        }
        tokio::time::sleep(PORT_RELEASE_RETRY_DELAY).await;
    }

    core.start().await.map_err(Error::Bootable)?;

    // Add core to bootables collection
    bootstrap.add_bootable(Box::new(core));

    info!("core started");

    Ok(())
}
