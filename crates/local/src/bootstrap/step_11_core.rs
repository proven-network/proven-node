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
use crate::error::{Error, Result};

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::Router;
use axum::routing::any;
use http::StatusCode;
use proven_applications::{ApplicationManagement, ApplicationManager, CreateApplicationOptions};
use proven_bootable::Bootable;
use proven_core::{Core, CoreOptions};
use proven_http_insecure::InsecureHttpServer;
use proven_identity::IdentityManager;
use proven_locks_nats::{NatsLockManager, NatsLockManagerConfig};
use proven_messaging::stream::Stream;
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

#[allow(clippy::too_many_lines)]
pub async fn execute(bootstrap: &mut Bootstrap) -> Result<()> {
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
    });

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
        NatsStream::new(
            "IDENTITY_EVENTS",
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
            durable_name: Some("IDENTITY_VIEW_CONSUMER".to_string()),
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        // Lock manager for distributed leadership
        lock_manager.clone(),
    );

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

    let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, bootstrap.args.port));
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
            durable_name: Some("APPLICATION_VIEW_CONSUMER".to_string()),
            jetstream_context: async_nats::jetstream::new(nats_client.clone()),
        },
        // Lock manager for distributed leadership
        lock_manager,
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let applications = application_manager.list_all_applications().await.unwrap();
    println!("applications: {applications:?}");
    // Create a test application if there are no applications
    if applications.is_empty() {
        let application = application_manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: Uuid::new_v4(),
            })
            .await;
        println!("created test application: {application:?}");
    }

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

    core.start().await.map_err(Error::Bootable)?;

    // Add core to bootables collection
    bootstrap.add_bootable(Box::new(core));

    info!("core started");

    Ok(())
}
