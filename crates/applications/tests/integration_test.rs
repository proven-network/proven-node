//! Integration test for applications-engine using engine directly
//!
//! This test verifies that the applications-engine crate works correctly
//! with a simple single-node engine setup using in-memory transport and storage.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use proven_applications::{
    ApplicationManagement, ApplicationManager, ApplicationManagerConfig, CreateApplicationOptions,
};
use proven_attestation_mock::MockAttestor;
use proven_engine::{EngineBuilder, EngineConfig};
use proven_network::NetworkManager;
use proven_network::connection_pool::ConnectionPoolConfig;
use proven_storage::manager::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::{MemoryOptions, MemoryTransport};
use uuid::Uuid;

#[tokio::test]
async fn test_basic_application_operations() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(1);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(node_id),
    }));

    // Create mock topology with this node registered
    let node = TopologyNode::new(
        "test-az".to_string(),
        format!("memory://{node_id}"),
        node_id,
        "test-region".to_string(),
        HashSet::new(),
    );

    let topology =
        MockTopologyAdaptor::new(vec![], vec![], "https://auth.test.com".to_string(), vec![]);
    let _ = topology.add_node(node);

    let topology_arc = Arc::new(topology);
    let topology_manager = Arc::new(TopologyManager::new(topology_arc.clone(), node_id));

    // Create signing key for network manager
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);

    // Create mock attestor
    let attestor = Arc::new(MockAttestor::new());

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id,
        transport,
        topology_manager.clone(),
        signing_key,
        ConnectionPoolConfig::default(),
        topology_arc.clone(),
        attestor,
    ));

    // Create storage
    let storage_manager = Arc::new(StorageManager::new(MemoryStorage::new()));

    // Configure engine for testing
    let mut config = EngineConfig::default();
    config.consensus.global.election_timeout_min = Duration::from_millis(50);
    config.consensus.global.election_timeout_max = Duration::from_millis(100);
    config.consensus.global.heartbeat_interval = Duration::from_millis(20);

    // Build engine
    let mut engine = EngineBuilder::new(node_id)
        .with_config(config)
        .with_network(network_manager)
        .with_topology(topology_manager)
        .with_storage(storage_manager)
        .build()
        .await
        .expect("Failed to build engine");

    // Start the engine
    engine.start().await.expect("Failed to start engine");

    // Give engine time to initialize and create default group
    println!("Waiting for engine initialization and default group creation...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check engine health
    let health = engine.health().await.expect("Failed to get health");
    println!("Engine health: {health:?}");

    // Get the client
    let client = Arc::new(engine.client());

    // Create application manager
    let config = ApplicationManagerConfig {
        stream_prefix: "test-apps".to_string(),
        leadership_lease_duration: Duration::from_secs(10),
        leadership_renewal_interval: Duration::from_secs(3),
        command_timeout: Duration::from_secs(60), // Longer timeout for tests
    };

    let app_manager = ApplicationManager::new(client.clone(), config)
        .await
        .expect("Failed to create application manager");

    // Wait for the manager to initialize (leadership, consumers, etc.)
    println!("Waiting for application manager initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Test 1: Create an application
    let owner_id = Uuid::new_v4();
    let app = app_manager
        .create_application(&CreateApplicationOptions {
            owner_identity_id: owner_id,
        })
        .await
        .expect("Failed to create application");

    println!("Created application: {app:?}");
    assert_eq!(app.owner_id, owner_id);

    // Give time for application creation to process
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test 2: Verify application exists
    let exists = app_manager
        .application_exists(&app.id)
        .await
        .expect("Failed to check application existence");
    assert!(exists);

    // Test 3: Get application by ID
    let retrieved_app = app_manager
        .get_application(&app.id)
        .await
        .expect("Failed to get application")
        .expect("Application not found");
    assert_eq!(retrieved_app.id, app.id);
    assert_eq!(retrieved_app.owner_id, owner_id);

    // Test 4: List applications by owner
    let apps_by_owner = app_manager
        .list_applications_by_owner(&owner_id)
        .await
        .expect("Failed to list applications by owner");
    assert_eq!(apps_by_owner.len(), 1);
    assert_eq!(apps_by_owner[0].id, app.id);

    // Test 5: Add allowed origin
    let origin =
        proven_util::Origin::from_str("https://example.com").expect("Failed to create origin");
    app_manager
        .add_allowed_origin(&app.id, &origin)
        .await
        .expect("Failed to add allowed origin");

    // Verify origin was added
    let updated_app = app_manager
        .get_application(&app.id)
        .await
        .expect("Failed to get application")
        .expect("Application not found");
    assert_eq!(updated_app.allowed_origins.len(), 1);
    assert_eq!(updated_app.allowed_origins[0], origin);

    // Test 6: Link HTTP domain
    let domain = proven_util::Domain::from_str("example.com").expect("Failed to create domain");
    app_manager
        .link_http_domain(&app.id, &domain)
        .await
        .expect("Failed to link HTTP domain");

    // Verify domain was linked
    let updated_app = app_manager
        .get_application(&app.id)
        .await
        .expect("Failed to get application")
        .expect("Application not found");
    assert_eq!(updated_app.linked_http_domains.len(), 1);
    assert_eq!(updated_app.linked_http_domains[0], domain);

    // Test 7: Transfer ownership
    let new_owner_id = Uuid::new_v4();
    app_manager
        .transfer_ownership(&app.id, &new_owner_id)
        .await
        .expect("Failed to transfer ownership");

    // Verify ownership was transferred
    let updated_app = app_manager
        .get_application(&app.id)
        .await
        .expect("Failed to get application")
        .expect("Application not found");
    assert_eq!(updated_app.owner_id, new_owner_id);

    // Test 8: Archive application
    app_manager
        .archive_application(&app.id)
        .await
        .expect("Failed to archive application");

    // Verify application no longer exists
    let exists = app_manager
        .application_exists(&app.id)
        .await
        .expect("Failed to check application existence");
    assert!(!exists);

    // Shutdown
    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_concurrent_operations() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(2);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(node_id),
    }));

    // Create mock topology with this node registered
    let node = TopologyNode::new(
        "test-az".to_string(),
        format!("memory://{node_id}"),
        node_id,
        "test-region".to_string(),
        HashSet::new(),
    );

    let topology =
        MockTopologyAdaptor::new(vec![], vec![], "https://auth.test.com".to_string(), vec![]);
    let _ = topology.add_node(node);

    let topology_arc = Arc::new(topology);
    let topology_manager = Arc::new(TopologyManager::new(topology_arc.clone(), node_id));

    // Create signing key for network manager
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);

    // Create mock attestor
    let attestor = Arc::new(MockAttestor::new());

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id,
        transport,
        topology_manager.clone(),
        signing_key,
        ConnectionPoolConfig::default(),
        topology_arc.clone(),
        attestor,
    ));

    // Create storage and engine
    let storage_manager = Arc::new(StorageManager::new(MemoryStorage::new()));
    let config = EngineConfig::default();

    let mut engine = EngineBuilder::new(node_id)
        .with_config(config)
        .with_network(network_manager)
        .with_topology(topology_manager)
        .with_storage(storage_manager)
        .build()
        .await
        .expect("Failed to build engine");

    engine.start().await.expect("Failed to start engine");

    // Wait for default group creation
    println!("Waiting for engine initialization and default group creation...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = Arc::new(engine.client());

    // Create application manager
    let app_manager = Arc::new(
        ApplicationManager::new(client.clone(), ApplicationManagerConfig::default())
            .await
            .expect("Failed to create application manager"),
    );

    println!("Waiting for application manager initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Test concurrent application creation
    let owner_id = Uuid::new_v4();
    let mut handles = vec![];

    for i in 0..5 {
        let manager = app_manager.clone();
        let handle = tokio::spawn(async move {
            println!("Creating application {i}");
            manager
                .create_application(&CreateApplicationOptions {
                    owner_identity_id: owner_id,
                })
                .await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // All should succeed
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(Ok(app)) => println!("Application {} created: {:?}", i, app.id),
            Ok(Err(e)) => panic!("Failed to create application {i}: {e:?}"),
            Err(e) => panic!("Task {i} panicked: {e:?}"),
        }
    }

    // Verify all applications were created
    let apps = app_manager
        .list_applications_by_owner(&owner_id)
        .await
        .expect("Failed to list applications");
    assert_eq!(apps.len(), 5);

    engine.stop().await.expect("Failed to stop engine");
}
