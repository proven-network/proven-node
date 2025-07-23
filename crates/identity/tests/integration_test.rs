//! Integration test for identity-engine using engine directly
//!
//! This test verifies that the identity-engine crate works correctly
//! with a simple single-node engine setup using in-memory transport and storage.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proven_engine::{EngineBuilder, EngineConfig};
use proven_identity::{IdentityManagement, IdentityManager, IdentityManagerConfig};
use proven_network::NetworkManager;
use proven_storage::manager::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::MemoryTransport;

#[tokio::test]
async fn test_basic_identity_operations() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(1);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(node_id.clone()));
    transport
        .register(&format!("memory://{node_id}"))
        .await
        .expect("Failed to register transport");

    // Create mock topology with this node registered
    let node = TopologyNode::new(
        "test-az".to_string(),
        format!("memory://{node_id}"),
        node_id.clone(),
        "test-region".to_string(),
        HashSet::new(),
    );

    let topology =
        MockTopologyAdaptor::new(vec![], vec![], "https://auth.test.com".to_string(), vec![]);
    let _ = topology.add_node(node);

    let topology_manager = Arc::new(TopologyManager::new(Arc::new(topology), node_id.clone()));

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
        transport,
        topology_manager.clone(),
    ));

    // Create storage
    let storage_manager = Arc::new(StorageManager::new(MemoryStorage::new()));

    // Configure engine for testing
    let mut config = EngineConfig::default();
    config.consensus.global.election_timeout_min = Duration::from_millis(50);
    config.consensus.global.election_timeout_max = Duration::from_millis(100);
    config.consensus.global.heartbeat_interval = Duration::from_millis(20);

    // Build engine
    let mut engine = EngineBuilder::new(node_id.clone())
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

    // Create identity manager
    let config = IdentityManagerConfig {
        stream_prefix: "test-identity".to_string(),
        leadership_lease_duration: Duration::from_secs(10),
        leadership_renewal_interval: Duration::from_secs(3),
        command_timeout: Duration::from_secs(60), // Longer timeout for tests
    };

    let identity_manager = IdentityManager::new(client.clone(), config)
        .await
        .expect("Failed to create identity manager");

    // Wait for the manager to initialize (leadership, consumers, etc.)
    println!("Waiting for identity manager initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Test 1: Create identity with PRF public key
    let prf_key1 = Bytes::from(vec![1u8; 32]);
    let identity1 = identity_manager
        .get_or_create_identity_by_prf_public_key(&prf_key1)
        .await
        .expect("Failed to create identity");

    println!("Created identity: {identity1:?}");

    // Give time for event processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test 2: Verify identity exists
    let exists = identity_manager
        .identity_exists(&identity1.id)
        .await
        .expect("Failed to check identity existence");
    assert!(exists);

    // Test 3: Get identity by ID
    let retrieved_identity = identity_manager
        .get_identity(&identity1.id)
        .await
        .expect("Failed to get identity")
        .expect("Identity not found");
    assert_eq!(retrieved_identity.id, identity1.id);

    // Test 4: Get same identity by PRF key (should not create new)
    let identity1_again = identity_manager
        .get_or_create_identity_by_prf_public_key(&prf_key1)
        .await
        .expect("Failed to get identity by PRF key");
    assert_eq!(identity1_again.id, identity1.id);

    // Test 5: Create another identity with different PRF key
    let prf_key2 = Bytes::from(vec![2u8; 32]);
    let identity2 = identity_manager
        .get_or_create_identity_by_prf_public_key(&prf_key2)
        .await
        .expect("Failed to create second identity");
    assert_ne!(identity2.id, identity1.id);

    // Test 6: List all identities
    let all_identities = identity_manager
        .list_identities()
        .await
        .expect("Failed to list identities");
    assert_eq!(all_identities.len(), 2);
    assert!(all_identities.iter().any(|i| i.id == identity1.id));
    assert!(all_identities.iter().any(|i| i.id == identity2.id));

    // Test 7: Verify view counts
    assert_eq!(identity_manager.view().identity_count().await, 2);
    assert_eq!(identity_manager.view().prf_public_key_count().await, 2);

    // Shutdown
    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_concurrent_identity_creation() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(2);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(node_id.clone()));
    transport
        .register(&format!("memory://{node_id}"))
        .await
        .expect("Failed to register transport");

    // Create mock topology with this node registered
    let node = TopologyNode::new(
        "test-az".to_string(),
        format!("memory://{node_id}"),
        node_id.clone(),
        "test-region".to_string(),
        HashSet::new(),
    );

    let topology =
        MockTopologyAdaptor::new(vec![], vec![], "https://auth.test.com".to_string(), vec![]);
    let _ = topology.add_node(node);

    let topology_manager = Arc::new(TopologyManager::new(Arc::new(topology), node_id.clone()));

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
        transport,
        topology_manager.clone(),
    ));

    // Create storage and engine
    let storage_manager = Arc::new(StorageManager::new(MemoryStorage::new()));
    let config = EngineConfig::default();

    let mut engine = EngineBuilder::new(node_id.clone())
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

    // Create identity manager
    let identity_manager = Arc::new(
        IdentityManager::new(client.clone(), IdentityManagerConfig::default())
            .await
            .expect("Failed to create identity manager"),
    );

    println!("Waiting for identity manager initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Test concurrent identity creation with different PRF keys
    let mut handles = vec![];

    for i in 0..5 {
        let manager = identity_manager.clone();
        let handle = tokio::spawn(async move {
            println!("Creating identity {i}");
            let prf_key = Bytes::from(vec![i as u8 + 10; 32]);
            manager
                .get_or_create_identity_by_prf_public_key(&prf_key)
                .await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // All should succeed
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(Ok(identity)) => println!("Identity {i} created: {:?}", identity.id),
            Ok(Err(e)) => panic!("Failed to create identity {i}: {e:?}"),
            Err(e) => panic!("Task {i} panicked: {e:?}"),
        }
    }

    // Give time for event processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify all identities were created
    let all_identities = identity_manager
        .list_identities()
        .await
        .expect("Failed to list identities");
    assert_eq!(all_identities.len(), 5);
    assert_eq!(identity_manager.view().prf_public_key_count().await, 5);

    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_duplicate_prf_key_handling() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(3);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(node_id.clone()));
    transport
        .register(&format!("memory://{node_id}"))
        .await
        .expect("Failed to register transport");

    // Create mock topology with this node registered
    let node = TopologyNode::new(
        "test-az".to_string(),
        format!("memory://{node_id}"),
        node_id.clone(),
        "test-region".to_string(),
        HashSet::new(),
    );

    let topology =
        MockTopologyAdaptor::new(vec![], vec![], "https://auth.test.com".to_string(), vec![]);
    let _ = topology.add_node(node);

    let topology_manager = Arc::new(TopologyManager::new(Arc::new(topology), node_id.clone()));

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
        transport,
        topology_manager.clone(),
    ));

    // Create storage and engine
    let storage_manager = Arc::new(StorageManager::new(MemoryStorage::new()));
    let config = EngineConfig::default();

    let mut engine = EngineBuilder::new(node_id.clone())
        .with_config(config)
        .with_network(network_manager)
        .with_topology(topology_manager)
        .with_storage(storage_manager)
        .build()
        .await
        .expect("Failed to build engine");

    engine.start().await.expect("Failed to start engine");

    // Wait for default group creation
    println!("Waiting for engine initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = Arc::new(engine.client());

    // Create identity manager
    let identity_manager = IdentityManager::new(client.clone(), IdentityManagerConfig::default())
        .await
        .expect("Failed to create identity manager");

    println!("Waiting for identity manager initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Create identity with PRF key
    let prf_key = Bytes::from(vec![100u8; 32]);
    let identity1 = identity_manager
        .get_or_create_identity_by_prf_public_key(&prf_key)
        .await
        .expect("Failed to create identity");

    // Give time for event processing
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Try to create concurrent requests with same PRF key
    let manager1 = identity_manager.clone();
    let manager2 = identity_manager.clone();
    let prf_key1 = prf_key.clone();
    let prf_key2 = prf_key.clone();

    let handle1 = tokio::spawn(async move {
        manager1
            .get_or_create_identity_by_prf_public_key(&prf_key1)
            .await
    });

    let handle2 = tokio::spawn(async move {
        manager2
            .get_or_create_identity_by_prf_public_key(&prf_key2)
            .await
    });

    let result1 = handle1
        .await
        .expect("Task 1 panicked")
        .expect("Request 1 failed");
    let result2 = handle2
        .await
        .expect("Task 2 panicked")
        .expect("Request 2 failed");

    // Both should return the same identity
    assert_eq!(result1.id, identity1.id);
    assert_eq!(result2.id, identity1.id);

    // Verify still only one identity exists
    assert_eq!(identity_manager.view().identity_count().await, 1);
    assert_eq!(identity_manager.view().prf_public_key_count().await, 1);

    engine.stop().await.expect("Failed to stop engine");
}
