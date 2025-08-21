//! Integration tests with a real engine instance

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_engine::{EngineBuilder, EngineConfig};
use proven_network::{NetworkManager, connection_pool::ConnectionPoolConfig};
use proven_storage::manager::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_store::Store;
use proven_store_engine::EngineStore;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::{MemoryOptions, MemoryTransport};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_store_with_engine() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_store_engine=debug,proven_engine=info")
        .with_test_writer()
        .try_init();

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

    // Create network manager with required parameters
    let signing_key = SigningKey::from_bytes(&[1u8; 32]);
    let pool_config = ConnectionPoolConfig::default();
    let attestor = Arc::new(MockAttestor::new());
    let governance = topology_arc.clone();

    let network_manager = Arc::new(NetworkManager::new(
        node_id,
        transport,
        topology_manager.clone(),
        signing_key,
        pool_config,
        governance,
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
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check engine health
    let health = engine.health().await.expect("Failed to get health");
    println!("Engine health: {health:?}");

    // Create store using the engine client
    let client = Arc::new(engine.client());
    let store: EngineStore<bytes::Bytes, std::convert::Infallible, std::convert::Infallible> =
        EngineStore::new(client);

    // Test put operation
    let key = "test_key";
    let value = bytes::Bytes::from("test_value");

    store
        .put(key, value.clone())
        .await
        .expect("Put should succeed");
    println!("Successfully put value to store");

    // Give time for the consumer to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test get operation
    let retrieved_value = store.get(key).await.expect("Get should succeed");
    assert_eq!(retrieved_value, Some(value.clone()), "Value should match");
    println!("Successfully retrieved value from store");

    // Test keys operation
    let keys = store.keys().await.expect("Keys should succeed");
    assert!(keys.contains(&key.to_string()), "Key should be in list");
    println!("Retrieved keys: {keys:?}");

    // Test delete operation
    store.delete(key).await.expect("Delete should succeed");
    println!("Successfully deleted key");

    // Give time for the consumer to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify key is deleted
    let deleted_value = store.get(key).await.expect("Get should succeed");
    assert_eq!(deleted_value, None, "Value should be deleted");

    // Clean up
    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_multiple_keys() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_store_engine=debug,proven_engine=info")
        .with_test_writer()
        .try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(2);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(node_id),
    }));

    // Create mock topology
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

    // Create network manager
    let signing_key = SigningKey::from_bytes(&[2u8; 32]);
    let pool_config = ConnectionPoolConfig::default();
    let attestor = Arc::new(MockAttestor::new());
    let governance = topology_arc.clone();

    let network_manager = Arc::new(NetworkManager::new(
        node_id,
        transport,
        topology_manager.clone(),
        signing_key,
        pool_config,
        governance,
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

    // Wait for initialization
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create store
    let client = Arc::new(engine.client());
    let store: EngineStore<bytes::Bytes, std::convert::Infallible, std::convert::Infallible> =
        EngineStore::new(client);

    // Put multiple keys
    let test_data = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
        ("key4", "value4"),
        ("key5", "value5"),
    ];

    for (key, value) in &test_data {
        store
            .put(*key, bytes::Bytes::from(*value))
            .await
            .expect("Put should succeed");
    }

    // Give time for the consumer to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all keys exist
    let keys = store.keys().await.expect("Keys should succeed");
    assert_eq!(keys.len(), test_data.len(), "Should have all keys");

    for (key, _) in &test_data {
        assert!(keys.contains(&key.to_string()), "Key {key} should exist");
    }

    // Test keys with prefix
    let prefix_keys = store
        .keys_with_prefix("key")
        .await
        .expect("Keys with prefix should succeed");
    assert_eq!(prefix_keys.len(), 5, "All keys start with 'key'");

    // Clean up
    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_concurrent_access() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_store_engine=debug,proven_engine=info")
        .with_test_writer()
        .try_init();

    // Setup engine
    let node_id = NodeId::from_seed(3);
    let transport = Arc::new(MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(node_id),
    }));

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

    let signing_key = SigningKey::from_bytes(&[3u8; 32]);
    let pool_config = ConnectionPoolConfig::default();
    let attestor = Arc::new(MockAttestor::new());
    let governance = topology_arc.clone();

    let network_manager = Arc::new(NetworkManager::new(
        node_id,
        transport,
        topology_manager.clone(),
        signing_key,
        pool_config,
        governance,
        attestor,
    ));

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
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create store
    let client = Arc::new(engine.client());
    let store: EngineStore<bytes::Bytes, std::convert::Infallible, std::convert::Infallible> =
        EngineStore::new(client);

    // Test concurrent writes
    let mut handles = vec![];
    for i in 0..10 {
        let store_clone = store.clone();
        let handle = tokio::spawn(async move {
            let key = format!("concurrent_key_{i}");
            let value = bytes::Bytes::from(format!("concurrent_value_{i}"));
            store_clone
                .put(key, value)
                .await
                .expect("Concurrent put should succeed");
        });
        handles.push(handle);
    }

    // Wait for all writes to complete
    for handle in handles {
        handle.await.expect("Task should complete");
    }

    // Give time for the consumer to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all keys exist
    let keys = store.keys().await.expect("Keys should succeed");
    assert_eq!(keys.len(), 10, "Should have all concurrent keys");

    // Clean up
    engine.stop().await.expect("Failed to stop engine");
}
