//! Integration test for sql-engine using engine directly
//!
//! This test verifies that the sql-engine crate works correctly
//! with a simple single-node engine setup using in-memory transport and storage.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use proven_attestation_mock::MockAttestor;
use proven_engine::{EngineBuilder, EngineConfig};
use proven_network::{NetworkManager, connection_pool::ConnectionPoolConfig};
use proven_sql::{SqlConnection, SqlParam, SqlStore, SqlStore1, SqlTransaction};
use proven_sql_engine::{SqlEngineStore, SqlEngineStore1};
use proven_storage::manager::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::{MemoryOptions, MemoryTransport};

#[tokio::test]
async fn test_basic_sql_operations() {
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

    // Create SQL store with test database
    let store = SqlEngineStore::new(client.clone(), "test_basic")
        .with_lease_duration(Duration::from_secs(10))
        .with_renewal_interval(Duration::from_secs(3));

    // Wait for SQL service to start and become leader
    println!("Waiting for SQL service initialization...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Connect with migrations
    let connection = store
        .connect(vec![
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, FOREIGN KEY(user_id) REFERENCES users(id))",
        ])
        .await
        .expect("Failed to connect to SQL store");

    // Test 1: Insert a user
    let affected = connection
        .execute(
            "INSERT INTO users (id, email) VALUES (?1, ?2)",
            vec![
                SqlParam::Integer(1),
                SqlParam::Text("alice@example.com".to_string()),
            ],
        )
        .await
        .expect("Failed to insert user");

    assert_eq!(affected, 1);
    println!("Inserted user successfully");

    // Test 2: Query the user
    let mut rows = connection
        .query(
            "SELECT id, email FROM users WHERE id = ?1",
            vec![SqlParam::Integer(1)],
        )
        .await
        .expect("Failed to query user");

    let row = rows.next().await.expect("Expected a row");
    assert_eq!(
        row,
        vec![
            SqlParam::IntegerWithName("id".to_string(), 1),
            SqlParam::TextWithName("email".to_string(), "alice@example.com".to_string()),
        ]
    );
    assert!(rows.next().await.is_none());
    println!("Queried user successfully");

    // Test 3: Insert multiple users with batch
    let batch_params = vec![
        vec![
            SqlParam::Integer(2),
            SqlParam::Text("bob@example.com".to_string()),
        ],
        vec![
            SqlParam::Integer(3),
            SqlParam::Text("charlie@example.com".to_string()),
        ],
        vec![
            SqlParam::Integer(4),
            SqlParam::Text("diana@example.com".to_string()),
        ],
    ];

    let batch_affected = connection
        .execute_batch(
            "INSERT INTO users (id, email) VALUES (?1, ?2)",
            batch_params,
        )
        .await
        .expect("Failed to batch insert users");

    assert_eq!(batch_affected, 3);
    println!("Batch inserted 3 users successfully");

    // Test 4: Insert posts for users
    connection
        .execute(
            "INSERT INTO posts (id, user_id, title) VALUES (?1, ?2, ?3)",
            vec![
                SqlParam::Integer(1),
                SqlParam::Integer(1),
                SqlParam::Text("Alice's First Post".to_string()),
            ],
        )
        .await
        .expect("Failed to insert post");

    // Test 5: Join query
    let mut join_rows = connection
        .query(
            "SELECT u.email, p.title FROM users u JOIN posts p ON u.id = p.user_id",
            vec![],
        )
        .await
        .expect("Failed to execute join query");

    let join_row = join_rows.next().await.expect("Expected a row from join");
    match (&join_row[0], &join_row[1]) {
        (SqlParam::TextWithName(col1, email), SqlParam::TextWithName(col2, title)) => {
            assert_eq!(col1, "email");
            assert_eq!(col2, "title");
            assert_eq!(email, "alice@example.com");
            assert_eq!(title, "Alice's First Post");
        }
        _ => panic!("Unexpected row format"),
    }
    println!("Join query executed successfully");

    // Test 6: Count query
    let mut count_rows = connection
        .query("SELECT COUNT(*) as count FROM users", vec![])
        .await
        .expect("Failed to count users");

    let count_row = count_rows.next().await.expect("Expected count row");
    match &count_row[0] {
        SqlParam::IntegerWithName(col, count) => {
            assert_eq!(col, "count");
            assert_eq!(*count, 4);
        }
        _ => panic!("Expected integer count"),
    }
    println!("Count query returned 4 users");

    // Test 7: Migration
    let migration_result = connection
        .migrate("CREATE TABLE IF NOT EXISTS comments (id INTEGER PRIMARY KEY, post_id INTEGER, content TEXT)")
        .await
        .expect("Failed to run migration");

    assert!(migration_result, "Migration should have run");
    println!("Migration executed successfully");

    // Run same migration again - should not execute
    let migration_result2 = connection
        .migrate("CREATE TABLE IF NOT EXISTS comments (id INTEGER PRIMARY KEY, post_id INTEGER, content TEXT)")
        .await
        .expect("Failed to run migration");

    assert!(!migration_result2, "Migration should not run twice");
    println!("Migration correctly skipped on second run");

    // Shutdown
    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_scoped_databases() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

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

    // Create signing key
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[2u8; 32]);

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

    // Wait for initialization
    println!("Waiting for engine initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = Arc::new(engine.client());

    // Create scoped SQL stores
    let store1 = SqlEngineStore1::new(client.clone(), "test");
    let app1_store = store1.scope("app1");
    let app2_store = store1.scope("app2");

    // Wait for services to start
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Connect to app1 database
    let app1_conn = app1_store
        .connect(vec![
            "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)",
        ])
        .await
        .expect("Failed to connect to app1 database");

    // Insert data into app1
    app1_conn
        .execute(
            "INSERT INTO config (key, value) VALUES (?1, ?2)",
            vec![
                SqlParam::Text("theme".to_string()),
                SqlParam::Text("dark".to_string()),
            ],
        )
        .await
        .expect("Failed to insert into app1");

    // Connect to app2 database
    let app2_conn = app2_store
        .connect(vec![
            "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)",
        ])
        .await
        .expect("Failed to connect to app2 database");

    // Query app2 - should be empty
    let mut app2_rows = app2_conn
        .query("SELECT * FROM config", vec![])
        .await
        .expect("Failed to query app2");

    assert!(app2_rows.next().await.is_none(), "App2 should have no data");
    println!("Scoped databases are properly isolated");

    // Insert different data into app2
    app2_conn
        .execute(
            "INSERT INTO config (key, value) VALUES (?1, ?2)",
            vec![
                SqlParam::Text("theme".to_string()),
                SqlParam::Text("light".to_string()),
            ],
        )
        .await
        .expect("Failed to insert into app2");

    // Verify app1 still has its own data
    let mut app1_rows = app1_conn
        .query(
            "SELECT value FROM config WHERE key = ?1",
            vec![SqlParam::Text("theme".to_string())],
        )
        .await
        .expect("Failed to query app1");

    let app1_row = app1_rows.next().await.expect("Expected row from app1");
    match &app1_row[0] {
        SqlParam::TextWithName(_, value) => assert_eq!(value, "dark"),
        _ => panic!("Unexpected value type"),
    }

    // Verify app2 has different data
    let mut app2_rows = app2_conn
        .query(
            "SELECT value FROM config WHERE key = ?1",
            vec![SqlParam::Text("theme".to_string())],
        )
        .await
        .expect("Failed to query app2");

    let app2_row = app2_rows.next().await.expect("Expected row from app2");
    match &app2_row[0] {
        SqlParam::TextWithName(_, value) => assert_eq!(value, "light"),
        _ => panic!("Unexpected value type"),
    }

    println!("Scoped databases maintain separate data successfully");

    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_error_handling() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create setup
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

    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[3u8; 32]);
    let attestor = Arc::new(MockAttestor::new());

    let network_manager = Arc::new(NetworkManager::new(
        node_id,
        transport,
        topology_manager.clone(),
        signing_key,
        ConnectionPoolConfig::default(),
        topology_arc.clone(),
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
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = Arc::new(engine.client());
    let store = SqlEngineStore::new(client.clone(), "test_errors");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let connection = store
        .connect::<String>(vec![])
        .await
        .expect("Failed to connect");

    // Test 1: Invalid SQL syntax
    let result = connection.execute("INVALID SQL STATEMENT", vec![]).await;
    assert!(result.is_err(), "Invalid SQL should fail");
    println!("Invalid SQL correctly rejected");

    // Test 2: Query non-existent table
    let result = connection
        .query("SELECT * FROM non_existent_table", vec![])
        .await;
    assert!(result.is_err(), "Query on non-existent table should fail");
    println!("Non-existent table query correctly failed");

    // Test 3: Create a table using migrate (since CREATE TABLE is a migration)
    connection
        .migrate("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("Failed to create test table");

    // Actually test with proper parameters first
    connection
        .execute(
            "INSERT INTO test (id, name) VALUES (?1, ?2)",
            vec![SqlParam::Integer(1), SqlParam::Text("test".to_string())],
        )
        .await
        .expect("Failed to insert test data");

    // Test 4: Unique constraint violation (duplicate primary key)
    let result = connection
        .execute(
            "INSERT INTO test (id, name) VALUES (?1, ?2)",
            vec![
                SqlParam::Integer(1),
                SqlParam::Text("duplicate".to_string()),
            ], // Same ID
        )
        .await;
    assert!(result.is_err(), "Duplicate primary key should fail");
    println!("Duplicate key correctly detected");

    // Test 4: Type mismatch
    let result = connection
        .execute(
            "INSERT INTO test (id, name) VALUES (?1, ?2)",
            vec![
                SqlParam::Text("not_a_number".to_string()), // Should be integer
                SqlParam::Text("test".to_string()),
            ],
        )
        .await;
    assert!(result.is_err(), "Type mismatch should fail");
    println!("Type mismatch correctly detected");

    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_transactions() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create setup
    let node_id = NodeId::from_seed(4);
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

    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[4u8; 32]);
    let attestor = Arc::new(MockAttestor::new());

    let network_manager = Arc::new(NetworkManager::new(
        node_id,
        transport,
        topology_manager.clone(),
        signing_key,
        ConnectionPoolConfig::default(),
        topology_arc.clone(),
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
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = Arc::new(engine.client());
    let store = SqlEngineStore::new(client.clone(), "test_transactions");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let connection = store
        .connect(vec![
            "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER)",
        ])
        .await
        .expect("Failed to connect");

    // Insert initial account data
    connection
        .execute(
            "INSERT INTO accounts (id, balance) VALUES (?1, ?2), (?3, ?4)",
            vec![
                SqlParam::Integer(1),
                SqlParam::Integer(1000),
                SqlParam::Integer(2),
                SqlParam::Integer(500),
            ],
        )
        .await
        .expect("Failed to insert accounts");

    // Test 1: Successful transaction (transfer money)
    let tx = connection
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Deduct from account 1
    tx.execute(
        "UPDATE accounts SET balance = balance - ?1 WHERE id = ?2",
        vec![SqlParam::Integer(200), SqlParam::Integer(1)],
    )
    .await
    .expect("Failed to update account 1");

    // Add to account 2
    tx.execute(
        "UPDATE accounts SET balance = balance + ?1 WHERE id = ?2",
        vec![SqlParam::Integer(200), SqlParam::Integer(2)],
    )
    .await
    .expect("Failed to update account 2");

    // Check balance within transaction
    let mut tx_rows = tx
        .query(
            "SELECT balance FROM accounts WHERE id = ?1",
            vec![SqlParam::Integer(1)],
        )
        .await
        .expect("Failed to query in transaction");

    let tx_row = tx_rows.next().await.expect("Expected row");
    match &tx_row[0] {
        SqlParam::IntegerWithName(_, balance) => assert_eq!(*balance, 800),
        _ => panic!("Expected balance"),
    }

    // Commit transaction
    tx.commit().await.expect("Failed to commit transaction");
    println!("Transaction committed successfully");

    // Verify the transaction succeeded
    let mut rows = connection
        .query("SELECT id, balance FROM accounts ORDER BY id", vec![])
        .await
        .expect("Failed to query accounts");

    let row1 = rows.next().await.expect("Expected row 1");
    match (&row1[0], &row1[1]) {
        (SqlParam::IntegerWithName(_, id), SqlParam::IntegerWithName(_, balance)) => {
            assert_eq!(*id, 1);
            assert_eq!(*balance, 800);
        }
        _ => panic!("Unexpected row format"),
    }

    let row2 = rows.next().await.expect("Expected row 2");
    match (&row2[0], &row2[1]) {
        (SqlParam::Integer(id), SqlParam::Integer(balance)) => {
            assert_eq!(*id, 2);
            assert_eq!(*balance, 700);
        }
        _ => panic!("Unexpected row format"),
    }
    println!("Balances correctly updated after commit");

    // Test 2: Rollback transaction
    let tx = connection
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Try to overdraw account 1
    tx.execute(
        "UPDATE accounts SET balance = balance - ?1 WHERE id = ?2",
        vec![SqlParam::Integer(1000), SqlParam::Integer(1)],
    )
    .await
    .expect("Failed to update in transaction");

    // Rollback
    tx.rollback().await.expect("Failed to rollback transaction");
    println!("Transaction rolled back successfully");

    // Verify rollback worked - balance should be unchanged
    let mut rows = connection
        .query(
            "SELECT balance FROM accounts WHERE id = ?1",
            vec![SqlParam::Integer(1)],
        )
        .await
        .expect("Failed to query after rollback");

    let row = rows.next().await.expect("Expected row");
    match &row[0] {
        SqlParam::IntegerWithName(_, balance) => assert_eq!(*balance, 800),
        _ => panic!("Expected balance"),
    }
    println!("Balance correctly unchanged after rollback");

    // Test 3: Transaction isolation (operations outside transaction don't see uncommitted changes)
    let tx = connection
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Update balance in transaction
    tx.execute(
        "UPDATE accounts SET balance = balance + ?1 WHERE id = ?2",
        vec![SqlParam::Integer(100), SqlParam::Integer(1)],
    )
    .await
    .expect("Failed to update in transaction");

    // Query outside transaction (using main connection) - should see old value
    let mut rows = connection
        .query(
            "SELECT balance FROM accounts WHERE id = ?1",
            vec![SqlParam::Integer(1)],
        )
        .await
        .expect("Failed to query outside transaction");

    let row = rows.next().await.expect("Expected row");
    match &row[0] {
        SqlParam::IntegerWithName(_, balance) => assert_eq!(*balance, 800),
        _ => panic!("Expected balance"),
    }
    println!("Transaction isolation working - uncommitted changes not visible outside");

    // Now commit the transaction
    tx.commit().await.expect("Failed to commit");

    // Query again - should see new value
    let mut rows = connection
        .query(
            "SELECT balance FROM accounts WHERE id = ?1",
            vec![SqlParam::Integer(1)],
        )
        .await
        .expect("Failed to query after commit");

    let row = rows.next().await.expect("Expected row");
    match &row[0] {
        SqlParam::IntegerWithName(_, balance) => assert_eq!(*balance, 900),
        _ => panic!("Expected balance"),
    }
    println!("Committed changes now visible");

    engine.stop().await.expect("Failed to stop engine");
}
