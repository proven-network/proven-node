//! Integration tests for local-cluster

mod common;

use proven_local::NodeStatus;
use proven_local_cluster::test_utils::{create_test_cluster, wait_for_cluster_ready};
use proven_local_cluster::{LogFilter, UserIdentity};
use std::time::Duration;
use tracing::info;

#[tokio::test]
async fn test_independent_node_and_topology() {
    common::init_test_logging();
    let mut test_cluster = create_test_cluster(1).await.unwrap();
    let cluster = &mut test_cluster.cluster;

    // Get node ID
    let nodes: Vec<_> = cluster.get_all_nodes().into_iter().collect();
    let (node_id, _) = &nodes[0];

    // Add node to topology before starting it
    cluster.add_to_topology(node_id).await.unwrap();

    // Start node (already in topology)
    cluster.start_node(node_id).await.unwrap();

    // Wait for node to be running
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(30) {
            panic!("Timeout waiting for node to start");
        }

        if let Some(status) = cluster.get_node_status(node_id)
            && matches!(status, NodeStatus::Running)
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify topology has 1 node
    let topology = cluster.get_topology().await.unwrap();
    assert_eq!(topology.len(), 1);

    // Node should be running
    assert_eq!(
        cluster.get_node_status(node_id).unwrap(),
        NodeStatus::Running
    );

    // Remove from topology while it's running
    cluster.remove_from_topology(node_id).unwrap();

    // Node should still be running
    assert_eq!(
        cluster.get_node_status(node_id).unwrap(),
        NodeStatus::Running
    );

    // Topology should be empty
    let topology = cluster.get_topology().await.unwrap();
    assert_eq!(topology.len(), 0);

    // Cleanup
    test_cluster.cleanup().await.unwrap();
}

#[tokio::test]
async fn test_multi_user_rpc() {
    common::init_test_logging();
    let mut test_cluster = create_test_cluster(1).await.unwrap();
    let cluster_mut = &mut test_cluster.cluster;

    // Start node and add to topology
    let nodes: Vec<_> = cluster_mut.get_all_nodes().into_iter().collect();
    let (node_id, _) = &nodes[0];

    // Add to topology before starting
    cluster_mut.add_to_topology(node_id).await.unwrap();

    // wait_for_cluster_ready will start all nodes
    wait_for_cluster_ready(cluster_mut, Duration::from_secs(60))
        .await
        .unwrap();

    // Create users
    let alice = UserIdentity::new("alice");
    let bob = UserIdentity::new("bob");

    // Create RPC clients - both connect to the same node
    let mut alice_client = cluster_mut
        .create_rpc_client(node_id, Some(alice.clone()))
        .await
        .unwrap();
    let mut bob_client = cluster_mut
        .create_rpc_client(node_id, Some(bob.clone()))
        .await
        .unwrap();

    // Test basic operations
    let alice_session = alice_client.who_am_i().await.unwrap();
    let bob_session = bob_client.who_am_i().await.unwrap();

    // Sessions should be different based on the enum variant
    match (&alice_session, &bob_session) {
        (
            proven_core::WhoAmIResponse::Anonymous {
                session_id: alice_id,
                ..
            },
            proven_core::WhoAmIResponse::Anonymous {
                session_id: bob_id, ..
            },
        ) => {
            assert_ne!(alice_id, bob_id);
        }
        _ => panic!("Expected anonymous sessions"),
    }

    // Identify users
    alice_client.identify().await.unwrap();
    bob_client.identify().await.unwrap();

    // Alice creates an application
    let app_id = alice_client.create_application("test-app").await.unwrap();
    info!("Alice created application: {}", app_id);

    // Check Alice can list her apps
    let alice_apps = alice_client.list_applications_by_owner().await.unwrap();
    assert_eq!(alice_apps.len(), 1);
    assert_eq!(alice_apps[0].id, app_id);

    // Bob should see no apps
    let bob_apps = bob_client.list_applications_by_owner().await.unwrap();
    assert_eq!(bob_apps.len(), 0);

    // Cleanup
    test_cluster.cleanup().await.unwrap();
}

#[tokio::test]
async fn test_logging_system() {
    // For this test, we'll verify that the logging system works
    // by directly writing to it rather than relying on the global subscriber
    let mut test_cluster = create_test_cluster(1).await.unwrap();
    let cluster_mut = &mut test_cluster.cluster;

    // Start node
    let nodes: Vec<_> = cluster_mut.get_all_nodes().into_iter().collect();
    let (node_id, node_info) = &nodes[0];

    // Directly add some test logs to the cluster's log system
    use chrono::Utc;
    use proven_local_cluster::{LogEntry, LogLevel};

    let log_writer = cluster_mut.get_log_system().get_writer();

    // Add test logs directly
    log_writer.add_log(LogEntry {
        node_id: "main".to_string(),
        execution_order: 0,
        level: LogLevel::Info,
        message: "Test log from main thread".to_string(),
        timestamp: Utc::now(),
        target: Some("test".to_string()),
    });

    // Get the session ID directly from the cluster
    let session_id = cluster_mut.get_session_id();

    log_writer.add_log(LogEntry {
        node_id: format!(
            "node-{}-{}-{}",
            session_id,
            node_info.execution_order,
            &node_id.to_string()[..8]
        ),
        execution_order: node_info.execution_order,
        level: LogLevel::Error,
        message: "Test error log from node".to_string(),
        timestamp: Utc::now(),
        target: Some("test".to_string()),
    });

    cluster_mut.start_node(node_id).await.unwrap();

    // Wait for logs to be processed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get all logs
    let all_logs = cluster_mut.get_logs(&LogFilter::default()).unwrap();
    assert!(!all_logs.is_empty(), "Should have captured some logs");

    // Get logs for specific node
    let node_logs = cluster_mut.get_node_logs(node_id).unwrap();
    assert!(!node_logs.is_empty(), "Node should have logs");

    // All node logs should be from this node
    for log in &node_logs {
        assert!(log.node_id.contains(&node_info.execution_order.to_string()));
    }

    // Test log filtering by level
    let error_logs = cluster_mut
        .get_logs(&LogFilter {
            level: Some(LogLevel::Error),
            ..Default::default()
        })
        .unwrap();

    // All returned logs should be ERROR level
    for log in &error_logs {
        assert_eq!(log.level, LogLevel::Error);
    }

    // Export logs
    let export_path = std::env::temp_dir().join("test_logs.json");
    cluster_mut.export_logs(&export_path).unwrap();
    assert!(export_path.exists());

    // Cleanup
    std::fs::remove_file(export_path).ok();
    test_cluster.cleanup().await.unwrap();
}

#[tokio::test]
async fn test_node_restart() {
    common::init_test_logging();
    let mut test_cluster = create_test_cluster(1).await.unwrap();
    let cluster_mut = &mut test_cluster.cluster;

    let nodes: Vec<_> = cluster_mut.get_all_nodes().into_iter().collect();
    let (node_id, _) = &nodes[0];

    // Start node
    cluster_mut.start_node(node_id).await.unwrap();
    cluster_mut.add_to_topology(node_id).await.unwrap();

    // Wait for node to be running
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(30) {
            panic!("Timeout waiting for node to start");
        }

        if let Some(status) = cluster_mut.get_node_status(node_id)
            && matches!(status, NodeStatus::Running)
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Restart node
    cluster_mut.restart_node(node_id).await.unwrap();

    // Wait for node to restart and be running again
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(30) {
            panic!("Timeout waiting for node to restart");
        }

        if let Some(status) = cluster_mut.get_node_status(node_id)
            && matches!(status, NodeStatus::Running)
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Should still be in topology
    let topology = cluster_mut.get_topology().await.unwrap();
    assert_eq!(topology.len(), 1);

    // Cleanup
    test_cluster.cleanup().await.unwrap();
}

#[tokio::test]
async fn test_anonymous_rpc_client() {
    common::init_test_logging();
    let mut test_cluster = create_test_cluster(1).await.unwrap();
    let cluster_mut = &mut test_cluster.cluster;

    let nodes: Vec<_> = cluster_mut.get_all_nodes().into_iter().collect();
    let (node_id, _) = &nodes[0];

    // Add to topology before starting
    cluster_mut.add_to_topology(node_id).await.unwrap();

    // wait_for_cluster_ready will start all nodes
    wait_for_cluster_ready(cluster_mut, Duration::from_secs(60))
        .await
        .unwrap();

    // Create anonymous client
    let mut anon_client = cluster_mut.create_anonymous_client(node_id).await.unwrap();

    // Anonymous client can check session
    let session = anon_client.who_am_i().await.unwrap();
    match session {
        proven_core::WhoAmIResponse::Anonymous { session_id, .. } => {
            assert!(!session_id.is_empty());
        }
        _ => panic!("Expected anonymous session"),
    }

    // Anonymous client cannot create applications
    let result = anon_client.create_application("test").await;
    assert!(result.is_err());

    // Cleanup
    test_cluster.cleanup().await.unwrap();
}
