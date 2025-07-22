//! Integration tests for WebSocket transport
//!
//! These tests verify that the WebSocket transport works correctly
//! with a real HTTP server and can handle discovery, connections,
//! and message passing between nodes.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use futures::StreamExt;
use proven_attestation_mock::MockAttestor;
use proven_logger::{error, info};
use proven_logger_macros::logged_tokio_test;
use proven_topology::{Node, NodeId, TopologyManager, Version};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport::{HttpIntegratedTransport, Transport};
use proven_transport_ws::{WebsocketConfig, WebsocketTransport};
use proven_util::port_allocator::allocate_port;
use tower_http::cors::CorsLayer;

/// Start an Axum server with the WebSocket transport router
async fn start_server(router: Router, port: u16) -> Result<tokio::task::JoinHandle<()>, String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    info!("Binding server to {addr}");

    let app = router.layer(CorsLayer::very_permissive());

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("Failed to bind TCP listener: {e}"))?;

    info!("Server bound successfully to {addr}");

    let handle = tokio::spawn(async move {
        info!("Starting to serve on {addr}");
        if let Err(e) = axum::serve(listener, app).await {
            error!("Server failed: {e}");
        }
    });

    Ok(handle)
}

#[logged_tokio_test]
async fn test_websocket_transport_basic() {
    // Create two nodes with deterministic keys
    let node1_key = SigningKey::from_bytes(&[1u8; 32]);
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node1_port = allocate_port();

    let node2_key = SigningKey::from_bytes(&[2u8; 32]);
    let node2_id = NodeId::from(node2_key.verifying_key());
    let node2_port = allocate_port();

    info!("Node 1: {node1_id} on port {node1_port}");
    info!("Node 2: {node2_id} on port {node2_port}");

    // Create mock governance with both nodes
    let attestor = MockAttestor::new();
    let pcrs = attestor.pcrs_sync();
    let governance = Arc::new(MockTopologyAdaptor::new(
        vec![
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node1_port}"),
                NodeId::from(node1_key.verifying_key()),
                "test-region".to_string(),
                HashSet::new(),
            ),
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node2_port}"),
                NodeId::from(node2_key.verifying_key()),
                "test-region".to_string(),
                HashSet::new(),
            ),
        ],
        vec![Version {
            ne_pcr0: pcrs.pcr0,
            ne_pcr1: pcrs.pcr1,
            ne_pcr2: pcrs.pcr2,
        }],
        "https://auth.test.com".to_string(),
        vec![],
    ));

    // Create topology managers
    let topology1 = Arc::new(TopologyManager::new(governance.clone(), node1_id.clone()));
    let topology2 = Arc::new(TopologyManager::new(governance.clone(), node2_id.clone()));

    // Create WebSocket transports
    let ws_config = WebsocketConfig::default();

    let transport1 = Arc::new(WebsocketTransport::new(
        ws_config.clone(),
        Arc::new(attestor.clone()),
        governance.clone(),
        node1_key,
        topology1.clone(),
    ));

    let transport2 = Arc::new(WebsocketTransport::new(
        ws_config,
        Arc::new(attestor.clone()),
        governance.clone(),
        node2_key,
        topology2.clone(),
    ));

    // Create router integrations
    let router1 = transport1
        .create_router_integration()
        .expect("Failed to create router 1");
    let router2 = transport2
        .create_router_integration()
        .expect("Failed to create router 2");

    // Start HTTP servers
    info!("Starting HTTP servers...");
    let _server1_handle = start_server(router1, node1_port)
        .await
        .expect("Failed to start server 1");
    let _server2_handle = start_server(router2, node2_port)
        .await
        .expect("Failed to start server 2");

    // Give servers time to start
    info!("Waiting for servers to start...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify servers are actually listening by checking health endpoint
    info!("Verifying servers are listening via health endpoint...");
    for (node_id, port) in [
        (node1_id.clone(), node1_port),
        (node2_id.clone(), node2_port),
    ] {
        let health_url = format!("http://127.0.0.1:{port}/consensus/health");
        match reqwest::get(&health_url).await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                info!("Health check for {node_id} on port {port}: status={status}, body={body}");
                if !status.is_success() {
                    panic!("Health check failed for port {port}: status={status}");
                }
            }
            Err(e) => {
                error!("Failed to check health for {node_id} on port {port}: {e}");
                panic!("Server health check failed on port {port}: {e}");
            }
        }
    }

    // Start topology managers
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // WebSocket transports don't need to be started - they're integrated with the HTTP server
    info!("WebSocket transports ready");

    // Set up a task to consume incoming messages on transport 2
    let transport2_clone = transport2.clone();
    let node1_id_clone = node1_id.clone();
    let incoming_task = tokio::spawn(async move {
        info!("Starting to listen for incoming messages on transport 2");
        let mut incoming = transport2_clone.incoming();

        if let Some(envelope) = incoming.next().await {
            info!(
                "Transport 2 received message from {}: type={}, payload_len={}, correlation_id={:?}",
                envelope.sender,
                envelope.message_type,
                envelope.payload.len(),
                envelope.correlation_id
            );

            // Verify this is the expected application message
            assert_eq!(
                envelope.message_type, "test.message",
                "Expected test.message, got {}",
                envelope.message_type
            );
            assert_eq!(
                envelope.payload,
                Bytes::from("Hello from node 1!"),
                "Payload mismatch"
            );
            assert_eq!(envelope.sender, node1_id_clone, "Sender mismatch");
        } else {
            panic!("No message received on transport 2");
        }
    });

    // Give everything time to initialize and establish WebSocket connections
    info!("Waiting for initialization...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Test sending a message from node1 to node2
    let test_payload = Bytes::from("Hello from node 1!");
    let test_message_type = "test.message";

    info!("Attempting to send message from node 1 ({node1_id}) to node 2 ({node2_id})");
    info!("Payload: {test_payload:?}, Message type: {test_message_type}");

    info!("About to call send_envelope...");
    let send_future = transport1.send_envelope(&node2_id, &test_payload, test_message_type, None);

    match tokio::time::timeout(Duration::from_secs(10), send_future).await {
        Ok(Ok(_)) => {
            info!("Message sent successfully!");
            // Give a moment for the message to be delivered
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(Err(e)) => {
            eprintln!("Failed to send message: {e:?}");
            panic!("Transport send failed: {e:?}");
        }
        Err(_) => {
            eprintln!("Send operation timed out after 10 seconds");
            panic!("Transport send timed out");
        }
    }

    // Wait for the incoming message to be received
    info!("Waiting for incoming message task to complete...");
    match tokio::time::timeout(Duration::from_secs(5), incoming_task).await {
        Ok(Ok(_)) => info!("Incoming message received successfully"),
        Ok(Err(e)) => panic!("Incoming task failed: {e:?}"),
        Err(_) => panic!("Incoming message task timed out"),
    }

    info!("WebSocket transport basic test completed");
}

#[logged_tokio_test]
async fn test_websocket_transport_connection_failure() {
    // Create node with deterministic key
    let node1_key = SigningKey::from_bytes(&[3u8; 32]);
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node1_port = allocate_port();

    // Create a non-existent node
    let node2_key = SigningKey::from_bytes(&[4u8; 32]);
    let node2_id = NodeId::from(node2_key.verifying_key());
    let node2_port = 19999; // Non-existent port

    info!("Node 1: {node1_id} on port {node1_port}");
    info!("Node 2 (non-existent): {node2_id} on port {node2_port}");

    // Create mock governance with both nodes
    let attestor = MockAttestor::new();
    let pcrs = attestor.pcrs_sync();
    let governance = Arc::new(MockTopologyAdaptor::new(
        vec![
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node1_port}"),
                node1_id.clone(),
                "test-region".to_string(),
                HashSet::new(),
            ),
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node2_port}"),
                node2_id.clone(),
                "test-region".to_string(),
                HashSet::new(),
            ),
        ],
        vec![Version {
            ne_pcr0: pcrs.pcr0,
            ne_pcr1: pcrs.pcr1,
            ne_pcr2: pcrs.pcr2,
        }],
        "https://auth.test.com".to_string(),
        vec![],
    ));

    // Create topology manager and transport for node 1
    let topology1 = Arc::new(TopologyManager::new(governance.clone(), node1_id.clone()));
    let transport1 = Arc::new(WebsocketTransport::new(
        WebsocketConfig::default(),
        Arc::new(attestor),
        governance,
        node1_key,
        topology1.clone(),
    ));

    // Create router and start server
    let router1 = transport1
        .create_router_integration()
        .expect("Failed to create router");
    let _server1 = start_server(router1, node1_port)
        .await
        .expect("Failed to start server");

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start topology manager (WebSocket transport doesn't need separate start)
    topology1.start().await.unwrap();

    // Wait a bit for initialization
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Try to send message to non-existent node
    let test_payload = Bytes::from("This should fail");
    let test_message_type = "test.message";

    info!("Attempting to send message to non-existent node");
    let start = tokio::time::Instant::now();
    let result = transport1
        .send_envelope(&node2_id, &test_payload, test_message_type, None)
        .await;
    let elapsed = start.elapsed();

    // Should fail
    assert!(result.is_err(), "Expected send to fail");
    info!(
        "Send failed as expected after {:?}: {:?}",
        elapsed,
        result.err()
    );

    info!("WebSocket transport connection failure test completed");
}
