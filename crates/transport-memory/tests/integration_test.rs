//! Integration tests for memory transport

use bytes::Bytes;
use proven_topology::{Node, NodeId};
use proven_transport::Transport;
use proven_transport_memory::{MemoryOptions, MemoryTransport};
use std::{collections::HashSet, time::Duration};
use tokio::time::timeout;
use tracing::info;

#[tokio::test]
async fn test_memory_echo_server() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_node_id = NodeId::from_seed(10);
    let transport = MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(server_node_id),
    });

    // Create listener
    let listener = transport.listen().await.expect("Failed to create listener");

    // Start echo server
    let server_task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok(conn) => {
                    info!("Server accepted connection");

                    // Echo loop
                    tokio::spawn(async move {
                        loop {
                            match conn.recv().await {
                                Ok(data) => {
                                    info!("Server echoing {} bytes", data.len());
                                    if let Err(e) = conn.send(data).await {
                                        info!("Server send error: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    info!("Server receive error: {}", e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    info!("Accept error: {}", e);
                    break;
                }
            }
        }
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Connect client
    let node = Node::new(
        "test-az".to_string(),
        "http://127.0.0.1:8080".to_string(),
        server_node_id,
        "test-region".to_string(),
        HashSet::new(),
    );
    let client_transport = MemoryTransport::new_default();
    let client = client_transport
        .connect(&node)
        .await
        .expect("Failed to connect");

    // Test echo
    let test_messages = vec![
        Bytes::from("Hello, Memory!"),
        Bytes::from("Test message 2"),
        Bytes::from("🦀 Rust is awesome!"),
    ];

    for msg in test_messages {
        client.send(msg.clone()).await.expect("Failed to send");

        let response = timeout(Duration::from_secs(1), client.recv())
            .await
            .expect("Timeout")
            .expect("Failed to receive");

        assert_eq!(msg, response);
        info!(
            "Successfully echoed: {:?}",
            String::from_utf8_lossy(&response)
        );
    }

    // Cleanup
    client.close().await.expect("Failed to close client");
    server_task.abort();
}

#[tokio::test]
async fn test_multiple_clients() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_node_id = NodeId::from_seed(20);
    let transport = MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(server_node_id),
    });

    // Create listener
    let listener = transport.listen().await.expect("Failed to create listener");

    // Start server that handles multiple connections
    let server_task = tokio::spawn(async move {
        let mut connection_count = 0;

        loop {
            match listener.accept().await {
                Ok(conn) => {
                    connection_count += 1;
                    let conn_id = connection_count;
                    info!("Server accepted connection #{}", conn_id);

                    // Handle each connection
                    tokio::spawn(async move {
                        loop {
                            match conn.recv().await {
                                Ok(data) => {
                                    // Add connection ID to response
                                    let mut response = data.to_vec();
                                    response.extend_from_slice(
                                        format!(" [from conn #{conn_id}]").as_bytes(),
                                    );
                                    let response = Bytes::from(response);
                                    if let Err(e) = conn.send(response).await {
                                        info!("Connection {} send error: {}", conn_id, e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    info!("Connection {} receive error: {}", conn_id, e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    info!("Accept error: {}", e);
                    break;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Connect multiple clients
    let mut clients = vec![];
    for i in 0..3 {
        let node = Node::new(
            "test-az".to_string(),
            "http://127.0.0.1:8080".to_string(),
            server_node_id,
            "test-region".to_string(),
            HashSet::new(),
        );
        let client_transport = MemoryTransport::new_default();
        let client = client_transport
            .connect(&node)
            .await
            .expect("Failed to connect");
        clients.push((i, client));
    }

    // Send messages from each client
    for (id, client) in &clients {
        let msg = Bytes::from(format!("Hello from client {id}"));
        client.send(msg.clone()).await.expect("Failed to send");

        let response = timeout(Duration::from_secs(1), client.recv())
            .await
            .expect("Timeout")
            .expect("Failed to receive");

        // Verify response contains original message
        assert!(response.starts_with(&msg));
        info!(
            "Client {} received: {:?}",
            id,
            String::from_utf8_lossy(&response)
        );
    }

    // Cleanup
    for (_, client) in clients {
        let _ = client.close().await;
    }
    server_task.abort();
}

#[tokio::test]
async fn test_connection_closed() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_node_id = NodeId::from_seed(30);
    let transport = MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(server_node_id),
    });

    // Create listener
    let listener = transport.listen().await.expect("Failed to create listener");

    // Accept one connection then close it
    let server_task = tokio::spawn(async move {
        let conn = listener.accept().await.expect("Failed to accept");
        info!("Server closing connection immediately");
        conn.close().await.expect("Failed to close");
    });

    // Connect client
    let node = Node::new(
        "test-az".to_string(),
        "http://127.0.0.1:8080".to_string(),
        server_node_id,
        "test-region".to_string(),
        HashSet::new(),
    );
    let client_transport = MemoryTransport::new_default();
    let client = client_transport
        .connect(&node)
        .await
        .expect("Failed to connect");

    // Wait for server to close
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to send - should fail
    let result = client.send(Bytes::from("Hello")).await;
    assert!(result.is_err());

    // Try to receive - should fail
    let result = client.recv().await;
    assert!(result.is_err());

    // Cleanup
    let _ = server_task.await;
}

#[tokio::test]
async fn test_listener_close() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_node_id = NodeId::from_seed(40);
    let transport = MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(server_node_id),
    });

    // Create and immediately close listener
    let listener = transport.listen().await.expect("Failed to create listener");

    listener.close().await.expect("Failed to close listener");

    // Try to connect - should fail
    let node = Node::new(
        "test-az".to_string(),
        "http://127.0.0.1:8080".to_string(),
        server_node_id,
        "test-region".to_string(),
        HashSet::new(),
    );
    let client_transport = MemoryTransport::new_default();
    let result = client_transport.connect(&node).await;
    assert!(result.is_err());

    // Should be able to create new listener on same node
    let new_transport = MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(server_node_id),
    });
    let _new_listener = new_transport
        .listen()
        .await
        .expect("Should be able to listen again");
}
