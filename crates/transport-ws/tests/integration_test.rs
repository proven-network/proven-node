//! Integration tests for WebSocket transport

use axum::{Router, routing::get};
use bytes::Bytes;
use proven_topology::{Node, NodeId};
use proven_transport::Transport;
use proven_transport_ws::WebSocketTransport;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
use tracing::info;

#[tokio::test]
async fn test_websocket_echo_server() {
    let _ = tracing_subscriber::fmt::try_init();

    // Create transport
    let transport = WebSocketTransport::new();

    // Create a test server address
    let port = portpicker::pick_unused_port().expect("No ports available");
    let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    // Create a listener first
    let _listener = transport.listen().await.expect("Failed to create listener");

    // Create Axum app with WebSocket handler
    let app = Router::new();
    let app = transport
        .mount_into_router(app)
        .await
        .expect("Failed to mount WebSocket handler");

    // Add a health check endpoint
    let app = app.route("/health", get(|| async { "OK" }));

    // Start the server
    let server = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(server_addr)
            .await
            .expect("Failed to bind");
        info!("Test server listening on {}", server_addr);

        axum::serve(listener, app).await.expect("Server failed");
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get the listeners that were created by mount_into_router
    let listeners = transport.get_listeners().await;
    assert!(!listeners.is_empty(), "Should have at least one listener");
    let listener =
        Box::new(listeners.into_iter().next().unwrap()) as Box<dyn proven_transport::Listener>;

    // Start accepting connections in the background
    let accept_task = tokio::spawn(async move {
        let conn = timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("Accept timeout")
            .expect("Failed to accept connection");

        info!("Server accepted connection");

        // Echo server
        loop {
            match conn.recv().await {
                Ok(data) => {
                    info!("Server received: {} bytes", data.len());
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

    // Give accept task time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect as client
    let http_url = format!("http://127.0.0.1:{port}");
    info!("Node origin: {}", http_url);

    // Create a node for connection - pass HTTP URL as origin
    let node = Node::new(
        "test-az".to_string(),
        http_url,
        NodeId::from_seed(1),
        "test-region".to_string(),
        HashSet::new(),
    );

    let client_conn = transport.connect(&node).await.expect("Failed to connect");

    // Send test messages
    let test_messages = vec![
        Bytes::from("Hello, WebSocket!"),
        Bytes::from("Test message 2"),
        Bytes::from("Final message"),
    ];

    for msg in test_messages {
        info!("Client sending: {} bytes", msg.len());
        client_conn.send(msg.clone()).await.expect("Failed to send");

        let response = timeout(Duration::from_secs(1), client_conn.recv())
            .await
            .expect("Receive timeout")
            .expect("Failed to receive");

        assert_eq!(msg, response);
        info!("Client received echo: {} bytes", response.len());
    }

    // Close connection
    client_conn.close().await.expect("Failed to close");

    // Cleanup
    accept_task.abort();
    server.abort();
}

#[tokio::test]
async fn test_multiple_connections() {
    let _ = tracing_subscriber::fmt::try_init();

    let transport = WebSocketTransport::new();
    let port = portpicker::pick_unused_port().expect("No ports available");
    let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Create a listener first
    let _listener = transport.listen().await.expect("Failed to create listener");

    // Create server with WebSocket endpoint
    let app = Router::new();
    let app = transport
        .mount_into_router(app)
        .await
        .expect("Failed to mount");

    // Start server
    let server = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(server_addr)
            .await
            .expect("Failed to bind");
        axum::serve(listener, app).await.expect("Server failed");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Accept connections in background
    let transport_for_accept = transport.clone();
    let accept_task = tokio::spawn(async move {
        // Get the listeners that were created by mount_into_router
        let listeners = transport_for_accept.get_listeners().await;
        assert!(!listeners.is_empty(), "Should have at least one listener");
        let listener =
            Box::new(listeners.into_iter().next().unwrap()) as Box<dyn proven_transport::Listener>;

        let mut connections = vec![];

        // Accept 3 connections
        for i in 0..3 {
            let conn = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("Accept timeout")
                .expect("Failed to accept");

            info!("Accepted connection {}", i);
            connections.push(conn);
        }

        // Echo on all connections
        let mut handles = vec![];
        for (i, conn) in connections.into_iter().enumerate() {
            let handle = tokio::spawn(async move {
                loop {
                    match conn.recv().await {
                        Ok(data) => {
                            // Add connection ID to response
                            let mut response = data.to_vec();
                            response.extend_from_slice(format!(" from conn {i}").as_bytes());
                            let response = Bytes::from(response);
                            if let Err(e) = conn.send(response).await {
                                info!("Connection {} send error: {}", i, e);
                                break;
                            }
                        }
                        Err(e) => {
                            info!("Connection {} receive error: {}", i, e);
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all to complete
        for handle in handles {
            let _ = handle.await;
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create multiple client connections
    let mut clients = vec![];
    for i in 0..3 {
        let node = Node::new(
            "test-az".to_string(),
            format!("http://127.0.0.1:{port}"),
            NodeId::from_seed(i as u8),
            "test-region".to_string(),
            HashSet::new(),
        );
        let conn = transport.connect(&node).await.expect("Failed to connect");
        clients.push((i, conn));
    }

    // Send messages from each client
    for (id, conn) in &clients {
        let msg = Bytes::from(format!("Hello from client {id}"));
        conn.send(msg.clone()).await.expect("Failed to send");

        let response = timeout(Duration::from_secs(1), conn.recv())
            .await
            .expect("Timeout")
            .expect("Failed to receive");

        info!(
            "Client {} received: {:?}",
            id,
            String::from_utf8_lossy(&response)
        );
        assert!(response.starts_with(&msg));
    }

    // Cleanup
    for (_, conn) in clients {
        let _ = conn.close().await;
    }

    accept_task.abort();
    server.abort();
}
