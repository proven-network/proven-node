//! Integration test for http-proxy-engine using engine directly

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::{Router, response::IntoResponse};
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};
use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_engine::{EngineBuilder, EngineConfig};
use proven_http_proxy::{ProxyClient, ProxyService};
use proven_network::{NetworkManager, connection_pool::ConnectionPoolConfig};
use proven_storage::manager::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::{MemoryOptions, MemoryTransport};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EchoResponse {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
}

async fn echo_handler(
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let path = uri
        .path_and_query()
        .map_or_else(|| uri.path().to_string(), std::string::ToString::to_string);

    let echo_response = EchoResponse {
        method: method.to_string(),
        path: path.clone(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect(),
        body: if body.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&body).to_string())
        },
    };

    let response_body = serde_json::to_string(&echo_response).unwrap();
    let mut response_headers = HeaderMap::new();
    response_headers.insert("X-Echo-Method", method.to_string().parse().unwrap());
    response_headers.insert("X-Echo-Path", path.parse().unwrap());

    (StatusCode::OK, response_headers, response_body).into_response()
}

async fn run_echo_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    let app = Router::new().fallback(echo_handler);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });

    (addr, handle)
}

#[tokio::test]
async fn test_http_proxy_integration() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(1);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(node_id.clone()),
    }));

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

    let topology_arc = Arc::new(topology);
    let topology_manager = Arc::new(TopologyManager::new(topology_arc.clone(), node_id.clone()));

    // Create signing key for network manager
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);

    // Create mock attestor
    let attestor = Arc::new(MockAttestor::new());

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
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

    // Test 1: Start Echo Server
    let (echo_addr, echo_server_handle) = run_echo_server().await;
    println!("Echo server started on {echo_addr}");

    // Test 2: Setup Proxy Client
    let client_listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .unwrap();
    let client_addr = client_listener.local_addr().unwrap();
    let client_port = client_addr.port();
    drop(client_listener);

    let proxy_client = ProxyClient::new(client.clone(), "test-proxy", client_port);
    proxy_client
        .start()
        .await
        .expect("Failed to start proxy client");

    println!("Proxy client started on port {client_port}");

    // Test 3: Setup Proxy Service
    let proxy_service = ProxyService::new(client.clone(), "test-proxy", echo_addr);
    proxy_service
        .start()
        .await
        .expect("Failed to start proxy service");

    println!("Proxy service started, forwarding to {echo_addr}");

    // Give services time to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test 4: Make Request through Proxy Client
    let http_client = reqwest::Client::new();
    let test_path = "/test/path?query=1";
    let test_body = "Hello, Proxy!";
    let test_url = format!("http://127.0.0.1:{client_port}{test_path}");

    println!("Making request to {test_url}");
    let response = http_client
        .post(&test_url)
        .header("X-Custom-Header", "TestValue")
        .body(test_body)
        .send()
        .await
        .expect("Failed to send request to proxy client");

    // Test 5: Verify Response
    assert_eq!(response.status(), StatusCode::OK);

    let headers = response.headers();
    assert_eq!(headers.get("X-Echo-Method").unwrap(), "POST");
    assert_eq!(headers.get("X-Echo-Path").unwrap(), test_path);

    let body_bytes = response
        .bytes()
        .await
        .expect("Failed to read response body");
    let echo_response: EchoResponse =
        serde_json::from_slice(&body_bytes).expect("Failed to parse echo response body");

    assert_eq!(echo_response.method, "POST");
    assert_eq!(echo_response.path, test_path);
    assert_eq!(echo_response.body.unwrap(), test_body);

    let custom_header = echo_response
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-custom-header"));
    assert!(custom_header.is_some());
    assert_eq!(custom_header.unwrap().1, "TestValue");

    println!("Test passed! Request was successfully proxied through engine streams");

    // Test 6: Multiple requests
    for i in 0..5 {
        let test_url = format!("http://127.0.0.1:{client_port}/test/{i}");
        let response = http_client
            .get(&test_url)
            .send()
            .await
            .expect("Failed to send request");

        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        assert_eq!(headers.get("X-Echo-Method").unwrap(), "GET");
        assert_eq!(headers.get("X-Echo-Path").unwrap(), &format!("/test/{i}"));
    }

    println!("Multiple request test passed!");

    // Cleanup
    proxy_client
        .shutdown()
        .await
        .expect("Client shutdown failed");
    proxy_service
        .shutdown()
        .await
        .expect("Service shutdown failed");
    echo_server_handle.abort();
    let _ = echo_server_handle.await;

    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_http_proxy_timeout() {
    // Initialize logging
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // Create a simple single-node setup
    let node_id = NodeId::from_seed(2);

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(MemoryOptions {
        listen_node_id: Some(node_id.clone()),
    }));

    // Create mock topology
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

    let topology_arc = Arc::new(topology);
    let topology_manager = Arc::new(TopologyManager::new(topology_arc.clone(), node_id.clone()));

    // Create signing key
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[2u8; 32]);

    // Create mock attestor
    let attestor = Arc::new(MockAttestor::new());

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
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

    let mut engine = EngineBuilder::new(node_id.clone())
        .with_config(config)
        .with_network(network_manager)
        .with_topology(topology_manager)
        .with_storage(storage_manager)
        .build()
        .await
        .expect("Failed to build engine");

    engine.start().await.expect("Failed to start engine");

    // Wait for initialization
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = Arc::new(engine.client());

    // Setup proxy client without service (to test timeout)
    let client_listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .unwrap();
    let client_port = client_listener.local_addr().unwrap().port();
    drop(client_listener);

    let proxy_client = ProxyClient::new(client.clone(), "test-timeout", client_port);
    proxy_client
        .start()
        .await
        .expect("Failed to start proxy client");

    // Make request without service running
    let http_client = reqwest::Client::new();
    let test_url = format!("http://127.0.0.1:{client_port}/timeout");

    let response = http_client
        .get(&test_url)
        .send()
        .await
        .expect("Failed to send request");

    // Should get gateway timeout
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

    // Cleanup
    proxy_client
        .shutdown()
        .await
        .expect("Client shutdown failed");
    engine.stop().await.expect("Failed to stop engine");
}
