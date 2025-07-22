//! Simple integration test for service-based request/reply with longer timeouts

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_network::{NetworkManager, ServiceMessage};
use proven_topology::{Node, NodeId, TopologyManager, Version};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport::{Config as TransportConfig, Transport};
use proven_transport_tcp::{TcpConfig, TcpTransport};
use proven_util::port_allocator::allocate_port;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Test service message
#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestServiceMessage {
    Request { id: u64, message: String },
}

/// Test service response
#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestServiceResponse {
    Response { id: u64, reply: String },
}

impl ServiceMessage for TestServiceMessage {
    type Response = TestServiceResponse;

    fn service_id() -> &'static str {
        "test_service"
    }
}

#[tokio::test]
async fn test_simple_request_reply() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create two nodes with deterministic keys
    let node1_key = SigningKey::from_bytes(&[5u8; 32]);
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node1_port = allocate_port();

    let node2_key = SigningKey::from_bytes(&[6u8; 32]);
    let node2_id = NodeId::from(node2_key.verifying_key());
    let node2_port = allocate_port();

    info!("Node 1: {} on port {}", node1_id, node1_port);
    info!("Node 2: {} on port {}", node2_id, node2_port);

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
                Default::default(),
            ),
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node2_port}"),
                NodeId::from(node2_key.verifying_key()),
                "test-region".to_string(),
                Default::default(),
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

    // Create TCP transports with simplified constructor and longer connection timeout
    let mut transport_config1 = TransportConfig::default();
    transport_config1.connection.connection_timeout = Duration::from_millis(10000); // 10 seconds

    let mut transport_config2 = TransportConfig::default();
    transport_config2.connection.connection_timeout = Duration::from_millis(10000); // 10 seconds

    let config1 = TcpConfig {
        transport: transport_config1,
        local_addr: format!("127.0.0.1:{node1_port}").parse().unwrap(),
        retry_attempts: 3,
        retry_delay_ms: 500,
    };

    let config2 = TcpConfig {
        transport: transport_config2,
        local_addr: format!("127.0.0.1:{node2_port}").parse().unwrap(),
        retry_attempts: 3,
        retry_delay_ms: 500,
    };

    let transport1 = Arc::new(TcpTransport::new(
        config1,
        Arc::new(attestor.clone()),
        governance.clone(),
        node1_key,
        topology1.clone(),
    ));

    let transport2 = Arc::new(TcpTransport::new(
        config2,
        Arc::new(attestor.clone()),
        governance.clone(),
        node2_key,
        topology2.clone(),
    ));

    // Start topology managers first
    info!("Starting topology managers");
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // Start both transports
    info!("Starting transports");
    let addr1 = transport1.start().await.unwrap();
    let addr2 = transport2.start().await.unwrap();
    info!("Transport 1 listening on {}", addr1);
    info!("Transport 2 listening on {}", addr2);

    // Create network managers
    let network1 = Arc::new(NetworkManager::new(
        node1_id.clone(),
        transport1.clone(),
        topology1,
    ));

    let network2 = Arc::new(NetworkManager::new(
        node2_id.clone(),
        transport2.clone(),
        topology2,
    ));

    // Start network managers
    info!("Starting network managers");
    network1.start().await.expect("Failed to start network1");
    network2.start().await.expect("Failed to start network2");

    // Give everything time to initialize
    info!("Waiting for initialization...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Pre-connect from node2 to node1 to avoid verification timeout during request/response
    info!("Pre-establishing connection from node2 to node1");
    transport2
        .send_envelope(&node1_id, &bytes::Bytes::from("ping"), "ping", None)
        .await
        .unwrap();

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Register service handler on node2
    let handler_called = Arc::new(tokio::sync::Mutex::new(false));
    let handler_called_clone = handler_called.clone();

    info!("Registering service handler on node2");
    network2
        .register_service::<TestServiceMessage, _>(move |sender, message| {
            let handler_called = handler_called_clone.clone();
            Box::pin(async move {
                info!("Handler called! Sender: {}, Message: {:?}", sender, message);
                *handler_called.lock().await = true;

                match message {
                    TestServiceMessage::Request { id, message } => {
                        Ok(TestServiceResponse::Response {
                            id,
                            reply: format!("Reply to: {message}"),
                        })
                    }
                }
            })
        })
        .await
        .expect("Failed to register handler");

    info!("Handler registered on node2");

    // Give time for handler registration to settle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send request from node1 to node2 with longer timeout
    let request = TestServiceMessage::Request {
        id: 42,
        message: "Hello from node1".to_string(),
    };

    info!("Sending request from {} to {}", node1_id, node2_id);

    let response_result = network1
        .service_request(
            node2_id.clone(),
            request,
            Duration::from_secs(15), // 15 second timeout
        )
        .await;

    // Check results
    match response_result {
        Ok(response) => {
            info!("Got response: {:?}", response);
            match response {
                TestServiceResponse::Response { id, reply } => {
                    assert_eq!(id, 42);
                    assert_eq!(reply, "Reply to: Hello from node1");
                }
            }
            assert!(
                *handler_called.lock().await,
                "Handler should have been called"
            );
        }
        Err(e) => {
            panic!("Request failed: {e}");
        }
    }

    // Shutdown
    info!("Shutting down");
    network1
        .shutdown()
        .await
        .expect("Failed to shutdown network1");
    network2
        .shutdown()
        .await
        .expect("Failed to shutdown network2");

    info!("Test completed successfully");
}
