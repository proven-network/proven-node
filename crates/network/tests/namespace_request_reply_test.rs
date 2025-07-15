//! Integration test for namespace-based request/reply

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_governance::{GovernanceNode, Version};
use proven_governance_mock::MockGovernance;
use proven_network::{NetworkManager, namespace::MessageType};
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Config as TransportConfig;
use proven_transport_tcp::{TcpConfig, TcpTransport};
use proven_util::port_allocator::allocate_port;
// No longer need to import verification components for manual construction
use serde::{Deserialize, Serialize};
use tracing::info;

/// Test request message
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestRequest {
    id: u64,
    message: String,
}

/// Test response message
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestResponse {
    id: u64,
    reply: String,
}

impl MessageType for TestRequest {
    fn message_type(&self) -> &'static str {
        "test_request"
    }
}

impl MessageType for TestResponse {
    fn message_type(&self) -> &'static str {
        "test_response"
    }
}

#[tokio::test]
async fn test_namespace_request_reply() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    // Create two nodes with deterministic keys
    let node1_key = SigningKey::from_bytes(&[1u8; 32]);
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node1_port = allocate_port();

    let node2_key = SigningKey::from_bytes(&[2u8; 32]);
    let node2_id = NodeId::from(node2_key.verifying_key());
    let node2_port = allocate_port();

    info!("Node 1: {} on port {}", node1_id, node1_port);
    info!("Node 2: {} on port {}", node2_id, node2_port);

    // Create mock governance with both nodes
    let attestor = MockAttestor::new();
    let pcrs = attestor.pcrs_sync();
    let governance = Arc::new(MockGovernance::new(
        vec![
            GovernanceNode {
                availability_zone: "test-az".to_string(),
                origin: format!("http://127.0.0.1:{node1_port}"),
                public_key: node1_key.verifying_key(),
                region: "test-region".to_string(),
                specializations: Default::default(),
            },
            GovernanceNode {
                availability_zone: "test-az".to_string(),
                origin: format!("http://127.0.0.1:{node2_port}"),
                public_key: node2_key.verifying_key(),
                region: "test-region".to_string(),
                specializations: Default::default(),
            },
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

    // Create TCP transports with simplified constructor
    let config1 = TcpConfig {
        transport: TransportConfig::default(),
        local_addr: format!("127.0.0.1:{node1_port}").parse().unwrap(),
        retry_attempts: 3,
        retry_delay_ms: 500,
    };

    let config2 = TcpConfig {
        transport: TransportConfig::default(),
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
    info!("Starting transport 1");
    let addr1 = transport1.start().await.unwrap();
    info!("Transport 1 listening on {}", addr1);

    info!("Starting transport 2");
    let addr2 = transport2.start().await.unwrap();
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
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Register namespace on both nodes
    let namespace = "test.namespace";
    info!("Registering namespace '{}' on both nodes", namespace);
    network1
        .register_namespace(namespace)
        .await
        .expect("Failed to register namespace on node1");
    network2
        .register_namespace(namespace)
        .await
        .expect("Failed to register namespace on node2");

    // Register request handler on node2
    let handler_called = Arc::new(tokio::sync::Mutex::new(false));
    let handler_called_clone = handler_called.clone();

    info!("Registering request handler on node2");
    network2
        .register_namespaced_request_handler::<TestRequest, TestResponse, _, _>(
            namespace,
            "test_request",
            move |sender, request| {
                let handler_called = handler_called_clone.clone();
                async move {
                    info!("Handler called! Sender: {}, Request: {:?}", sender, request);
                    *handler_called.lock().await = true;

                    Ok(TestResponse {
                        id: request.id,
                        reply: format!("Reply to: {}", request.message),
                    })
                }
            },
        )
        .await
        .expect("Failed to register handler");

    info!("Handler registered on node2");

    // Give time for handler registration to settle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send request from node1 to node2
    let request = TestRequest {
        id: 42,
        message: "Hello from node1".to_string(),
    };

    info!("Sending request from {} to {}", node1_id, node2_id);

    let response_result = network1
        .request_namespaced::<TestRequest, TestResponse>(
            namespace,
            node2_id.clone(),
            request.clone(),
            Duration::from_secs(5), // 5 seconds is plenty now that verification is fast
        )
        .await;

    // Check results
    match response_result {
        Ok(response) => {
            info!("Got response: {:?}", response);
            assert_eq!(response.id, 42);
            assert_eq!(response.reply, "Reply to: Hello from node1");
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

#[tokio::test]
async fn test_namespace_request_timeout() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    // Create two nodes with deterministic keys
    let node1_key = SigningKey::from_bytes(&[3u8; 32]);
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node1_port = allocate_port();

    let node2_key = SigningKey::from_bytes(&[4u8; 32]);
    let node2_id = NodeId::from(node2_key.verifying_key());
    let node2_port = allocate_port();

    info!("Node 1: {} on port {}", node1_id, node1_port);
    info!("Node 2: {} on port {}", node2_id, node2_port);

    // Create mock governance with both nodes
    let attestor = MockAttestor::new();
    let pcrs = attestor.pcrs_sync();
    let governance = Arc::new(MockGovernance::new(
        vec![
            GovernanceNode {
                availability_zone: "test-az".to_string(),
                origin: format!("http://127.0.0.1:{node1_port}"),
                public_key: node1_key.verifying_key(),
                region: "test-region".to_string(),
                specializations: Default::default(),
            },
            GovernanceNode {
                availability_zone: "test-az".to_string(),
                origin: format!("http://127.0.0.1:{node2_port}"),
                public_key: node2_key.verifying_key(),
                region: "test-region".to_string(),
                specializations: Default::default(),
            },
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

    // Create TCP transports with simplified constructor
    let config1 = TcpConfig {
        transport: TransportConfig::default(),
        local_addr: format!("127.0.0.1:{node1_port}").parse().unwrap(),
        retry_attempts: 3,
        retry_delay_ms: 500,
    };

    let config2 = TcpConfig {
        transport: TransportConfig::default(),
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

    // Start everything
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();
    transport1.start().await.unwrap();
    transport2.start().await.unwrap();

    // Create and start network managers
    let network1 = Arc::new(NetworkManager::new(node1_id.clone(), transport1, topology1));

    let network2 = Arc::new(NetworkManager::new(node2_id.clone(), transport2, topology2));

    network1.start().await.unwrap();
    network2.start().await.unwrap();

    // Give time to initialize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Register namespace but DON'T register any handler on node2
    let namespace = "test.namespace2";
    network1.register_namespace(namespace).await.unwrap();
    network2.register_namespace(namespace).await.unwrap();

    // Send request from node1 to node2 (should timeout)
    let request = TestRequest {
        id: 99,
        message: "This should timeout".to_string(),
    };

    info!("Sending request that should timeout");

    let response_result = network1
        .request_namespaced::<TestRequest, TestResponse>(
            namespace,
            node2_id.clone(),
            request,
            Duration::from_secs(2),
        )
        .await;

    // Should timeout
    match response_result {
        Err(e) => {
            info!("Got expected error: {}", e);
            assert!(e.to_string().contains("timed out") || e.to_string().contains("Timeout"));
        }
        Ok(_) => {
            panic!("Expected timeout but got response");
        }
    }

    // Shutdown
    network1.shutdown().await.unwrap();
    network2.shutdown().await.unwrap();

    info!("Timeout test completed successfully");
}
