//! Integration tests for TCP transport

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_topology::{Node, Version};
use proven_topology::{NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport::{Config as TransportConfig, Transport};
use proven_transport_tcp::{TcpConfig, TcpTransport};
// No longer need to import verification components for manual construction
use proven_logger::info;
use proven_logger_macros::logged_tokio_test;

#[logged_tokio_test]
async fn test_tcp_transport_basic() {
    use proven_util::port_allocator::allocate_port;

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
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // Start both transports
    info!("Starting transport 1");
    let addr1 = transport1.start().await.unwrap();
    info!("Transport 1 listening on {addr1}");

    info!("Starting transport 2");
    let addr2 = transport2.start().await.unwrap();
    info!("Transport 2 listening on {addr2}");

    // Set up a task to consume incoming messages on transport 2
    let transport2_clone = transport2.clone();
    let node1_id_clone = node1_id.clone();
    let incoming_task = tokio::spawn(async move {
        use futures::StreamExt;

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

            // Verify this is the expected application message, not a verification message
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

    // Give everything time to initialize
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

    info!("TCP transport basic test completed");
}
