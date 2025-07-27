//! Integration test for streaming functionality

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_network::{
    NetworkManager, NetworkResult, connection_pool::ConnectionPoolConfig,
    service::StreamingService, stream::Stream,
};
use proven_topology::{Node, NodeId, TopologyManager, Version};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_tcp::{TcpOptions, TcpTransport};
use proven_util::port_allocator::allocate_port;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{Level, info};

/// Simple counter service that counts messages
struct CounterStreamingService {
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl CounterStreamingService {
    fn new() -> Self {
        Self {
            counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn new_with_counter(counter: Arc<std::sync::atomic::AtomicU64>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl StreamingService for CounterStreamingService {
    fn stream_type(&self) -> &'static str {
        "counter"
    }

    async fn handle_stream(
        &self,
        peer: NodeId,
        stream: Stream,
        _metadata: HashMap<String, String>,
    ) -> NetworkResult<()> {
        info!("Counter stream opened by {}", peer);

        loop {
            match stream.recv().await {
                Some(_data) => {
                    // Increment counter
                    let count = self
                        .counter
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                        + 1;

                    // Send back the current count
                    let response = format!("{count}");
                    if let Err(e) = stream.send(Bytes::from(response)).await {
                        info!("Failed to send count: {}", e);
                        break;
                    }
                }
                None => {
                    info!("Counter stream closed");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_streaming_end_to_end() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    // Allocate ports for the test
    let node1_port = allocate_port();
    let node2_port = allocate_port();

    // Create signing keys and node IDs
    let node1_key = SigningKey::generate(&mut rand::thread_rng());
    let node2_key = SigningKey::generate(&mut rand::thread_rng());
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node2_id = NodeId::from(node2_key.verifying_key());

    // Create mock attestor and governance
    let attestor = MockAttestor::new();
    let pcrs = attestor.pcrs_sync();
    let governance = Arc::new(MockTopologyAdaptor::new(
        vec![
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node1_port}"),
                node1_id.clone(),
                "test-region".to_string(),
                Default::default(),
            ),
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node2_port}"),
                node2_id.clone(),
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

    // Start topology managers
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // Create network managers
    let transport1 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node1_port}").parse().unwrap()),
    }));
    let transport2 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node2_port}").parse().unwrap()),
    }));

    let manager1 = NetworkManager::new(
        node1_id.clone(),
        transport1,
        topology1.clone(),
        node1_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    );

    let manager2 = NetworkManager::new(
        node2_id.clone(),
        transport2,
        topology2.clone(),
        node2_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    );

    // Register counter service on node 2
    let counter_service = CounterStreamingService::new();
    manager2
        .register_streaming_service(counter_service)
        .await
        .unwrap();

    // Start both managers
    manager1.start().await.unwrap();
    manager2.start().await.unwrap();

    // Give services time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Open a stream from node1 to node2
    let mut metadata = HashMap::new();
    metadata.insert("test_id".to_string(), "1".to_string());

    let stream = manager1
        .open_stream(node2_id, "counter", metadata)
        .await
        .expect("Failed to open stream");

    // Send some messages and verify counts
    for i in 1..=5 {
        stream
            .send(Bytes::from(format!("Message {i}")))
            .await
            .unwrap();

        let response = stream.recv().await.expect("Failed to receive response");
        let count: u64 = std::str::from_utf8(&response).unwrap().parse().unwrap();
        assert_eq!(count, i as u64);
    }

    // Close stream by dropping it
    drop(stream);

    // Stop managers
    manager1.stop().await.unwrap();
    manager2.stop().await.unwrap();
}

#[tokio::test]
async fn test_multiple_concurrent_streams() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    // Allocate ports
    let node1_port = allocate_port();
    let node2_port = allocate_port();

    // Create signing keys and node IDs
    let node1_key = SigningKey::generate(&mut rand::thread_rng());
    let node2_key = SigningKey::generate(&mut rand::thread_rng());
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node2_id = NodeId::from(node2_key.verifying_key());

    // Create mock attestor and governance
    let attestor = MockAttestor::new();
    let pcrs = attestor.pcrs_sync();
    let governance = Arc::new(MockTopologyAdaptor::new(
        vec![
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node1_port}"),
                node1_id.clone(),
                "test-region".to_string(),
                Default::default(),
            ),
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node2_port}"),
                node2_id.clone(),
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

    // Start topology managers
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // Create network managers
    let transport1 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node1_port}").parse().unwrap()),
    }));
    let transport2 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node2_port}").parse().unwrap()),
    }));

    let manager1 = Arc::new(NetworkManager::new(
        node1_id.clone(),
        transport1,
        topology1.clone(),
        node1_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    ));

    let manager2 = NetworkManager::new(
        node2_id.clone(),
        transport2,
        topology2.clone(),
        node2_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    );

    // Register counter service on node 2 with shared counter
    let shared_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter_service = CounterStreamingService::new_with_counter(shared_counter.clone());
    manager2
        .register_streaming_service(counter_service)
        .await
        .unwrap();

    // Start both managers
    manager1.start().await.unwrap();
    manager2.start().await.unwrap();

    // Give services time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Open multiple concurrent streams
    let mut handles = vec![];

    for stream_id in 0..3 {
        let manager = manager1.clone();
        let node2 = node2_id.clone();

        let handle = tokio::spawn(async move {
            let mut metadata = HashMap::new();
            metadata.insert("stream_id".to_string(), stream_id.to_string());

            let stream = manager
                .open_stream(node2, "counter", metadata)
                .await
                .expect("Failed to open stream");

            // Send messages on this stream
            for i in 0..10 {
                let msg = format!("Stream {stream_id} Message {i}");
                stream.send(Bytes::from(msg)).await.unwrap();

                let _response = stream.recv().await.expect("Failed to receive response");
            }
        });

        handles.push(handle);
    }

    // Wait for all streams to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify total count (3 streams * 10 messages each)
    assert_eq!(
        shared_counter.load(std::sync::atomic::Ordering::Acquire),
        30
    );

    // Stop managers
    manager1.stop().await.unwrap();
    manager2.stop().await.unwrap();
}

#[tokio::test]
async fn test_incoming_streams() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    // Allocate ports
    let node1_port = allocate_port();
    let node2_port = allocate_port();

    // Create signing keys and node IDs
    let node1_key = SigningKey::generate(&mut rand::thread_rng());
    let node2_key = SigningKey::generate(&mut rand::thread_rng());
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node2_id = NodeId::from(node2_key.verifying_key());

    // Create mock attestor and governance
    let attestor = MockAttestor::new();
    let pcrs = attestor.pcrs_sync();
    let governance = Arc::new(MockTopologyAdaptor::new(
        vec![
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node1_port}"),
                node1_id.clone(),
                "test-region".to_string(),
                Default::default(),
            ),
            Node::new(
                "test-az".to_string(),
                format!("http://127.0.0.1:{node2_port}"),
                node2_id.clone(),
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

    // Start topology managers
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // Create network managers
    let transport1 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node1_port}").parse().unwrap()),
    }));
    let transport2 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node2_port}").parse().unwrap()),
    }));

    let manager1 = NetworkManager::new(
        node1_id.clone(),
        transport1,
        topology1.clone(),
        node1_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    );

    let manager2 = NetworkManager::new(
        node2_id.clone(),
        transport2,
        topology2.clone(),
        node2_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    );

    // Start both managers
    manager1.start().await.unwrap();
    manager2.start().await.unwrap();

    // Give services time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get incoming streams receiver on node1
    let incoming_streams = manager1.incoming_streams();

    // Open a stream from node2 to node1
    let mut metadata = HashMap::new();
    metadata.insert("purpose".to_string(), "test".to_string());

    let stream2 = manager2
        .open_stream(node1_id, "custom_stream", metadata.clone())
        .await
        .expect("Failed to open stream");

    // Send initial message
    stream2.send(Bytes::from("Hello from node2")).await.unwrap();

    // Receive incoming stream on node1
    let incoming = incoming_streams.recv_async().await.unwrap();
    assert_eq!(incoming.metadata, metadata);

    let stream1 = incoming.stream;

    // Receive message
    let msg = stream1.recv().await.expect("Failed to receive message");
    assert_eq!(msg, Bytes::from("Hello from node2"));

    // Send response
    stream1.send(Bytes::from("Hello from node1")).await.unwrap();

    // Receive response on node2
    let response = stream2.recv().await.expect("Failed to receive response");
    assert_eq!(response, Bytes::from("Hello from node1"));

    // Stop managers
    manager1.stop().await.unwrap();
    manager2.stop().await.unwrap();
}
