//! Simple integration test for service-based request/reply with longer timeouts

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_network::connection_pool::ConnectionPoolConfig;
use proven_network::service::ServiceContext;
use proven_network::{NetworkManager, NetworkMessage, Service, ServiceMessage};
use proven_topology::{Node, NodeId, TopologyManager, Version};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_tcp::{TcpOptions, TcpTransport};
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

impl NetworkMessage for TestServiceMessage {
    fn message_type() -> &'static str {
        "test_service_msg"
    }
}

impl NetworkMessage for TestServiceResponse {
    fn message_type() -> &'static str {
        "test_service_resp"
    }
}

impl ServiceMessage for TestServiceMessage {
    type Response = TestServiceResponse;

    fn service_id() -> &'static str {
        "test_service"
    }
}

/// Test service implementation
struct TestService {
    handler_called: Arc<tokio::sync::Mutex<bool>>,
}

#[async_trait]
impl Service for TestService {
    type Request = TestServiceMessage;

    async fn handle(
        &self,
        request: Self::Request,
        ctx: ServiceContext,
    ) -> proven_network::NetworkResult<<Self::Request as ServiceMessage>::Response> {
        info!(
            "Handler called! Sender: {}, Request: {:?}",
            ctx.sender, request
        );
        *self.handler_called.lock().await = true;

        match request {
            TestServiceMessage::Request { id, message } => Ok(TestServiceResponse::Response {
                id,
                reply: format!("Reply to: {message}"),
            }),
        }
    }
}

#[tokio::test]
async fn test_simple_request_reply() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
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

    // Create TCP transports with listen addresses
    let transport1 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node1_port}").parse().unwrap()),
    }));
    let transport2 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node2_port}").parse().unwrap()),
    }));

    // Start topology managers first
    info!("Starting topology managers");
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // Transports don't need to be started in the new architecture

    // Create network managers with generics
    let network1 = Arc::new(NetworkManager::new(
        node1_id.clone(),
        transport1.clone(),
        topology1,
        node1_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    ));

    let network2 = Arc::new(NetworkManager::new(
        node2_id.clone(),
        transport2.clone(),
        topology2,
        node2_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    ));

    // Start network managers
    info!("Starting network managers");
    network1.start().await.expect("Failed to start network1");
    network2.start().await.expect("Failed to start network2");

    // Give everything time to initialize
    info!("Waiting for initialization...");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Pre-connect from node2 to node1 to avoid verification timeout during request/response
    info!("Pre-establishing connection from node2 to node1");
    // The first request will establish the connection automatically
    // No need for manual ping anymore

    // Create and register service handler on node2
    let handler_called = Arc::new(tokio::sync::Mutex::new(false));
    let service = TestService {
        handler_called: handler_called.clone(),
    };

    info!("Registering service handler on node2");
    network2
        .register_service(service)
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
        .request_with_timeout(
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
    network1.stop().await.expect("Failed to shutdown network1");
    network2.stop().await.expect("Failed to shutdown network2");

    info!("Test completed successfully");
}

#[tokio::test]
async fn test_request_reply_benchmark() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    // Create two nodes with deterministic keys
    let node1_key = SigningKey::from_bytes(&[9u8; 32]);
    let node1_id = NodeId::from(node1_key.verifying_key());
    let node1_port = allocate_port();

    let node2_key = SigningKey::from_bytes(&[10u8; 32]);
    let node2_id = NodeId::from(node2_key.verifying_key());
    let node2_port = allocate_port();

    info!("Benchmark - Node 1: {} on port {}", node1_id, node1_port);
    info!("Benchmark - Node 2: {} on port {}", node2_id, node2_port);

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

    // Create TCP transports with listen addresses
    let transport1 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node1_port}").parse().unwrap()),
    }));
    let transport2 = Arc::new(TcpTransport::new(TcpOptions {
        listen_addr: Some(format!("127.0.0.1:{node2_port}").parse().unwrap()),
    }));

    // Start everything
    topology1.start().await.unwrap();
    topology2.start().await.unwrap();

    // Transports don't need to be started in the new architecture

    // Create network managers
    let network1 = Arc::new(NetworkManager::new(
        node1_id.clone(),
        transport1.clone(),
        topology1,
        node1_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    ));

    let network2 = Arc::new(NetworkManager::new(
        node2_id.clone(),
        transport2.clone(),
        topology2,
        node2_key,
        ConnectionPoolConfig::default(),
        governance.clone(),
        Arc::new(attestor.clone()),
    ));

    // Start network managers
    network1.start().await.expect("Failed to start network1");
    network2.start().await.expect("Failed to start network2");

    // Give everything time to initialize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Pre-establish connection by sending a dummy request
    info!("Pre-establishing connection");
    // The connection will be established automatically on first request
    // Send a dummy request to establish connection
    let _ = network2
        .request_with_timeout(
            node1_id.clone(),
            TestServiceMessage::Request {
                id: 0,
                message: "ping".to_string(),
            },
            Duration::from_secs(5),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Register service handler on node2

    network2
        .register_service(TestService {
            handler_called: Arc::new(tokio::sync::Mutex::new(false)),
        })
        .await
        .expect("Failed to register handler");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Benchmark parameters
    let num_requests = 100;
    let concurrent_requests = 10;

    info!(
        "Starting benchmark: {} total requests, {} concurrent",
        num_requests, concurrent_requests
    );

    let start_time = std::time::Instant::now();

    // Run concurrent requests
    let mut handles = Vec::new();
    for batch in 0..(num_requests / concurrent_requests) {
        let mut batch_handles = Vec::new();

        for i in 0..concurrent_requests {
            let network = network1.clone();
            let target = node2_id.clone();
            let request_id = (batch * concurrent_requests + i) as u64;

            let handle = tokio::spawn(async move {
                let request = TestServiceMessage::Request {
                    id: request_id,
                    message: format!("Benchmark request {request_id}"),
                };

                let start = std::time::Instant::now();
                let result = network
                    .request_with_timeout(target, request, Duration::from_secs(5))
                    .await;
                let latency = start.elapsed();

                match result {
                    Ok(TestServiceResponse::Response { id, reply: _ }) => {
                        assert_eq!(id, request_id);
                        Some(latency)
                    }
                    Err(e) => {
                        eprintln!("Request {request_id} failed: {e}");
                        None
                    }
                }
            });

            batch_handles.push(handle);
        }

        // Wait for batch to complete
        for handle in batch_handles {
            if let Ok(Some(latency)) = handle.await {
                handles.push(latency);
            }
        }
    }

    let total_time = start_time.elapsed();

    // Calculate statistics
    let successful_requests = handles.len();
    let mut latencies: Vec<_> = handles.into_iter().map(|d| d.as_micros()).collect();
    latencies.sort();

    let avg_latency = latencies.iter().sum::<u128>() / latencies.len() as u128;
    let min_latency = latencies.first().copied().unwrap_or(0);
    let max_latency = latencies.last().copied().unwrap_or(0);
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];

    let requests_per_second = (successful_requests as f64) / total_time.as_secs_f64();

    info!("=== Request-Response Benchmark Results ===");
    info!("Total requests: {}", num_requests);
    info!("Successful requests: {}", successful_requests);
    info!("Total time: {:?}", total_time);
    info!("Requests/second: {:.2}", requests_per_second);
    info!(
        "Latency (μs) - Min: {}, Avg: {}, Max: {}",
        min_latency, avg_latency, max_latency
    );
    info!("Latency (μs) - P50: {}, P95: {}, P99: {}", p50, p95, p99);

    // Shutdown
    network1.stop().await.expect("Failed to stop network1");
    network2.stop().await.expect("Failed to stop network2");

    info!("Benchmark completed successfully");
}
