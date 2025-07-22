//! Integration tests for client-service interactions

use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use proven_engine::{EngineBuilder, EngineConfig};
use proven_logger::info;
use proven_logger_macros::logged_test;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::service_responder::ServiceResponder;
use proven_messaging::stream::{InitializedStream, Stream};
use proven_messaging_engine::client::EngineMessagingClientOptions;
use proven_messaging_engine::service::EngineMessagingServiceOptions;
use proven_messaging_engine::stream::{EngineStream, EngineStreamOptions};
use proven_network::NetworkManager;
use proven_storage::manager::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::MemoryTransport;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};

/// Simple error type for test handlers
#[derive(Debug)]
struct TestError(String);

impl TestError {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for TestError {}

/// Test request type
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TestRequest {
    id: u64,
    query: String,
}

/// Test response type
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TestResponse {
    id: u64,
    result: String,
}

/// Implement conversions for request
impl TryFrom<Bytes> for TestRequest {
    type Error = serde_cbor::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(&bytes)
    }
}

impl TryInto<Bytes> for TestRequest {
    type Error = serde_cbor::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let vec = serde_cbor::to_vec(&self)?;
        Ok(Bytes::from(vec))
    }
}

/// Implement conversions for response
impl TryFrom<Bytes> for TestResponse {
    type Error = serde_cbor::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(&bytes)
    }
}

impl TryInto<Bytes> for TestResponse {
    type Error = serde_cbor::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let vec = serde_cbor::to_vec(&self)?;
        Ok(Bytes::from(vec))
    }
}

/// Test service handler that processes requests
#[derive(Clone, Debug)]
struct TestServiceHandler {
    processed_count: Arc<AtomicUsize>,
    responses: Arc<RwLock<Vec<TestResponse>>>,
}

impl TestServiceHandler {
    fn new() -> Self {
        Self {
            processed_count: Arc::new(AtomicUsize::new(0)),
            responses: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl ServiceHandler<TestRequest, serde_cbor::Error, serde_cbor::Error> for TestServiceHandler {
    type Error = TestError;
    type ResponseType = TestResponse;
    type ResponseDeserializationError = serde_cbor::Error;
    type ResponseSerializationError = serde_cbor::Error;

    async fn handle<R>(
        &self,
        request: TestRequest,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
                TestRequest,
                serde_cbor::Error,
                serde_cbor::Error,
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
    {
        self.processed_count.fetch_add(1, Ordering::SeqCst);

        // Create response based on request
        let response = TestResponse {
            id: request.id,
            result: format!("Processed: {}", request.query),
        };

        // Store response for verification
        self.responses.write().await.push(response.clone());

        // Send response
        Ok(responder.reply(response).await)
    }

    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        info!("Service caught up");
        Ok(())
    }
}

/// Helper to create a test engine client
async fn create_test_engine()
-> proven_engine::Client<MemoryTransport, MockTopologyAdaptor, MemoryStorage> {
    use std::sync::atomic::{AtomicU8, Ordering};
    static NODE_COUNTER: AtomicU8 = AtomicU8::new(1);

    let node_id = NodeId::from_seed(NODE_COUNTER.fetch_add(1, Ordering::SeqCst));

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(node_id.clone()));
    transport
        .register(&format!("memory://{node_id}"))
        .await
        .expect("Failed to register transport");

    // Create mock topology
    let node = TopologyNode::new(
        "test-az".to_string(),
        format!("memory://{node_id}"),
        node_id.clone(),
        "test-region".to_string(),
        Default::default(),
    );

    let topology =
        MockTopologyAdaptor::new(vec![], vec![], "https://auth.test.com".to_string(), vec![]);
    let _ = topology.add_node(node);
    let topology_manager = Arc::new(TopologyManager::new(Arc::new(topology), node_id.clone()));

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
        transport,
        topology_manager.clone(),
    ));

    // Create storage
    let storage_manager = Arc::new(StorageManager::new(MemoryStorage::new()));

    // Configure engine
    let mut config = EngineConfig::default();
    config.consensus.global.election_timeout_min = Duration::from_millis(50);
    config.consensus.global.election_timeout_max = Duration::from_millis(100);
    config.consensus.global.heartbeat_interval = Duration::from_millis(20);

    // Build and start engine
    let mut engine = EngineBuilder::new(node_id.clone())
        .with_config(config)
        .with_network(network_manager)
        .with_topology(topology_manager)
        .with_storage(storage_manager)
        .build()
        .await
        .expect("Failed to build engine");

    engine.start().await.expect("Failed to start engine");

    // Engine start() now ensures default group exists
    engine.client()
}

#[logged_test]
#[tokio::test]
async fn test_basic_client_service_interaction() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestRequest, _, _> =
        EngineStream::new("test_service", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create service
    let handler = TestServiceHandler::new();
    let service = initialized
        .service(
            "test_service_handler",
            EngineMessagingServiceOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create service");

    // Start the service
    use proven_bootable::Bootable;
    service.start().await.expect("Failed to start service");

    // Give service time to start
    sleep(Duration::from_millis(100)).await;

    // Create client
    let client = initialized
        .client::<_, TestServiceHandler>("test_client", EngineMessagingClientOptions::default())
        .await
        .expect("Failed to create client");

    // Send request
    let request = TestRequest {
        id: 1,
        query: "Hello, Service!".to_string(),
    };

    let response = timeout(Duration::from_secs(5), client.request(request.clone()))
        .await
        .expect("Request timed out")
        .expect("Failed to send request");

    // Verify response
    match response {
        ClientResponseType::Response(resp) => {
            assert_eq!(resp.id, 1);
            assert_eq!(resp.result, "Processed: Hello, Service!");
        }
        ClientResponseType::Stream(_) => panic!("Expected response, got stream"),
    }

    // Verify handler processed the request
    assert_eq!(handler.processed_count.load(Ordering::SeqCst), 1);

    // Shutdown service
    service
        .shutdown()
        .await
        .expect("Failed to shutdown service");
}

#[logged_test]
#[tokio::test]
async fn test_multiple_requests() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestRequest, _, _> = EngineStream::new("test_multi", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create service
    let handler = TestServiceHandler::new();
    let service = initialized
        .service(
            "multi_service",
            EngineMessagingServiceOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create service");

    // Start the service
    use proven_bootable::Bootable;
    service.start().await.expect("Failed to start service");
    sleep(Duration::from_millis(100)).await;

    // Create client
    let client = initialized
        .client::<_, TestServiceHandler>("multi_client", EngineMessagingClientOptions::default())
        .await
        .expect("Failed to create client");

    // Send multiple requests
    let mut responses = vec![];
    for i in 1..=5 {
        let request = TestRequest {
            id: i,
            query: format!("Request {i}"),
        };

        let response = timeout(Duration::from_secs(5), client.request(request))
            .await
            .expect("Request timed out")
            .expect("Failed to send request");

        match response {
            ClientResponseType::Response(resp) => responses.push(resp),
            ClientResponseType::Stream(_) => panic!("Expected response, got stream"),
        }
    }

    // Verify all responses
    for (i, response) in responses.iter().enumerate() {
        assert_eq!(response.id, (i + 1) as u64);
        assert_eq!(response.result, format!("Processed: Request {}", i + 1));
    }

    // Verify handler processed all requests
    assert_eq!(handler.processed_count.load(Ordering::SeqCst), 5);

    service
        .shutdown()
        .await
        .expect("Failed to shutdown service");
}

#[logged_test]
#[tokio::test]
async fn test_concurrent_requests() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestRequest, _, _> =
        EngineStream::new("test_concurrent", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create service
    let handler = TestServiceHandler::new();
    let service = initialized
        .service(
            "concurrent_service",
            EngineMessagingServiceOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create service");

    // Start the service
    use proven_bootable::Bootable;
    service.start().await.expect("Failed to start service");
    sleep(Duration::from_millis(100)).await;

    // Create client
    let client = Arc::new(
        initialized
            .client::<_, TestServiceHandler>(
                "concurrent_client",
                EngineMessagingClientOptions::default(),
            )
            .await
            .expect("Failed to create client"),
    );

    // Send concurrent requests
    let mut handles = vec![];
    for i in 1..=10 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let request = TestRequest {
                id: i,
                query: format!("Concurrent {i}"),
            };

            let response = timeout(Duration::from_secs(5), client_clone.request(request))
                .await
                .expect("Request timed out")
                .expect("Failed to send request");
            match response {
                ClientResponseType::Response(resp) => resp,
                ClientResponseType::Stream(_) => panic!("Expected response, got stream"),
            }
        });
        handles.push(handle);
    }

    // Wait for all requests
    let mut responses = vec![];
    for handle in handles {
        responses.push(handle.await.expect("Task failed"));
    }

    // Verify all responses received
    assert_eq!(responses.len(), 10);

    // Verify handler processed all requests
    assert_eq!(handler.processed_count.load(Ordering::SeqCst), 10);

    service
        .shutdown()
        .await
        .expect("Failed to shutdown service");
}

#[logged_test]
#[tokio::test]
async fn test_request_with_timeout() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestRequest, _, _> =
        EngineStream::new("test_timeout", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create client (no service running)
    let client = initialized
        .client::<_, TestServiceHandler>(
            "timeout_client",
            EngineMessagingClientOptions {
                timeout: Duration::from_millis(100),
            },
        )
        .await
        .expect("Failed to create client");

    // Send request (should timeout since no service is running)
    let request = TestRequest {
        id: 1,
        query: "This will timeout".to_string(),
    };

    let result = timeout(Duration::from_secs(1), client.request(request)).await;

    // Should timeout
    assert!(result.is_ok()); // outer timeout didn't trigger
    assert!(result.unwrap().is_err()); // inner request timed out
}

#[logged_test]
#[tokio::test]
async fn test_multiple_clients_one_service() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestRequest, _, _> =
        EngineStream::new("test_multi_client", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create service
    let handler = TestServiceHandler::new();
    let service = initialized
        .service(
            "single_service",
            EngineMessagingServiceOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create service");

    // Start the service
    use proven_bootable::Bootable;
    service.start().await.expect("Failed to start service");
    sleep(Duration::from_millis(100)).await;

    // Create multiple clients
    let mut clients = vec![];
    for i in 1..=3 {
        let client = initialized
            .client::<_, TestServiceHandler>(
                &format!("client_{i}"),
                EngineMessagingClientOptions::default(),
            )
            .await
            .expect("Failed to create client");
        clients.push(client);
    }

    // Send requests from each client
    let mut all_responses = vec![];
    for (i, client) in clients.iter().enumerate() {
        let request = TestRequest {
            id: (i + 1) as u64,
            query: format!("From client {}", i + 1),
        };

        let response = timeout(Duration::from_secs(5), client.request(request))
            .await
            .expect("Request timed out")
            .expect("Failed to send request");

        match response {
            ClientResponseType::Response(resp) => all_responses.push(resp),
            ClientResponseType::Stream(_) => panic!("Expected response, got stream"),
        }
    }

    // Verify all responses
    assert_eq!(all_responses.len(), 3);

    // Verify handler processed all requests
    assert_eq!(handler.processed_count.load(Ordering::SeqCst), 3);

    service
        .shutdown()
        .await
        .expect("Failed to shutdown service");
}

#[logged_test]
#[tokio::test]
async fn test_service_start_after_requests() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestRequest, _, _> =
        EngineStream::new("test_delayed", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create client first
    let client = initialized
        .client::<_, TestServiceHandler>("early_client", EngineMessagingClientOptions::default())
        .await
        .expect("Failed to create client");

    // Send requests before service starts (they'll be queued in the stream)
    let mut request_futures = vec![];
    for i in 1..=3 {
        let request = TestRequest {
            id: i,
            query: format!("Early request {i}"),
        };

        let future = client.request(request);
        request_futures.push(future);
    }

    // Now create and start the service
    let handler = TestServiceHandler::new();
    let service = initialized
        .service(
            "delayed_service",
            EngineMessagingServiceOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create service");

    use proven_bootable::Bootable;
    service.start().await.expect("Failed to start service");

    // Give service time to process queued requests
    sleep(Duration::from_millis(500)).await;

    // Now the requests should complete
    let mut responses = vec![];
    for future in request_futures {
        let response = timeout(Duration::from_secs(5), future)
            .await
            .expect("Request timed out")
            .expect("Failed to get response");
        match response {
            ClientResponseType::Response(resp) => responses.push(resp),
            ClientResponseType::Stream(_) => panic!("Expected response, got stream"),
        }
    }

    // Verify all responses
    assert_eq!(responses.len(), 3);
    for (i, response) in responses.iter().enumerate() {
        assert_eq!(response.id, (i + 1) as u64);
        assert_eq!(
            response.result,
            format!("Processed: Early request {}", i + 1)
        );
    }

    // Verify handler processed all requests
    assert_eq!(handler.processed_count.load(Ordering::SeqCst), 3);

    service
        .shutdown()
        .await
        .expect("Failed to shutdown service");
}

/// Test service handler that returns errors
#[derive(Clone, Debug)]
struct ErrorServiceHandler;

#[async_trait]
impl ServiceHandler<TestRequest, serde_cbor::Error, serde_cbor::Error> for ErrorServiceHandler {
    type Error = TestError;
    type ResponseType = TestResponse;
    type ResponseDeserializationError = serde_cbor::Error;
    type ResponseSerializationError = serde_cbor::Error;

    async fn handle<R>(
        &self,
        request: TestRequest,
        _responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
                TestRequest,
                serde_cbor::Error,
                serde_cbor::Error,
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
    {
        // Always return an error
        Err(TestError::new(format!(
            "Failed to process request {}",
            request.id
        )))
    }

    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[logged_test]
#[tokio::test]
async fn test_service_error_handling() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestRequest, _, _> = EngineStream::new("test_error", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create service with error handler
    let service = initialized
        .service(
            "error_service",
            EngineMessagingServiceOptions::default(),
            ErrorServiceHandler,
        )
        .await
        .expect("Failed to create service");

    // Start the service
    use proven_bootable::Bootable;
    service.start().await.expect("Failed to start service");
    sleep(Duration::from_millis(100)).await;

    // Create client
    let client = initialized
        .client::<_, ErrorServiceHandler>("error_client", EngineMessagingClientOptions::default())
        .await
        .expect("Failed to create client");

    // Send request (service will return error but shouldn't crash)
    let request = TestRequest {
        id: 1,
        query: "This will fail".to_string(),
    };

    // The current implementation doesn't propagate service errors to client,
    // so this will timeout
    let result = timeout(Duration::from_millis(500), client.request(request)).await;

    // Should timeout because error responses aren't implemented
    assert!(result.is_err());

    service
        .shutdown()
        .await
        .expect("Failed to shutdown service");
}
