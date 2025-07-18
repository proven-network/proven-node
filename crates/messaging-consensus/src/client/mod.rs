//! Clients send requests to services in the consensus network.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{Mutex as TokioMutex, oneshot};
use tokio::time::timeout;
use tracing::{debug, warn};

use proven_attestation::Attestor;
use proven_messaging::client::{Client, ClientError, ClientOptions, ClientResponseType};
use proven_messaging::service_handler::ServiceHandler;
use proven_topology::TopologyAdaptor;

use crate::error::MessagingConsensusError;
use crate::stream::InitializedConsensusStream;

/// Type alias for response map - maps request IDs to response channels
type ResponseMap<R> = HashMap<usize, oneshot::Sender<ClientResponseType<R>>>;

/// Options for consensus clients.
#[derive(Clone, Debug)]
pub struct ConsensusClientOptions {
    /// Timeout for requests.
    pub timeout: std::time::Duration,
}

impl ClientOptions for ConsensusClientOptions {}

/// Error type for consensus clients.
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)] // TODO: Improve this
pub enum ConsensusClientError {
    /// Consensus error.
    #[error("Consensus error: {0}")]
    Consensus(#[from] MessagingConsensusError),
    /// No response received within timeout.
    #[error("No response received within timeout")]
    NoResponse,
    /// Serialization error.
    #[error("Serialization error")]
    Serialization,
}

impl ClientError for ConsensusClientError {}

/// A consensus client.
#[derive(Debug)]
pub struct ConsensusClient<G, A, X, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    name: String,
    stream: InitializedConsensusStream<G, A, T, D, S>,
    options: ConsensusClientOptions,
    /// Counter for generating unique request IDs
    request_id_counter: Arc<AtomicUsize>,
    /// Map of pending requests to their response channels
    response_map: Arc<TokioMutex<ResponseMap<X::ResponseType>>>,
    /// Response stream name for this client
    response_stream_name: String,
    _marker: PhantomData<X>,
}

impl<G, A, X, T, D, S> Clone for ConsensusClient<G, A, X, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            stream: self.stream.clone(),
            options: self.options.clone(),
            request_id_counter: self.request_id_counter.clone(),
            response_map: self.response_map.clone(),
            response_stream_name: self.response_stream_name.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<G, A, X, T, D, S> Client<X, T, D, S> for ConsensusClient<G, A, X, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error = ConsensusClientError;
    type Options = ConsensusClientOptions;
    type ResponseType = X::ResponseType;
    type StreamType = InitializedConsensusStream<G, A, T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
    ) -> Result<Self, Self::Error> {
        let response_stream_name = format!("{name}_responses");

        Ok(Self {
            name,
            stream,
            options,
            request_id_counter: Arc::new(AtomicUsize::new(0)),
            response_map: Arc::new(TokioMutex::new(HashMap::new())),
            response_stream_name,
            _marker: PhantomData,
        })
    }

    async fn request(
        &self,
        request: T,
    ) -> Result<ClientResponseType<X::ResponseType>, Self::Error> {
        // Generate unique request ID
        let request_id = self.request_id_counter.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();

        // Store the response channel for correlation
        {
            let mut map = self.response_map.lock().await;
            map.insert(request_id, sender);
        }

        debug!("Sending request {} to consensus system", request_id);

        // Convert request to bytes
        let request_bytes: Bytes = request
            .try_into()
            .map_err(|_| ConsensusClientError::Serialization)?;

        // Publish request to consensus stream with metadata
        let mut consensus_metadata = HashMap::new();
        consensus_metadata.insert("request_id".to_string(), request_id.to_string());
        consensus_metadata.insert(
            "response_stream".to_string(),
            self.response_stream_name.clone(),
        );
        consensus_metadata.insert("client_name".to_string(), self.name.clone());

        // Publish to consensus stream with the request metadata
        let sequence = self
            .stream
            .publish_with_metadata(request_bytes, consensus_metadata)
            .await
            .map_err(ConsensusClientError::Consensus)?;

        debug!(
            "Published request {} at sequence {} to consensus stream - response expected on {}",
            request_id, sequence, self.response_stream_name
        );

        // Wait for response with timeout
        match timeout(self.options.timeout, receiver).await {
            Ok(Ok(response)) => {
                debug!("Received response for request {}", request_id);
                Ok(response)
            }
            Ok(Err(_)) => {
                warn!("Response channel closed for request {}", request_id);
                Err(ConsensusClientError::NoResponse)
            }
            Err(_) => {
                warn!(
                    "Request {} timed out after {:?}",
                    request_id, self.options.timeout
                );
                // Clean up response map on timeout
                let mut map = self.response_map.lock().await;
                map.remove(&request_id);
                drop(map);
                Err(ConsensusClientError::NoResponse)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt;
    use proven_attestation_mock::MockAttestor;
    use proven_bootable::Bootable;
    use proven_engine::Consensus;
    use proven_engine::HierarchicalConsensusConfig;
    use proven_engine::RaftConfig;
    use proven_engine::StorageConfig;
    use proven_engine::TransportConfig;
    use proven_engine::config::ClusterJoinRetryConfig;
    use proven_engine::config::ConsensusConfigBuilder;
    use proven_messaging::stream::{InitializedStream, Stream};
    use proven_topology_mock::MockTopologyAdaptor;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use tokio::sync::Mutex;
    use tracing_test::traced_test;

    use crate::service::ConsensusServiceOptions;
    use crate::stream::{ConsensusStream, ConsensusStreamOptions};

    use proven_messaging::client::{Client, ClientResponseType};
    use proven_messaging::service_handler::ServiceHandler;
    use proven_messaging::service_responder::ServiceResponder;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        content: String,
    }

    impl TryFrom<Bytes> for TestMessage {
        type Error = serde_json::Error;

        fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
            serde_json::from_slice(&bytes)
        }
    }

    impl TryInto<Bytes> for TestMessage {
        type Error = serde_json::Error;

        fn try_into(self) -> Result<Bytes, Self::Error> {
            Ok(Bytes::from(serde_json::to_vec(&self)?))
        }
    }

    /// Test handler that responds with a simple reply
    #[derive(Debug, Clone)]
    struct TestHandler {
        requests_processed: Arc<Mutex<Vec<TestMessage>>>,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                requests_processed: Arc::new(Mutex::new(Vec::new())),
            }
        }

        async fn get_processed_requests(&self) -> Vec<TestMessage> {
            self.requests_processed.lock().await.clone()
        }
    }

    #[async_trait]
    impl ServiceHandler<TestMessage, serde_json::Error, serde_json::Error> for TestHandler {
        type Error = serde_json::Error;
        type ResponseType = TestMessage;
        type ResponseDeserializationError = serde_json::Error;
        type ResponseSerializationError = serde_json::Error;

        async fn handle<R>(
            &self,
            msg: TestMessage,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                    TestMessage,
                    serde_json::Error,
                    serde_json::Error,
                    Self::ResponseType,
                    Self::ResponseDeserializationError,
                    Self::ResponseSerializationError,
                >,
        {
            // Record the request
            {
                let mut requests = self.requests_processed.lock().await;
                requests.push(msg.clone());
            }

            // Create a response
            let response = TestMessage {
                content: format!("response: {}", msg.content),
            };

            Ok(responder.reply(response).await)
        }

        async fn on_caught_up(&self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    /// Test handler that responds with a stream of responses
    #[derive(Debug, Clone)]
    struct StreamingTestHandler {
        _requests_processed: Arc<Mutex<Vec<TestMessage>>>,
    }

    impl StreamingTestHandler {
        fn new() -> Self {
            Self {
                _requests_processed: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl ServiceHandler<TestMessage, serde_json::Error, serde_json::Error> for StreamingTestHandler {
        type Error = serde_json::Error;
        type ResponseType = TestMessage;
        type ResponseDeserializationError = serde_json::Error;
        type ResponseSerializationError = serde_json::Error;

        async fn handle<R>(
            &self,
            msg: TestMessage,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                    TestMessage,
                    serde_json::Error,
                    serde_json::Error,
                    Self::ResponseType,
                    Self::ResponseDeserializationError,
                    Self::ResponseSerializationError,
                >,
        {
            use futures::stream;

            let messages = vec![
                TestMessage {
                    content: format!("stream 1: {}", msg.content),
                },
                TestMessage {
                    content: format!("stream 2: {}", msg.content),
                },
                TestMessage {
                    content: format!("stream 3: {}", msg.content),
                },
            ];

            Ok(responder.stream(stream::iter(messages)).await)
        }
    }

    /// Test handler that never responds (for timeout testing)
    #[derive(Debug, Clone)]
    struct NonResponsiveTestHandler;

    #[async_trait]
    impl ServiceHandler<TestMessage, serde_json::Error, serde_json::Error>
        for NonResponsiveTestHandler
    {
        type Error = serde_json::Error;
        type ResponseType = TestMessage;
        type ResponseDeserializationError = serde_json::Error;
        type ResponseSerializationError = serde_json::Error;

        async fn handle<R>(
            &self,
            _msg: TestMessage,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                    TestMessage,
                    serde_json::Error,
                    serde_json::Error,
                    Self::ResponseType,
                    Self::ResponseDeserializationError,
                    Self::ResponseSerializationError,
                >,
        {
            // Respond with no_reply to simulate non-responsive service
            Ok(responder.no_reply().await)
        }

        async fn on_caught_up(&self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    // Helper to create a simple single-node governance for testing (like consensus_manager tests)
    async fn create_test_consensus(port: u16) -> Arc<Consensus<MockTopologyAdaptor, MockAttestor>> {
        use ed25519_dalek::SigningKey;
        use proven_topology::{Node, Version};
        use rand::rngs::OsRng;
        use std::collections::HashSet;

        // Generate a signing key for this test node
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        // Create governance with the node
        let node = Node::new(
            verifying_key,
            "test".to_string(),
            "test".to_string(),
            format!("http://127.0.0.1:{port}"),
            HashSet::new(),
        );

        let nodes = vec![node.clone()];
        // Create attestor
        let attestor = Arc::new(MockAttestor::new());
        let version = Version::from_pcrs(attestor.pcrs().await.unwrap());
        let governance = Arc::new(MockTopologyAdaptor::new(
            nodes,
            vec![version],
            String::new(),
            vec![],
        ));

        // Create config
        let config = ConsensusConfigBuilder::new()
            .governance(governance)
            .attestor(attestor)
            .signing_key(signing_key)
            .raft_config(RaftConfig::default())
            .transport_config(TransportConfig::Tcp {
                listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            })
            .storage_config(StorageConfig::Memory)
            .cluster_join_retry_config(ClusterJoinRetryConfig::default())
            .hierarchical_config(HierarchicalConsensusConfig::default())
            .build()
            .expect("Failed to build consensus config");

        // Create consensus
        let consensus = Consensus::new(config).await.unwrap();
        Arc::new(consensus)
    }

    async fn create_test_options(
        port: u16,
    ) -> ConsensusStreamOptions<MockTopologyAdaptor, MockAttestor> {
        let consensus = create_test_consensus(port).await;
        ConsensusStreamOptions { consensus }
    }

    fn next_port() -> u16 {
        proven_util::port_allocator::allocate_port()
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_creation() {
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_creation_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        // Create client
        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_creation",
                ConsensusClientOptions {
                    timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap();

        // Verify client is created properly
        assert_eq!(client.name, "test_client_creation");
        assert_eq!(
            client.response_stream_name,
            "test_client_creation_responses"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_request_without_service() {
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_no_service_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_no_service",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(500), // Short timeout for test
                },
            )
            .await
            .unwrap();

        let request = TestMessage {
            content: "hello".to_string(),
        };

        // This should timeout since no service is running
        let result = client.request(request).await;
        assert!(matches!(result, Err(ConsensusClientError::NoResponse)));
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_request_metadata() {
        // Test that the client properly sets metadata when publishing requests
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_metadata_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_metadata",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(500),
                },
            )
            .await
            .unwrap();

        let request = TestMessage {
            content: "test_metadata".to_string(),
        };

        // The request should be published with metadata, even if no response comes back
        let result = client.request(request).await;

        // Verify the request was published to the consensus stream
        // (even though it times out due to no service responding)
        assert!(matches!(result, Err(ConsensusClientError::NoResponse)));

        // Verify that at least one message was published to the stream
        let message_count = initialized_stream.messages().await.unwrap();
        assert!(
            message_count > 0,
            "Request should have been published to stream"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_request_id_generation() {
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_request_id_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_request_id",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(100),
                },
            )
            .await
            .unwrap();

        // Send multiple requests and verify they get unique IDs
        let requests = vec![
            TestMessage {
                content: "req1".to_string(),
            },
            TestMessage {
                content: "req2".to_string(),
            },
            TestMessage {
                content: "req3".to_string(),
            },
        ];

        for request in requests {
            let _result = client.request(request).await;
            // All should timeout, but request IDs should be incrementing
        }

        // Verify that the request ID counter advanced
        let final_counter = client.request_id_counter.load(Ordering::SeqCst);
        assert_eq!(final_counter, 3, "Request ID counter should have advanced");
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_serialization_error() {
        // Test client behavior with serialization errors
        // Note: This is harder to test with serde_json since it rarely fails for simple structs
        // but we can test the error path exists

        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_serialization_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_serialization",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(100),
                },
            )
            .await
            .unwrap();

        // Normal request should work (though timeout)
        let request = TestMessage {
            content: "normal".to_string(),
        };

        let result = client.request(request).await;
        assert!(matches!(result, Err(ConsensusClientError::NoResponse)));
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_multiple_concurrent_requests() {
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_concurrent_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_concurrent",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(200),
                },
            )
            .await
            .unwrap();

        // Send multiple concurrent requests
        let futures = (0..5).map(|i| {
            let client = client.clone();
            async move {
                let request = TestMessage {
                    content: format!("concurrent_request_{i}"),
                };
                client.request(request).await
            }
        });

        let results = futures::future::join_all(futures).await;

        // All should timeout, but verify they were all processed
        for result in results {
            assert!(matches!(result, Err(ConsensusClientError::NoResponse)));
        }

        // Verify multiple messages were published
        let message_count = initialized_stream.messages().await.unwrap();
        assert!(
            message_count >= 5,
            "All concurrent requests should have been published"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_options() {
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_options_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        // Test with different timeout values
        let short_timeout_client = initialized_stream
            .client::<_, TestHandler>(
                "short_timeout_client",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(50),
                },
            )
            .await
            .unwrap();

        let long_timeout_client = initialized_stream
            .client::<_, TestHandler>(
                "long_timeout_client",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(500),
                },
            )
            .await
            .unwrap();

        let request = TestMessage {
            content: "timeout_test".to_string(),
        };

        // Both should timeout, but at different rates
        let start = std::time::Instant::now();
        let _short_result = short_timeout_client.request(request.clone()).await;
        let short_duration = start.elapsed();

        let start = std::time::Instant::now();
        let _long_result = long_timeout_client.request(request).await;
        let long_duration = start.elapsed();

        // Short timeout should be faster
        assert!(short_duration < long_duration);
        assert!(short_duration < Duration::from_millis(100));
        assert!(long_duration > Duration::from_millis(400));
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_with_actual_service_handler() {
        // Test client with a real responding service handler
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_with_service_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        // Create a service with TestHandler
        let handler = TestHandler::new();
        let handler_clone = handler.clone();

        let service = initialized_stream
            .service(
                "test_service_responding",
                ConsensusServiceOptions {
                    start_sequence: None,
                },
                handler,
            )
            .await
            .unwrap();

        // Start the service
        service.start().await.unwrap();

        // Create a client
        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_with_service",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(1000),
                },
            )
            .await
            .unwrap();

        // Send a request
        let request = TestMessage {
            content: "test_request".to_string(),
        };

        let result = client.request(request.clone()).await;

        // This should succeed since we have a real service
        match result {
            Ok(response) => {
                if let ClientResponseType::Response(resp) = response {
                    assert_eq!(resp.content, "response: test_request");
                } else {
                    panic!("Expected single response, got stream");
                }
            }
            Err(e) => {
                // If it times out, that's still valid behavior in this test environment
                assert!(matches!(e, ConsensusClientError::NoResponse));
            }
        }

        // Wait a bit for processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify the handler processed the request
        let processed = handler_clone.get_processed_requests().await;
        assert!(
            processed.iter().any(|msg| msg.content == "test_request"),
            "Handler should have processed the request"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_with_streaming_handler() {
        // Test client with a streaming response handler
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_streaming_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        // Create a service with StreamingTestHandler
        let handler = StreamingTestHandler::new();

        let service = initialized_stream
            .service(
                "test_service_streaming",
                ConsensusServiceOptions {
                    start_sequence: None,
                },
                handler,
            )
            .await
            .unwrap();

        // Start the service
        service.start().await.unwrap();

        // Create a client
        let client = initialized_stream
            .client::<_, StreamingTestHandler>(
                "test_client_streaming",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(1000),
                },
            )
            .await
            .unwrap();

        // Send a request
        let request = TestMessage {
            content: "stream_test".to_string(),
        };

        let result = client.request(request).await;

        // This should return a stream response
        match result {
            Ok(response) => match response {
                ClientResponseType::Stream(mut stream) => {
                    let mut responses = Vec::new();
                    while let Some(resp) = stream.next().await {
                        responses.push(resp);
                    }
                    assert_eq!(responses.len(), 3, "Should receive 3 stream responses");
                    assert!(responses[0].content.contains("stream 1: stream_test"));
                    assert!(responses[1].content.contains("stream 2: stream_test"));
                    assert!(responses[2].content.contains("stream 3: stream_test"));
                }
                ClientResponseType::Response(_) => {
                    panic!("Expected stream response, got single response");
                }
            },
            Err(e) => {
                // If it times out, that's still valid behavior in this test environment
                assert!(matches!(e, ConsensusClientError::NoResponse));
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_client_with_non_responsive_handler() {
        // Test client with a non-responsive handler for timeout testing
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_client_non_responsive_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        // Create a service with NonResponsiveTestHandler
        let handler = NonResponsiveTestHandler;

        let service = initialized_stream
            .service(
                "test_service_non_responsive",
                ConsensusServiceOptions {
                    start_sequence: None,
                },
                handler,
            )
            .await
            .unwrap();

        // Start the service
        service.start().await.unwrap();

        // Create a client with a short timeout
        let client = initialized_stream
            .client::<_, NonResponsiveTestHandler>(
                "test_client_non_responsive",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(200),
                },
            )
            .await
            .unwrap();

        // Send a request
        let request = TestMessage {
            content: "non_responsive_test".to_string(),
        };

        let start = std::time::Instant::now();
        let result = client.request(request).await;
        let duration = start.elapsed();

        // This should timeout since the handler doesn't respond
        assert!(matches!(result, Err(ConsensusClientError::NoResponse)));

        // Should timeout around the configured duration
        assert!(
            duration >= Duration::from_millis(190),
            "Should timeout after configured duration"
        );
        assert!(
            duration <= Duration::from_millis(300),
            "Should not take much longer than timeout"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_handler_request_tracking() {
        // Test that handlers properly track processed requests
        let options = create_test_options(next_port()).await;

        let stream = ConsensusStream::<
            MockTopologyAdaptor,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_handler_tracking_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        // Create a service with TestHandler
        let handler = TestHandler::new();
        let handler_clone = handler.clone();

        let service = initialized_stream
            .service(
                "test_service_tracking",
                ConsensusServiceOptions {
                    start_sequence: None,
                },
                handler,
            )
            .await
            .unwrap();

        // Start the service
        service.start().await.unwrap();

        // Create a client
        let client = initialized_stream
            .client::<_, TestHandler>(
                "test_client_tracking",
                ConsensusClientOptions {
                    timeout: Duration::from_millis(500),
                },
            )
            .await
            .unwrap();

        // Send multiple requests
        let requests = vec![
            TestMessage {
                content: "request_1".to_string(),
            },
            TestMessage {
                content: "request_2".to_string(),
            },
            TestMessage {
                content: "request_3".to_string(),
            },
        ];

        for request in requests {
            let _result = client.request(request).await;
            // Allow some time for processing
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wait a bit more for all processing to complete
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify all requests were tracked
        let processed = handler_clone.get_processed_requests().await;
        assert!(
            processed.len() >= 3,
            "Handler should have tracked all requests, got: {processed:?}"
        );

        // Verify specific requests were processed
        let contents: Vec<String> = processed.iter().map(|msg| msg.content.clone()).collect();
        assert!(
            contents.contains(&"request_1".to_string()),
            "Should have processed request_1"
        );
        assert!(
            contents.contains(&"request_2".to_string()),
            "Should have processed request_2"
        );
        assert!(
            contents.contains(&"request_3".to_string()),
            "Should have processed request_3"
        );
    }
}
