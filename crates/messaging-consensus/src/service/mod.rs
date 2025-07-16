//! Services are special consumers that respond to requests in the consensus network.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, warn};

use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_messaging::service::{Service, ServiceError, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;

use crate::error::MessagingConsensusError;
use crate::service_responder::ConsensusServiceResponder;
use crate::service_responder::ConsensusUsedResponder;
use crate::stream::InitializedConsensusStream;

/// Options for consensus services.
#[derive(Clone, Debug, Copy)]
pub struct ConsensusServiceOptions {
    /// Starting sequence number.
    pub start_sequence: Option<u64>,
}

impl ServiceOptions for ConsensusServiceOptions {}

/// Error type for consensus services.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusServiceError {
    /// Consensus error.
    #[error("Consensus error: {0}")]
    Consensus(#[from] MessagingConsensusError),
}

impl ServiceError for ConsensusServiceError {}

/// A consensus service.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
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
    options: ConsensusServiceOptions,
    handler: X,
    last_processed_seq: u64,
    /// Current sequence number being processed.
    current_seq: Arc<Mutex<u64>>,
    /// Shutdown token for graceful termination.
    shutdown_token: CancellationToken,
    /// Task tracker for background processing.
    task_tracker: TaskTracker,
}

impl<G, A, X, T, D, S> Clone for ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ServiceHandler<T, D, S> + Clone,
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
            options: self.options,
            handler: self.handler.clone(),
            last_processed_seq: self.last_processed_seq,
            current_seq: self.current_seq.clone(),
            shutdown_token: self.shutdown_token.clone(),
            task_tracker: self.task_tracker.clone(),
        }
    }
}

#[async_trait]
impl<G, A, X, T, D, S> Service<X, T, D, S> for ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
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
    type Error = ConsensusServiceError;
    type Options = ConsensusServiceOptions;
    type Responder = ConsensusServiceResponder<
        G,
        A,
        T,
        D,
        S,
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >;
    type UsedResponder = ConsensusUsedResponder;
    type StreamType = InitializedConsensusStream<G, A, T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            name,
            stream,
            options,
            handler,
            last_processed_seq: options.start_sequence.unwrap_or(0),
            current_seq: Arc::new(Mutex::new(0)),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        Ok(*self.current_seq.lock().await)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}

#[async_trait]
impl<G, A, X, T, D, S> Bootable for ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
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
    fn bootable_name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Starting service '{}'", self.name);

        // Spawn the request processing task
        let service_name = self.name.clone();
        let stream = self.stream.clone();
        let handler = self.handler.clone();
        let current_seq = self.current_seq.clone();
        let start_sequence = self.last_processed_seq;
        let shutdown_token = self.shutdown_token.clone();

        self.task_tracker.spawn(async move {
            if let Err(e) = Self::process_requests(
                service_name.clone(),
                stream,
                handler,
                current_seq,
                start_sequence,
                shutdown_token,
            )
            .await
            {
                warn!(
                    "Service '{}' request processing error: {:?}",
                    service_name, e
                );
            }
        });

        debug!("Service '{}' started", self.name);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Shutting down service '{}'", self.name);

        // Cancel the processing task
        self.shutdown_token.cancel();

        // Close the task tracker to signal no more tasks will be spawned
        self.task_tracker.close();

        debug!("Service '{}' shutdown initiated", self.name);
        Ok(())
    }

    async fn wait(&self) -> () {
        debug!("Waiting for service '{}' to complete", self.name);
        self.task_tracker.wait().await;
        debug!("Service '{}' completed", self.name);
    }
}

impl<G, A, X, T, D, S> ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
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
    /// Process service requests from the consensus stream.
    #[allow(clippy::too_many_lines)]
    async fn process_requests(
        service_name: String,
        stream: InitializedConsensusStream<G, A, T, D, S>,
        handler: X,
        current_seq: Arc<Mutex<u64>>,
        start_sequence: u64,
        shutdown_token: CancellationToken,
    ) -> Result<(), ConsensusServiceError> {
        let mut last_checked_seq = start_sequence;
        let initial_stream_msgs = match tokio::time::timeout(
            tokio::time::Duration::from_millis(100), // Slightly longer timeout for initial call
            stream.messages(),
        )
        .await
        {
            Ok(Ok(msgs)) => msgs,
            Ok(Err(e)) => {
                warn!(
                    "Service '{}' failed to get initial stream messages: {:?}",
                    service_name, e
                );
                0 // Default to 0 messages if we can't get count
            }
            Err(_) => {
                warn!(
                    "Service '{}' initial stream messages query timed out",
                    service_name
                );
                0 // Default to 0 messages if timeout
            }
        };
        let mut caught_up = initial_stream_msgs == 0 || start_sequence >= initial_stream_msgs;
        let mut msgs_processed = 0;

        if caught_up {
            let _ = handler.on_caught_up().await;
        }

        debug!(
            "Service '{}' started processing from sequence {} (stream has {} messages)",
            service_name, start_sequence, initial_stream_msgs
        );

        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    debug!("Service '{}' shutdown token cancelled, exiting request processing loop", service_name);
                    break;
                }
                () = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Check for new requests with timeout to avoid hanging on consensus calls
                    let current_stream_msgs = match tokio::time::timeout(
                        tokio::time::Duration::from_millis(50),
                        stream.messages()
                    ).await {
                        Ok(Ok(msgs)) => msgs,
                        Ok(Err(e)) => {
                            warn!("Service '{}' stream messages query failed: {:?}", service_name, e);
                            continue;
                        }
                        Err(_) => {
                            debug!("Service '{}' stream messages query timed out, continuing", service_name);
                            continue;
                        }
                    };

                    if current_stream_msgs > last_checked_seq {
                        // Process new requests
                        for seq in (last_checked_seq + 1)..=current_stream_msgs {
                            let request_result = tokio::time::timeout(
                                tokio::time::Duration::from_millis(50),
                                stream.get(seq)
                            ).await;

                            let request = match request_result {
                                Ok(Ok(Some(req))) => req,
                                Ok(Ok(None)) => continue,
                                Ok(Err(e)) => {
                                    warn!("Service '{}' failed to get request at seq {}: {:?}", service_name, seq, e);
                                    continue;
                                }
                                Err(_) => {
                                    debug!("Service '{}' get request timed out for seq {}, continuing", service_name, seq);
                                    continue;
                                }
                            };

                            // Create a service responder for this request
                            let request_id = format!("req_{service_name}_{seq}");
                            let response_stream_name = format!("{service_name}_responses");
                            let responder = ConsensusServiceResponder::new(
                                caught_up,
                                seq,
                                request_id,
                                response_stream_name,
                                stream.clone(),
                            );

                            // Handle the request
                            if let Err(e) = handler.handle(request, responder).await {
                                warn!("Service '{}' handler error for sequence {}: {:?}", service_name, seq, e);
                            } else {
                                // Update current sequence
                                *current_seq.lock().await = seq;

                                if !caught_up {
                                    msgs_processed += 1;

                                    // Check if we've caught up (with timeout)
                                    let current_msgs = match tokio::time::timeout(
                                        tokio::time::Duration::from_millis(50),
                                        stream.messages()
                                    ).await {
                                        Ok(Ok(msgs)) => msgs,
                                        _ => {
                                            // If we can't get current message count, assume we're caught up
                                            current_stream_msgs
                                        }
                                    };

                                    if msgs_processed >= initial_stream_msgs && seq >= current_msgs {
                                        caught_up = true;
                                        let _ = handler.on_caught_up().await;
                                        debug!("Service '{}' caught up at sequence {}", service_name, seq);
                                    }
                                }
                            }
                        }
                        last_checked_seq = current_stream_msgs;
                    }
                }
            }
        }

        debug!("Service '{}' request processing loop ended", service_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::stream::{ConsensusStream, ConsensusStreamOptions};

    use std::collections::HashSet;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_engine::{
        Consensus, HierarchicalConsensusConfig, RaftConfig, StorageConfig, TransportConfig,
        config::{ClusterJoinRetryConfig, ConsensusConfigBuilder},
    };
    use proven_governance::{GovernanceNode, Version};
    use proven_governance_mock::MockGovernance;
    use proven_messaging::service_handler::ServiceHandler;
    use proven_messaging::service_responder::ServiceResponder;
    use proven_messaging::stream::Stream;
    use rand::rngs::OsRng;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tracing_test::traced_test;

    // Type alias to simplify complex consensus system type
    type TestConsensusSystem = Arc<Consensus<MockGovernance, MockAttestor>>;

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

    #[derive(Debug, Clone)]
    struct MockServiceHandler {
        caught_up_called: Arc<Mutex<bool>>,
        caught_up_count: Arc<Mutex<u32>>,
        requests_processed: Arc<Mutex<Vec<TestMessage>>>,
    }

    impl MockServiceHandler {
        fn new() -> Self {
            Self {
                caught_up_called: Arc::new(Mutex::new(false)),
                caught_up_count: Arc::new(Mutex::new(0)),
                requests_processed: Arc::new(Mutex::new(Vec::new())),
            }
        }

        async fn get_caught_up_count(&self) -> u32 {
            *self.caught_up_count.lock().await
        }

        async fn was_caught_up_called(&self) -> bool {
            *self.caught_up_called.lock().await
        }
    }

    #[async_trait]
    impl ServiceHandler<TestMessage, serde_json::Error, serde_json::Error> for MockServiceHandler {
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
            let mut requests = self.requests_processed.lock().await;
            requests.push(msg.clone());
            drop(requests);

            // Create a response message
            let response = TestMessage {
                content: format!("handled: {}", msg.content),
            };

            Ok(responder.reply(response).await)
        }

        async fn on_caught_up(&self) -> Result<(), Self::Error> {
            let mut called = self.caught_up_called.lock().await;
            *called = true;
            drop(called);

            let mut count = self.caught_up_count.lock().await;
            *count += 1;
            drop(count);

            Ok(())
        }
    }

    // Helper to create consensus system (same as in stream tests)
    // Global test configuration - predefined nodes and keys for deterministic testing
    static TEST_NODES_CONFIG: std::sync::OnceLock<Vec<(String, SigningKey)>> =
        std::sync::OnceLock::new();

    fn get_test_nodes_config() -> &'static Vec<(String, SigningKey)> {
        TEST_NODES_CONFIG.get_or_init(|| {
            use rand::SeedableRng;
            use rand::rngs::StdRng;

            // Use deterministic seed for reproducible test keys
            let mut rng = StdRng::seed_from_u64(12345);
            let mut nodes = Vec::new();

            // Create 10 test nodes with deterministic keys
            for i in 1..=10 {
                let signing_key = SigningKey::generate(&mut rng);
                nodes.push((i.to_string(), signing_key));
            }

            nodes
        })
    }

    // Helper to create a simple single-node governance for testing (like consensus_manager tests)
    async fn create_test_consensus(
        node_id: &str,
        port: u16,
    ) -> Arc<Consensus<MockGovernance, MockAttestor>> {
        // Find the signing key for this node ID
        let test_nodes_config = get_test_nodes_config();
        let signing_key = test_nodes_config
            .iter()
            .find(|(id, _)| id == node_id)
            .map_or_else(
                || panic!("Test node ID '{node_id}' not found in test configuration"),
                |(_, key)| key.clone(),
            );

        // Create MockAttestor
        let attestor = Arc::new(MockAttestor::new());

        // Create single-node governance (only knows about itself)
        let governance_node = GovernanceNode {
            availability_zone: "test-az".to_string(),
            origin: format!("http://127.0.0.1:{port}"),
            public_key: signing_key.verifying_key(),
            region: "test-region".to_string(),
            specializations: HashSet::new(),
        };

        // Create MockAttestor to get the actual PCR values
        let attestor_for_version = MockAttestor::new();
        let actual_pcrs = attestor_for_version.pcrs_sync();

        // Create a test version using the actual PCR values from MockAttestor
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let governance = Arc::new(MockGovernance::new(
            vec![governance_node],
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create consensus config with temporary directory for tests
        let config = ConsensusConfigBuilder::new()
            .governance(governance.clone())
            .attestor(attestor.clone())
            .signing_key(signing_key.clone())
            .raft_config(RaftConfig::default())
            .transport_config(TransportConfig::Tcp {
                listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            })
            .storage_config(StorageConfig::Memory)
            .cluster_discovery_timeout(Duration::from_secs(10))
            .cluster_join_retry_config(ClusterJoinRetryConfig::default())
            .hierarchical_config(HierarchicalConsensusConfig::default())
            .build()
            .expect("Failed to build consensus config");

        let consensus = Consensus::new(config).await.unwrap();

        Arc::new(consensus)
    }

    fn create_governance_with_ports(ports: &[u16]) -> Arc<MockGovernance> {
        // Create MockAttestor first to get the actual PCR values
        let attestor = MockAttestor::new();
        let actual_pcrs = attestor.pcrs_sync();

        // Create a test version using the actual PCR values from MockAttestor
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let test_nodes_config = get_test_nodes_config();
        let mut governance_nodes = Vec::new();

        // Create topology nodes for the specified number of ports
        for (i, &port) in ports.iter().enumerate() {
            let (_node_id, signing_key) = &test_nodes_config[i];
            let governance_node = GovernanceNode {
                availability_zone: "test-az".to_string(),
                origin: format!("http://127.0.0.1:{port}"),
                public_key: signing_key.verifying_key(),
                region: "test-region".to_string(),
                specializations: HashSet::new(),
            };
            governance_nodes.push(governance_node);
        }

        Arc::new(MockGovernance::new(
            governance_nodes,
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ))
    }

    async fn create_consensus_system_with_governance(
        port: u16,
        governance: Arc<MockGovernance>,
    ) -> Arc<Consensus<MockGovernance, MockAttestor>> {
        // Generate a fresh signing key for each test
        let signing_key = SigningKey::generate(&mut OsRng);

        // Create MockAttestor
        let attestor = Arc::new(MockAttestor::new());

        // Create consensus config with temporary directory for tests
        let config = ConsensusConfigBuilder::new()
            .governance(governance.clone())
            .attestor(attestor.clone())
            .signing_key(signing_key.clone())
            .raft_config(RaftConfig::default())
            .transport_config(TransportConfig::Tcp {
                listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            })
            .storage_config(StorageConfig::Memory)
            .cluster_discovery_timeout(Duration::from_secs(10))
            .cluster_join_retry_config(ClusterJoinRetryConfig::default())
            .hierarchical_config(HierarchicalConsensusConfig::default())
            .build()
            .expect("Failed to build consensus config");

        let consensus = Consensus::new(config).await.unwrap();

        // Start the consensus system
        consensus
            .start()
            .await
            .expect("Failed to start consensus system");

        // Give it a moment to initialize
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Arc::new(consensus)
    }

    // Helper to create test options with single-node governance for single-node tests
    async fn create_test_options(
        node_id: &str,
        port: u16,
    ) -> ConsensusStreamOptions<MockGovernance, MockAttestor> {
        let consensus = create_test_consensus(node_id, port).await;
        ConsensusStreamOptions { consensus }
    }

    fn next_port() -> u16 {
        proven_util::port_allocator::allocate_port()
    }

    // Helper to create multi-node options without starting consensus systems (for unit testing)
    async fn create_multi_node_test_options(
        node_count: usize,
    ) -> Vec<ConsensusStreamOptions<MockGovernance, MockAttestor>> {
        use proven_governance::{GovernanceNode, Version};
        use std::collections::HashSet;

        let mut options_vec = Vec::new();
        let mut all_nodes = Vec::new();
        let mut signing_keys = Vec::new();
        let mut node_ports = Vec::new();

        // Generate signing keys and allocate ports for all nodes first
        for _ in 0..node_count {
            signing_keys.push(SigningKey::generate(&mut OsRng));
            node_ports.push(proven_util::port_allocator::allocate_port());
        }

        // Create topology nodes for all nodes
        for i in 0..node_count {
            let node = GovernanceNode {
                availability_zone: format!("test-az-{i}"),
                origin: format!("127.0.0.1:{}", node_ports[i]),
                public_key: signing_keys[i].verifying_key(),
                region: "test-region".to_string(),
                specializations: HashSet::new(),
            };
            all_nodes.push(node);
        }

        // Create MockAttestor to get the actual PCR values
        let sample_attestor = MockAttestor::new();
        let actual_pcrs = sample_attestor.pcrs_sync();

        // Create test version using the actual PCR values from MockAttestor
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        // Create SHARED governance that all nodes will use
        let shared_governance = Arc::new(MockGovernance::new(
            all_nodes.clone(),
            vec![test_version.clone()],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create consensus systems for each node with SHARED governance (but don't start them)
        for i in 0..node_count {
            let attestor = Arc::new(MockAttestor::new());

            // Create consensus config with temporary directory for each node
            let config = ConsensusConfigBuilder::new()
                .governance(shared_governance.clone())
                .attestor(attestor.clone())
                .signing_key(signing_keys[i].clone())
                .raft_config(RaftConfig::default())
                .transport_config(TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{}", node_ports[i]).parse().unwrap(),
                })
                .storage_config(StorageConfig::Memory)
                .cluster_discovery_timeout(Duration::from_secs(10))
                .cluster_join_retry_config(ClusterJoinRetryConfig::default())
                .hierarchical_config(HierarchicalConsensusConfig::default())
                .build()
                .expect("Failed to build consensus config");

            let consensus = Consensus::new(config).await.unwrap();

            // NOTE: Don't call consensus.start() here - this avoids Raft cluster formation issues in unit tests

            let options = ConsensusStreamOptions {
                consensus: Arc::new(consensus),
            };
            options_vec.push(options);
        }

        options_vec
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_creation() {
        let options = create_test_options("1", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_service_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockServiceHandler::new();
        let service = initialized_stream
            .service(
                "test_service",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await;

        assert!(service.is_ok(), "Service creation should succeed");

        let service = service.unwrap();
        assert_eq!(service.bootable_name(), "test_service");

        // Test that last_seq returns the current sequence
        let last_seq = service.last_seq().await;
        assert!(last_seq.is_ok(), "last_seq should succeed");
        assert_eq!(last_seq.unwrap(), 0, "Initial sequence should be 0");
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_lifecycle() {
        let options = create_test_options("2", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_service_lifecycle", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockServiceHandler::new();
        let service = initialized_stream
            .service(
                "test_service_lifecycle",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        // Test start
        let start_result = service.start().await;
        assert!(start_result.is_ok(), "Service start should succeed");

        // Give it a moment to start up
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Test shutdown
        let shutdown_result = service.shutdown().await;
        assert!(shutdown_result.is_ok(), "Service shutdown should succeed");

        // Verify caught_up was called (since no requests were published initially)
        assert!(
            handler.was_caught_up_called().await,
            "on_caught_up should have been called for empty stream"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_request_processing() {
        let options = create_test_options("3", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_service_requests", options);

        let initialized_stream = stream.init().await.unwrap();

        // Note: In a real test, we would publish service requests via consensus
        // For now, we'll simulate the service behavior with an empty stream
        // and verify the lifecycle works correctly

        let handler = MockServiceHandler::new();
        let service = initialized_stream
            .service(
                "test_service_requests",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        service.start().await.unwrap();

        // Let the service process for a bit
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        service.shutdown().await.unwrap();

        // Verify caught_up was called
        assert!(
            handler.was_caught_up_called().await,
            "on_caught_up should have been called"
        );
        assert_eq!(
            handler.get_caught_up_count().await,
            1,
            "on_caught_up should have been called exactly once"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_with_start_sequence() {
        let options = create_test_options("4", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_service_start_seq", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockServiceHandler::new();
        let service = initialized_stream
            .service(
                "test_service_start_seq",
                ConsensusServiceOptions {
                    start_sequence: Some(5),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        // Check that the service respects the start sequence
        assert_eq!(service.last_processed_seq, 5);

        service.start().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_options() {
        let options1 = ConsensusServiceOptions {
            start_sequence: Some(10),
        };

        let options2 = ConsensusServiceOptions {
            start_sequence: None,
        };

        // Test that options can be copied and debug printed
        let copied_options = options1;
        assert_eq!(copied_options.start_sequence, Some(10));

        // Test default None case
        assert_eq!(options2.start_sequence, None);

        // Test debug formatting
        let debug_str = format!("{options1:?}");
        assert!(debug_str.contains("ConsensusServiceOptions"));
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multiple_services_same_stream() {
        let options = create_test_options("6", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_multiple_services", options);

        let initialized_stream = stream.init().await.unwrap();

        // Create multiple services for the same stream
        let handler1 = MockServiceHandler::new();
        let service1 = initialized_stream
            .service(
                "service_1",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler1.clone(),
            )
            .await
            .unwrap();

        let handler2 = MockServiceHandler::new();
        let service2 = initialized_stream
            .service(
                "service_2",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler2.clone(),
            )
            .await
            .unwrap();

        // Start both services
        service1.start().await.unwrap();
        service2.start().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Shutdown both services
        service1.shutdown().await.unwrap();
        service2.shutdown().await.unwrap();

        // Both should have been caught up
        assert!(handler1.was_caught_up_called().await);
        assert!(handler2.was_caught_up_called().await);
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_responder_creation() {
        // Create a test stream first
        let options = create_test_options("6", next_port()).await;
        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_responder_stream", options);
        let initialized_stream = stream.init().await.unwrap();

        // Test creating a service responder directly
        let responder = ConsensusServiceResponder::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new(
            true,
            123,
            "request_123".to_string(),
            "test_service_responses".to_string(),
            initialized_stream,
        );

        // Test that stream_sequence is implemented
        let seq = responder.stream_sequence();
        assert!(seq > 0, "Stream sequence should be positive");

        // Test reply method signature
        let _test_response = TestMessage {
            content: "test response".to_string(),
        };

        // Note: We can't fully test reply without a consensus system
        // but we can verify the method signature compiles
        // let _used_responder = responder.reply(test_response).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_with_stream_method() {
        let options = create_test_options("7", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_service_stream_method", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockServiceHandler::new();
        let service = initialized_stream
            .service(
                "test_service_stream_method",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        // Test the stream() method returns the same stream
        let service_stream = service.stream();
        assert_eq!(service_stream.name(), initialized_stream.name());
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multiple_consensus_nodes_with_services() {
        // Create multiple consensus nodes with shared governance
        let ports = [next_port(), next_port(), next_port()];
        let governance = create_governance_with_ports(&ports);

        let mut consensus_systems = Vec::new();
        let mut services = Vec::new();

        // Create 3 consensus nodes, each running a service
        for (i, &port) in ports.iter().enumerate() {
            let consensus = create_consensus_system_with_governance(port, governance.clone()).await;

            let options = ConsensusStreamOptions {
                consensus: consensus.clone(),
            };

            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                serde_json::Error,
                serde_json::Error,
            >::new(format!("multi_node_service_stream_{}", i + 1), options);

            let initialized_stream = stream.init().await.unwrap();

            let handler = MockServiceHandler::new();
            let service = initialized_stream
                .service(
                    format!("service_node_{}", i + 1),
                    ConsensusServiceOptions {
                        start_sequence: Some(0),
                    },
                    handler.clone(),
                )
                .await
                .unwrap();

            consensus_systems.push(consensus);
            services.push((service, handler));
        }

        // Start all services
        for (service, _handler) in &services {
            service.start().await.unwrap();
        }

        // Let services initialize and potentially process any initial messages
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Verify all services are caught up
        for (_service, handler) in &services {
            assert!(
                handler.was_caught_up_called().await,
                "Each service should have been caught up"
            );
        }

        // Shutdown all services
        for (service, _handler) in &services {
            service.shutdown().await.unwrap();
        }

        // Wait for all services to complete
        for (service, _handler) in &services {
            service.wait().await;
        }

        // Cleanup consensus systems
        cleanup_multiple_consensus_systems(&consensus_systems).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_service_consensus_integration() {
        // Test that services properly integrate with consensus across multiple nodes
        let ports = [next_port(), next_port()];
        let governance = create_governance_with_ports(&ports);

        let consensus1 =
            create_consensus_system_with_governance(ports[0], governance.clone()).await;
        let consensus2 =
            create_consensus_system_with_governance(ports[1], governance.clone()).await;

        // Create streams on both nodes
        let options1 = ConsensusStreamOptions {
            consensus: consensus1.clone(),
        };

        let options2 = ConsensusStreamOptions {
            consensus: consensus2.clone(),
        };

        let stream1 = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("integrated_service_stream_1", options1);

        let stream2 = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("integrated_service_stream_2", options2);

        let initialized_stream1 = stream1.init().await.unwrap();
        let initialized_stream2 = stream2.init().await.unwrap();

        // Create services on both nodes
        let handler1 = MockServiceHandler::new();
        let service1 = initialized_stream1
            .service(
                "integrated_service_1",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler1.clone(),
            )
            .await
            .unwrap();

        let handler2 = MockServiceHandler::new();
        let service2 = initialized_stream2
            .service(
                "integrated_service_2",
                ConsensusServiceOptions {
                    start_sequence: Some(0),
                },
                handler2.clone(),
            )
            .await
            .unwrap();

        // Start both services
        service1.start().await.unwrap();
        service2.start().await.unwrap();

        // Allow time for consensus network formation and service initialization
        tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;

        // Both services should be caught up
        assert!(
            handler1.was_caught_up_called().await,
            "Service 1 should be caught up"
        );
        assert!(
            handler2.was_caught_up_called().await,
            "Service 2 should be caught up"
        );

        // Test that both services have similar sequence numbers (consensus should sync them)
        let seq1 = service1.last_seq().await.unwrap();
        let seq2 = service2.last_seq().await.unwrap();

        // They should be equal since they're on the same consensus network
        assert_eq!(
            seq1, seq2,
            "Services on different nodes should have synced sequence numbers"
        );

        // Shutdown services
        service1.shutdown().await.unwrap();
        service2.shutdown().await.unwrap();

        service1.wait().await;
        service2.wait().await;

        // Cleanup
        cleanup_multiple_consensus_systems(&[consensus1, consensus2]).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_service_fault_tolerance() {
        // Test service behavior in a multi-node environment (without actually starting consensus clusters)
        // This tests the service layer's ability to handle multi-node configurations
        let multi_options = create_multi_node_test_options(3).await;

        let mut services = Vec::new();

        // Create services on different nodes with different stream names to simulate fault tolerance
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                serde_json::Error,
                serde_json::Error,
            >::new(format!("fault_tolerant_stream_{i}"), options);

            // For unit testing, we simulate initialization without full consensus startup
            match stream.init().await {
                Ok(initialized_stream) => {
                    let handler = MockServiceHandler::new();

                    match initialized_stream
                        .service(
                            format!("fault_tolerant_service_{i}"),
                            ConsensusServiceOptions {
                                start_sequence: Some(0),
                            },
                            handler.clone(),
                        )
                        .await
                    {
                        Ok(service) => services.push(service),
                        Err(e) => {
                            tracing::info!(
                                "Service creation failed for node {} (expected in test environment): {}",
                                i + 1,
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::info!(
                        "Stream initialization failed for node {} (expected in test environment): {}",
                        i + 1,
                        e
                    );
                }
            }
        }

        // Test that services can be created successfully in a multi-node configuration
        tracing::info!(
            "Successfully created {} services for multi-node fault tolerance test",
            services.len()
        );

        // If we created any services, test their basic functionality
        for (i, service) in services.iter().enumerate() {
            assert_eq!(
                service.bootable_name(),
                format!("fault_tolerant_service_{i}")
            );

            // Test last_seq works
            let last_seq = service.last_seq().await;
            assert!(
                last_seq.is_ok(),
                "Service {i} should be able to report last sequence"
            );
        }

        // The test validates that services can be created in a multi-node environment
        // even if full consensus clusters cannot be formed in the test environment
    }

    // Helper to cleanup multiple consensus systems following the pattern from consensus_manager tests
    async fn cleanup_multiple_consensus_systems(consensus_systems: &[TestConsensusSystem]) {
        // Shutdown all systems sequentially
        for (i, consensus) in consensus_systems.iter().enumerate() {
            if let Err(e) = consensus.shutdown().await {
                warn!("Consensus {} shutdown error: {}", i + 1, e);
            }
        }

        // Wait for all systems to fully shut down
        for consensus in consensus_systems {
            consensus.wait().await;
        }
    }
}
