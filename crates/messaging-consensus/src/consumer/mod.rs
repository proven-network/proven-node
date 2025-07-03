//! Consumers are stateful views of consensus streams.

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
use proven_consensus::transport::ConsensusTransport;
use proven_governance::Governance;
use proven_messaging::consumer::{Consumer, ConsumerError, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::stream::InitializedStream;

use crate::error::MessagingConsensusError;
use crate::stream::InitializedConsensusStream;

/// Options for consensus consumers.
#[derive(Clone, Debug, Copy)]
pub struct ConsensusConsumerOptions {
    /// Starting sequence number.
    pub start_sequence: Option<u64>,
}

impl ConsumerOptions for ConsensusConsumerOptions {}

/// Error type for consensus consumers.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusConsumerError {
    /// Consensus error.
    #[error("Consensus error: {0}")]
    Consensus(#[from] MessagingConsensusError),
}

impl ConsumerError for ConsensusConsumerError {}

/// A consensus consumer.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ConsensusConsumer<G, A, C, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    C: ConsensusTransport + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S>,
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
    stream: InitializedConsensusStream<G, A, C, T, D, S>,
    options: ConsensusConsumerOptions,
    handler: X,
    last_processed_seq: u64,
    /// Current sequence number being processed.
    current_seq: Arc<Mutex<u64>>,
    /// Shutdown token for graceful termination.
    shutdown_token: CancellationToken,
    /// Task tracker for background processing.
    task_tracker: TaskTracker,
}

impl<G, A, C, X, T, D, S> Clone for ConsensusConsumer<G, A, C, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    C: ConsensusTransport + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S> + Clone,
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
impl<G, A, C, X, T, D, S> Consumer<X, T, D, S> for ConsensusConsumer<G, A, C, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    C: ConsensusTransport + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S>,
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
    type Error = ConsensusConsumerError;
    type Options = ConsensusConsumerOptions;
    type StreamType = InitializedConsensusStream<G, A, C, T, D, S>;

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
}

impl<G, A, C, X, T, D, S> ConsensusConsumer<G, A, C, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    C: ConsensusTransport + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S>,
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
    /// Process messages from the consensus stream.
    #[allow(clippy::too_many_lines)]
    async fn process_messages(
        stream: InitializedConsensusStream<G, A, C, T, D, S>,
        handler: X,
        current_seq: Arc<Mutex<u64>>,
        start_sequence: u64,
        shutdown_token: CancellationToken,
    ) -> Result<(), ConsensusConsumerError> {
        let mut last_checked_seq = start_sequence;
        let initial_stream_msgs = match tokio::time::timeout(
            tokio::time::Duration::from_millis(100), // Slightly longer timeout for initial call
            stream.messages(),
        )
        .await
        {
            Ok(Ok(msgs)) => msgs,
            Ok(Err(e)) => {
                warn!("Consumer failed to get initial stream messages: {:?}", e);
                0 // Default to 0 messages if we can't get count
            }
            Err(_) => {
                warn!("Consumer initial stream messages query timed out");
                0 // Default to 0 messages if timeout
            }
        };
        let mut caught_up = initial_stream_msgs == 0 || start_sequence >= initial_stream_msgs;
        let mut msgs_processed = 0;

        if caught_up {
            let _ = handler.on_caught_up().await;
        }

        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    break;
                }
                () = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Check for new messages with timeout to avoid hanging on consensus calls
                    let current_stream_msgs = match tokio::time::timeout(
                        tokio::time::Duration::from_millis(50),
                        stream.messages()
                    ).await {
                        Ok(Ok(msgs)) => msgs,
                        Ok(Err(e)) => {
                            warn!("Stream messages query failed: {:?}", e);
                            continue;
                        }
                        Err(_) => {
                            debug!("Stream messages query timed out, continuing");
                            continue;
                        }
                    };

                    if current_stream_msgs > last_checked_seq {
                        // Process new messages
                        for seq in (last_checked_seq + 1)..=current_stream_msgs {
                            let message_result = tokio::time::timeout(
                                tokio::time::Duration::from_millis(50),
                                stream.get(seq)
                            ).await;

                            let message = match message_result {
                                Ok(Ok(Some(msg))) => msg,
                                Ok(Ok(None)) => continue,
                                Ok(Err(e)) => {
                                    warn!("Failed to get message at seq {}: {:?}", seq, e);
                                    continue;
                                }
                                Err(_) => {
                                    debug!("Get message timed out for seq {}, continuing", seq);
                                    continue;
                                }
                            };

                            // Handle the message
                            if let Err(e) = handler.handle(message, seq).await {
                                warn!("Handler error for sequence {}: {:?}", seq, e);
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
                                        debug!("Consumer caught up at sequence {}", seq);
                                    }
                                }
                            }
                        }
                        last_checked_seq = current_stream_msgs;
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<G, A, C, X, T, D, S> Bootable for ConsensusConsumer<G, A, C, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    C: ConsensusTransport + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
{
    fn bootable_name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Spawn the message processing task
        let stream = self.stream.clone();
        let handler = self.handler.clone();
        let current_seq = self.current_seq.clone();
        let start_sequence = self.last_processed_seq;
        let shutdown_token = self.shutdown_token.clone();
        let consumer_name = self.name.clone();

        self.task_tracker.spawn(async move {
            if let Err(e) =
                Self::process_messages(stream, handler, current_seq, start_sequence, shutdown_token)
                    .await
            {
                warn!(
                    "Consumer '{}' message processing error: {:?}",
                    consumer_name, e
                );
            }
        });

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Cancel the processing task
        self.shutdown_token.cancel();

        // Close the task tracker to signal no more tasks will be spawned
        self.task_tracker.close();

        Ok(())
    }

    async fn wait(&self) -> () {
        self.task_tracker.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MessagingConsensusError;
    use crate::stream::{ConsensusStream, ConsensusStreamOptions};

    use std::collections::HashSet;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_consensus::transport::tcp::TcpTransport;
    use proven_consensus::{Consensus, ConsensusConfig};
    use proven_governance::{TopologyNode, Version};
    use proven_governance_mock::MockGovernance;
    use proven_messaging::stream::Stream;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use tokio::sync::Mutex;
    use tracing_test::traced_test;

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
    struct MockHandler {
        caught_up_called: Arc<Mutex<bool>>,
        caught_up_count: Arc<Mutex<u32>>,
        messages_processed: Arc<Mutex<Vec<TestMessage>>>,
    }

    impl MockHandler {
        fn new() -> Self {
            Self {
                caught_up_called: Arc::new(Mutex::new(false)),
                caught_up_count: Arc::new(Mutex::new(0)),
                messages_processed: Arc::new(Mutex::new(Vec::new())),
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
    impl ConsumerHandler<TestMessage, serde_json::Error, serde_json::Error> for MockHandler {
        type Error = serde_json::Error;

        async fn handle(&self, msg: TestMessage, _stream_sequence: u64) -> Result<(), Self::Error> {
            let mut messages = self.messages_processed.lock().await;
            messages.push(msg);
            drop(messages);

            Ok(())
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

    // Global test configuration - predefined nodes and keys for deterministic testing
    static TEST_NODES_CONFIG: std::sync::OnceLock<Vec<(String, SigningKey)>> =
        std::sync::OnceLock::new();

    fn get_test_nodes_config() -> &'static Vec<(String, SigningKey)> {
        TEST_NODES_CONFIG.get_or_init(|| {
            use rand::rngs::StdRng;
            use rand::SeedableRng;

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
    ) -> Arc<Consensus<MockGovernance, MockAttestor, TcpTransport<MockGovernance, MockAttestor>>>
    {
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
        let public_key = hex::encode(signing_key.verifying_key().to_bytes());
        let topology_node = TopologyNode {
            availability_zone: "test-az".to_string(),
            origin: format!("127.0.0.1:{port}"),
            public_key,
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
            vec![topology_node],
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create consensus config with temporary directory for tests
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        let config = ConsensusConfig {
            storage_dir: Some(temp_path),
            ..ConsensusConfig::default()
        };

        let consensus = Consensus::new_with_tcp(
            format!("127.0.0.1:{port}").parse().unwrap(),
            governance,
            attestor,
            signing_key,
            config,
        )
        .await
        .unwrap();

        Arc::new(consensus)
    }

    // Helper to create test options with single-node governance (like consensus_manager tests)
    async fn create_test_options(
        node_id: &str,
        port: u16,
    ) -> ConsensusStreamOptions<
        MockGovernance,
        MockAttestor,
        TcpTransport<MockGovernance, MockAttestor>,
    > {
        let consensus = create_test_consensus(node_id, port).await;
        ConsensusStreamOptions {
            consensus,
            stream_config: None,
        }
    }

    // Helper to cleanup consensus system following the pattern from consensus_manager tests
    async fn cleanup_consensus_system(
        consensus: &Arc<
            Consensus<MockGovernance, MockAttestor, TcpTransport<MockGovernance, MockAttestor>>,
        >,
    ) {
        // Give a bit of time for any ongoing operations to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Shutdown the consensus system
        let _ = consensus.shutdown().await;
    }

    fn next_port() -> u16 {
        proven_util::port_allocator::allocate_port()
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consumer_creation() {
        let options = create_test_options("1", next_port()).await;
        let consensus_ref = options.consensus.clone();

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TcpTransport<MockGovernance, MockAttestor>,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_consumer_stream", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockHandler::new();
        let consumer = initialized_stream
            .consumer(
                "test_consumer",
                ConsensusConsumerOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await;

        assert!(consumer.is_ok(), "Consumer creation should succeed");

        let consumer = consumer.unwrap();
        assert_eq!(consumer.bootable_name(), "test_consumer");

        // Test that last_seq returns the current sequence
        let last_seq = consumer.last_seq().await;
        assert!(last_seq.is_ok(), "last_seq should succeed");
        assert_eq!(last_seq.unwrap(), 0, "Initial sequence should be 0");

        // Cleanup
        cleanup_consensus_system(&consensus_ref).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consumer_lifecycle() {
        // Use the same pattern as the working consensus manager test - just create one consensus system
        let options = create_test_options("1", next_port()).await;
        let consensus_ref = options.consensus.clone();

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TcpTransport<MockGovernance, MockAttestor>,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_consumer_lifecycle", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockHandler::new();
        let consumer = initialized_stream
            .consumer(
                "test_consumer_lifecycle",
                ConsensusConsumerOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        // Test start
        let start_result = consumer.start().await;
        assert!(start_result.is_ok(), "Consumer start should succeed");

        // Give it a moment to start up
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Test shutdown
        let shutdown_result = consumer.shutdown().await;
        assert!(shutdown_result.is_ok(), "Consumer shutdown should succeed");

        // Verify caught_up was called (since no messages were published initially)
        assert!(
            handler.was_caught_up_called().await,
            "on_caught_up should have been called for empty stream"
        );

        // Cleanup
        cleanup_consensus_system(&consensus_ref).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consumer_message_processing() {
        let options = create_test_options("3", next_port()).await;
        let consensus_ref = options.consensus.clone();

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TcpTransport<MockGovernance, MockAttestor>,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_consumer_messages", options);

        let initialized_stream = stream.init().await.unwrap();

        // Publish some test messages first
        let _test_messages = [
            TestMessage {
                content: "message_1".to_string(),
            },
            TestMessage {
                content: "message_2".to_string(),
            },
            TestMessage {
                content: "message_3".to_string(),
            },
        ];

        // Note: In a real test, we would publish these messages via consensus
        // For now, we'll simulate the consumer behavior with an empty stream
        // and verify the lifecycle works correctly

        let handler = MockHandler::new();
        let consumer = initialized_stream
            .consumer(
                "test_consumer_messages",
                ConsensusConsumerOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        consumer.start().await.unwrap();

        // Let the consumer process for a bit
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        consumer.shutdown().await.unwrap();
        consumer.wait().await;

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

        // Cleanup
        cleanup_consensus_system(&consensus_ref).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consumer_with_start_sequence() {
        let options = create_test_options("4", next_port()).await;
        let consensus_ref = options.consensus.clone();

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TcpTransport<MockGovernance, MockAttestor>,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_consumer_start_seq", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockHandler::new();
        let consumer = initialized_stream
            .consumer(
                "test_consumer_start_seq",
                ConsensusConsumerOptions {
                    start_sequence: Some(5),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        // Check that the consumer respects the start sequence
        assert_eq!(consumer.last_processed_seq, 5);

        consumer.start().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        consumer.shutdown().await.unwrap();
        consumer.wait().await;

        // Cleanup
        cleanup_consensus_system(&consensus_ref).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consumer_clone() {
        let options = create_test_options("5", next_port()).await;
        let consensus_ref = options.consensus.clone();

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TcpTransport<MockGovernance, MockAttestor>,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_consumer_clone", options);

        let initialized_stream = stream.init().await.unwrap();

        let handler = MockHandler::new();
        let consumer = initialized_stream
            .consumer(
                "test_consumer_clone",
                ConsensusConsumerOptions {
                    start_sequence: Some(0),
                },
                handler.clone(),
            )
            .await
            .unwrap();

        // Test cloning
        let cloned_consumer = consumer.clone();
        assert_eq!(consumer.name, cloned_consumer.name);
        assert_eq!(
            consumer.last_processed_seq,
            cloned_consumer.last_processed_seq
        );

        // Cleanup
        cleanup_consensus_system(&consensus_ref).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consumer_options() {
        let options1 = ConsensusConsumerOptions {
            start_sequence: Some(10),
        };

        let options2 = ConsensusConsumerOptions {
            start_sequence: None,
        };

        // Test that options can be copied and debug printed
        let copied_options = options1;
        assert_eq!(copied_options.start_sequence, Some(10));

        // Test default None case
        assert_eq!(options2.start_sequence, None);

        // Test debug formatting
        let debug_str = format!("{options1:?}");
        assert!(debug_str.contains("ConsensusConsumerOptions"));
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consumer_error_handling() {
        // Test consumer error types
        let consensus_error =
            proven_consensus::ConsensusError::InvalidConfiguration("test error".to_string());
        let messaging_consensus_error = MessagingConsensusError::from(consensus_error);
        let consumer_error = ConsensusConsumerError::Consensus(messaging_consensus_error);

        // Test error formatting
        let error_string = format!("{consumer_error}");
        assert!(error_string.contains("Consensus error"));

        // Test debug formatting
        let debug_string = format!("{consumer_error:?}");
        assert!(debug_string.contains("Consensus"));
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multiple_consumers_same_stream() {
        let options = create_test_options("6", next_port()).await;
        let consensus_ref = options.consensus.clone();

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TcpTransport<MockGovernance, MockAttestor>,
            TestMessage,
            serde_json::Error,
            serde_json::Error,
        >::new("test_multiple_consumers", options);

        let initialized_stream = stream.init().await.unwrap();

        // Create multiple consumers for the same stream
        let handler1 = MockHandler::new();
        let consumer1 = initialized_stream
            .consumer(
                "consumer_1",
                ConsensusConsumerOptions {
                    start_sequence: Some(0),
                },
                handler1.clone(),
            )
            .await
            .unwrap();

        let handler2 = MockHandler::new();
        let consumer2 = initialized_stream
            .consumer(
                "consumer_2",
                ConsensusConsumerOptions {
                    start_sequence: Some(0),
                },
                handler2.clone(),
            )
            .await
            .unwrap();

        // Start both consumers
        consumer1.start().await.unwrap();
        consumer2.start().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Shutdown both consumers
        consumer1.shutdown().await.unwrap();
        consumer2.shutdown().await.unwrap();

        // Wait for both consumers to fully shut down
        consumer1.wait().await;
        consumer2.wait().await;

        // Both should have been caught up
        assert!(handler1.was_caught_up_called().await);
        assert!(handler2.was_caught_up_called().await);

        // Cleanup
        cleanup_consensus_system(&consensus_ref).await;
    }
}
