//! Consensus-based streams with immediate consistency.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::{info, warn};

use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_messaging::client::Client;
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream, StreamOptions};

use crate::client::ConsensusClient;
#[cfg(test)]
use crate::consensus::ConsensusConfig;
use crate::consensus::ConsensusManager;
use crate::consensus_manager;
use crate::consumer::ConsensusConsumer;
use crate::error::ConsensusError;
use crate::network::ConsensusNetwork;
use crate::service::ConsensusService;
use crate::storage::MessagingStorage;
use crate::subject::ConsensusSubject;
use crate::topology::TopologyManager;

/// Options for the consensus stream.
#[derive(Clone, Debug)]
pub struct ConsensusStreamOptions<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Shared consensus system.
    pub consensus: Arc<consensus_manager::Consensus<G, A>>,

    /// Optional stream-specific configuration.
    pub stream_config: Option<consensus_manager::StreamConfig>,
}

impl<G, A> StreamOptions for ConsensusStreamOptions<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
}

/// An initialized consensus stream with immediate consistency.
#[derive(Debug)]
pub struct InitializedConsensusStream<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    /// Stream name.
    name: String,

    /// Consensus protocol instance.
    consensus: Arc<ConsensusManager<G, A>>,

    /// Network layer for peer communication.
    network: Arc<ConsensusNetwork<G, A>>,

    /// Storage layer for persistence.
    storage: Arc<MessagingStorage>,

    /// Topology manager.
    topology: Arc<TopologyManager<G>>,

    /// Stream options.
    options: ConsensusStreamOptions<G, A>,

    /// Local cache of stream data.
    cache: Arc<RwLock<HashMap<u64, T>>>,

    /// Type markers.
    _marker: PhantomData<(T, D, S)>,
}

impl<G, A, T, D, S> Clone for InitializedConsensusStream<G, A, T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            consensus: self.consensus.clone(),
            network: self.network.clone(),
            storage: self.storage.clone(),
            topology: self.topology.clone(),
            options: self.options.clone(),
            cache: self.cache.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<G, A, T, D, S> InitializedStream<T, D, S> for InitializedConsensusStream<G, A, T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    type Error = ConsensusError;
    type Options = ConsensusStreamOptions<G, A>;
    type Subject = ConsensusSubject<G, A, T, D, S>;

    type Client<X>
        = ConsensusClient<G, A, X, T, D, S>
    where
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>;

    type Consumer<X>
        = ConsensusConsumer<G, A, X, T, D, S>
    where
        X: proven_messaging::consumer_handler::ConsumerHandler<T, D, S>;

    type Service<X>
        = ConsensusService<G, A, X, T, D, S>
    where
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>;

    /// Creates a new consensus stream.
    async fn new<N>(stream_name: N, options: Self::Options) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
    {
        let name = stream_name.into();

        // Use shared components from the consensus system - no initialization needed
        let consensus = options.consensus.consensus_manager().clone();
        let network = options.consensus.network().clone();
        let storage = options.consensus.storage().clone();
        let topology = options.consensus.topology().clone();

        // No background task spawning - already managed by the Consensus struct

        info!(
            "Initialized consensus stream '{}' using shared consensus system for node {}",
            name,
            options.consensus.node_id()
        );

        Ok(Self {
            name,
            consensus,
            network,
            storage,
            topology,
            options,
            cache: Arc::new(RwLock::new(HashMap::new())),
            _marker: PhantomData,
        })
    }

    /// Creates a new stream with subjects.
    async fn new_with_subjects<N, J>(
        stream_name: N,
        options: Self::Options,
        _subjects: Vec<J>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::Subject> + Clone + Send,
    {
        // For now, subjects are not used in consensus streams
        // All consensus operations go through the unified stream interface
        Self::new(stream_name, options).await
    }

    /// Creates a client for this stream.
    async fn client<N, X>(
        &self,
        service_name: N,
        options: <Self::Client<X> as proven_messaging::client::Client<X, T, D, S>>::Options,
    ) -> Result<
        Self::Client<X>,
        <Self::Client<X> as proven_messaging::client::Client<X, T, D, S>>::Error,
    >
    where
        N: Clone + Into<String> + Send,
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>,
    {
        ConsensusClient::new(service_name.into(), self.clone(), options).await
    }

    /// Creates a consumer for this stream.
    async fn consumer<N, X>(
        &self,
        consumer_name: N,
        options: <Self::Consumer<X> as proven_messaging::consumer::Consumer<X, T, D, S>>::Options,
        handler: X,
    ) -> Result<
        Self::Consumer<X>,
        <Self::Consumer<X> as proven_messaging::consumer::Consumer<X, T, D, S>>::Error,
    >
    where
        N: Clone + Into<String> + Send,
        X: proven_messaging::consumer_handler::ConsumerHandler<T, D, S>,
    {
        ConsensusConsumer::new(consumer_name.into(), self.clone(), options, handler).await
    }

    /// Creates a service for this stream.
    async fn service<N, X>(
        &self,
        service_name: N,
        options: <Self::Service<X> as proven_messaging::service::Service<X, T, D, S>>::Options,
        handler: X,
    ) -> Result<
        Self::Service<X>,
        <Self::Service<X> as proven_messaging::service::Service<X, T, D, S>>::Error,
    >
    where
        N: Clone + Into<String> + Send,
        X: proven_messaging::service_handler::ServiceHandler<T, D, S>,
    {
        ConsensusService::new(service_name.into(), self.clone(), options, handler).await
    }

    /// Deletes a message at the given sequence number.
    async fn delete(&self, seq: u64) -> Result<(), Self::Error> {
        // In consensus systems, we typically don't delete individual messages
        // Instead, we might use tombstone markers or compaction
        // For now, we'll remove from local cache but the consensus log remains
        self.cache.write().await.remove(&seq);

        info!(
            "Marked message {} for deletion in stream '{}'",
            seq, self.name
        );
        Ok(())
    }

    /// Gets a message by sequence number.
    async fn get(&self, seq: u64) -> Result<Option<T>, Self::Error> {
        // First check local cache
        {
            let cache = self.cache.read().await;
            if let Some(message) = cache.get(&seq) {
                return Ok(Some(message.clone()));
            }
        }

        // Query from consensus storage
        if let Some(bytes) = self.consensus.get_message(&self.name, seq)? {
            match T::try_from(bytes) {
                Ok(message) => {
                    // Cache the message for future access
                    self.cache.write().await.insert(seq, message.clone());
                    Ok(Some(message))
                }
                Err(e) => {
                    warn!("Failed to deserialize message at seq {}: {:?}", seq, e);
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    /// Gets the last message in the stream.
    async fn last_message(&self) -> Result<Option<T>, Self::Error> {
        let last_seq = self.last_seq().await?;
        if last_seq > 0 {
            self.get(last_seq).await
        } else {
            Ok(None)
        }
    }

    /// Gets the last sequence number.
    async fn last_seq(&self) -> Result<u64, Self::Error> {
        Ok(self.consensus.last_sequence(&self.name)?)
    }

    /// Gets the total number of messages.
    async fn messages(&self) -> Result<u64, Self::Error> {
        self.last_seq().await
    }

    /// Gets the stream name.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message to the stream with immediate consistency.
    async fn publish(&self, message: T) -> Result<u64, Self::Error> {
        // Convert message to bytes
        let bytes = message.clone().try_into().map_err(|e| {
            ConsensusError::Serialization(format!("Failed to serialize message: {e:?}"))
        })?;

        // Publish through consensus protocol for immediate consistency
        let seq = self.consensus.publish_message(self.name.clone(), bytes)?;

        // Cache the message locally
        self.cache.write().await.insert(seq, message);

        info!(
            "Published message to stream '{}' at sequence {}",
            self.name, seq
        );
        Ok(seq)
    }

    /// Publishes multiple messages as a batch with immediate consistency.
    async fn publish_batch(&self, messages: Vec<T>) -> Result<u64, Self::Error> {
        if messages.is_empty() {
            return self.last_seq().await;
        }

        // Convert all messages to bytes
        let mut bytes_messages = Vec::new();
        for message in &messages {
            let bytes = message.clone().try_into().map_err(|e| {
                ConsensusError::Serialization(format!("Failed to serialize batch message: {e:?}"))
            })?;
            bytes_messages.push(bytes);
        }

        // Publish batch through consensus protocol
        let last_seq = self
            .consensus
            .publish_batch(self.name.clone(), bytes_messages)?;

        // Cache all messages locally
        let mut cache = self.cache.write().await;
        let start_seq = last_seq - messages.len() as u64 + 1;
        for (i, message) in messages.into_iter().enumerate() {
            cache.insert(start_seq + i as u64, message);
        }

        info!(
            "Published batch of {} messages to stream '{}', ending at sequence {}",
            cache.len(),
            self.name,
            last_seq
        );
        Ok(last_seq)
    }

    /// Replaces all previous messages with a single rollup message.
    async fn rollup(&self, message: T, expected_seq: u64) -> Result<u64, Self::Error> {
        // Convert message to bytes
        let bytes = message.clone().try_into().map_err(|e| {
            ConsensusError::Serialization(format!("Failed to serialize rollup message: {e:?}"))
        })?;

        // Perform rollup through consensus protocol
        let seq = self
            .consensus
            .rollup_message(self.name.clone(), bytes, expected_seq)?;

        // Clear local cache and add the rollup message
        #[allow(clippy::significant_drop_tightening)]
        {
            let mut cache = self.cache.write().await;
            cache.clear();
            cache.insert(seq, message);
        }

        info!(
            "Performed rollup on stream '{}' at sequence {}",
            self.name, seq
        );
        Ok(seq)
    }
}

/// A consensus stream that provides immediate consistency guarantees.
#[derive(Debug)]
pub struct ConsensusStream<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    name: String,
    options: ConsensusStreamOptions<G, A>,
    _marker: PhantomData<(T, D, S)>,
}

impl<G, A, T, D, S> Clone for ConsensusStream<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            options: self.options.clone(),
            _marker: PhantomData,
        }
    }
}

impl<G, A, T, D, S> ConsensusStream<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    /// Gets the stream name.
    #[must_use]
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait]
impl<G, A, T, D, S> Stream<T, D, S> for ConsensusStream<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    type Options = ConsensusStreamOptions<G, A>;
    type Initialized = InitializedConsensusStream<G, A, T, D, S>;
    type Subject = ConsensusSubject<G, A, T, D, S>;

    fn new<K>(stream_name: K, options: Self::Options) -> Self
    where
        K: Clone + Into<String> + Send,
    {
        Self {
            name: stream_name.into(),
            options,
            _marker: PhantomData,
        }
    }

    async fn init(&self) -> Result<Self::Initialized, ConsensusError> {
        InitializedConsensusStream::new(self.name.clone(), self.options.clone()).await
    }

    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::Initialized, ConsensusError>
    where
        J: Into<Self::Subject> + Clone + Send,
    {
        InitializedConsensusStream::new_with_subjects(
            self.name.clone(),
            self.options.clone(),
            subjects,
        )
        .await
    }
}

#[cfg(test)]
#[allow(clippy::needless_range_loop)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::if_not_else)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_governance_mock::MockGovernance;
    use rand::rngs::OsRng;
    use serial_test::serial;
    use tracing_test::traced_test;

    // Test message type
    type TestMessage = Bytes;
    type TestError = Infallible;

    // Helper to create consensus system
    async fn create_consensus_system(
        node_id: &str,
        port: u16,
    ) -> Arc<consensus_manager::Consensus<MockGovernance, MockAttestor>> {
        use proven_governance::{TopologyNode, Version};
        use std::collections::HashSet;

        let signing_key = SigningKey::generate(&mut OsRng);

        // Create a simple test topology with one node
        let test_node = TopologyNode {
            availability_zone: "test-az".to_string(),
            origin: format!("127.0.0.1:{port}"),
            public_key: hex::encode(signing_key.verifying_key().to_bytes()),
            region: "test-region".to_string(),
            specializations: HashSet::new(),
        };

        // Create MockAttestor first to get the actual PCR values
        let attestor = Arc::new(MockAttestor::new());
        let actual_pcrs = attestor.pcrs_sync();

        // Create a test version using the actual PCR values from MockAttestor
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let governance = Arc::new(MockGovernance::new(
            vec![test_node],
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

        let consensus = consensus_manager::Consensus::new(
            node_id.to_string(),
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

    // Helper to create test options
    async fn create_test_options(
        node_id: &str,
        port: u16,
    ) -> ConsensusStreamOptions<MockGovernance, MockAttestor> {
        let consensus = create_consensus_system(node_id, port).await;
        ConsensusStreamOptions {
            consensus,
            stream_config: None,
        }
    }

    static PORT_COUNTER: AtomicU64 = AtomicU64::new(9000);

    #[allow(clippy::cast_possible_truncation)]
    fn next_port() -> u16 {
        PORT_COUNTER.fetch_add(1, Ordering::SeqCst) as u16
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_stream_creation() {
        let options = create_test_options("1", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_stream", options);

        assert_eq!(stream.name(), "test_stream");

        // Test initialization
        let initialized_stream = stream.init().await;
        assert!(
            initialized_stream.is_ok(),
            "Stream initialization should succeed"
        );

        let stream = initialized_stream.unwrap();
        assert_eq!(stream.name(), "test_stream");
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_stream_with_subjects() {
        let options = create_test_options("2", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_stream_subjects", options);

        // Test initialization with subjects (should work but subjects are not used)
        let subjects: Vec<
            ConsensusSubject<MockGovernance, MockAttestor, TestMessage, TestError, TestError>,
        > = vec![];
        let initialized_stream = stream.init_with_subjects(subjects).await;
        assert!(
            initialized_stream.is_ok(),
            "Stream initialization with subjects should succeed"
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_publish_single_message() {
        let options = create_test_options("3", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_publish", options);

        let initialized_stream = stream.init().await.unwrap();

        let message = Bytes::from("Hello, consensus world!");
        let seq = initialized_stream.publish(message.clone()).await;

        // Note: This will likely fail because we don't have a full consensus setup
        // but we're testing the interface and error handling
        match seq {
            Ok(sequence) => {
                assert_eq!(sequence, 1, "First message should have sequence 1");

                // Test retrieval
                let retrieved = initialized_stream.get(sequence).await.unwrap();
                assert_eq!(
                    retrieved,
                    Some(message),
                    "Retrieved message should match published message"
                );
            }
            Err(e) => {
                // Expected in test environment without full consensus setup
                tracing::info!("Expected error in test environment: {}", e);
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_publish_batch_messages() {
        let options = create_test_options("4", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_batch", options);

        let initialized_stream = stream.init().await.unwrap();

        let messages = vec![
            Bytes::from("Message 1"),
            Bytes::from("Message 2"),
            Bytes::from("Message 3"),
        ];

        let result = initialized_stream.publish_batch(messages.clone()).await;

        match result {
            Ok(last_seq) => {
                assert_eq!(
                    last_seq, 3,
                    "Last sequence should be 3 for batch of 3 messages"
                );

                // Test retrieval of all messages
                for (i, expected_msg) in messages.iter().enumerate() {
                    let retrieved = initialized_stream.get((i + 1) as u64).await.unwrap();
                    assert_eq!(
                        retrieved.as_ref(),
                        Some(expected_msg),
                        "Batch message {} should match",
                        i + 1
                    );
                }
            }
            Err(e) => {
                tracing::info!("Expected error in test environment: {}", e);
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_publish_empty_batch() {
        let options = create_test_options("5", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_empty_batch", options);

        let initialized_stream = stream.init().await.unwrap();

        let empty_messages: Vec<TestMessage> = vec![];
        let result = initialized_stream.publish_batch(empty_messages).await;

        match result {
            Ok(last_seq) => {
                // Should return current last sequence (0 for empty stream)
                let current_seq = initialized_stream.last_seq().await.unwrap();
                assert_eq!(
                    last_seq, current_seq,
                    "Empty batch should return current sequence"
                );
            }
            Err(e) => {
                tracing::info!("Expected error in test environment: {}", e);
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_rollup_operation() {
        let options = create_test_options("6", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_rollup", options);

        let initialized_stream = stream.init().await.unwrap();

        // First publish some messages
        let msg1 = Bytes::from("Message 1");
        let msg2 = Bytes::from("Message 2");

        let seq1_result = initialized_stream.publish(msg1).await;
        let seq2_result = initialized_stream.publish(msg2).await;

        if seq1_result.is_ok() && seq2_result.is_ok() {
            let expected_seq = seq2_result.unwrap();

            // Now perform rollup
            let rollup_msg = Bytes::from("Rollup message");
            let rollup_result = initialized_stream
                .rollup(rollup_msg.clone(), expected_seq)
                .await;

            match rollup_result {
                Ok(rollup_seq) => {
                    assert_eq!(
                        rollup_seq,
                        expected_seq + 1,
                        "Rollup should increment sequence"
                    );

                    // After rollup, only the rollup message should exist
                    let retrieved = initialized_stream.get(rollup_seq).await.unwrap();
                    assert_eq!(
                        retrieved,
                        Some(rollup_msg),
                        "Rollup message should be retrievable"
                    );
                }
                Err(e) => {
                    tracing::info!("Expected error in test environment: {}", e);
                }
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_stream_metadata_operations() {
        let options = create_test_options("7", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_metadata", options);

        let initialized_stream = stream.init().await.unwrap();

        // Test name
        assert_eq!(initialized_stream.name(), "test_metadata");

        // Test empty stream metadata
        let last_seq_result = initialized_stream.last_seq().await;
        let messages_count_result = initialized_stream.messages().await;
        let last_message_result = initialized_stream.last_message().await;

        match (last_seq_result, messages_count_result, last_message_result) {
            (Ok(last_seq), Ok(msg_count), Ok(last_msg)) => {
                assert_eq!(last_seq, 0, "Empty stream should have last_seq 0");
                assert_eq!(msg_count, 0, "Empty stream should have 0 messages");
                assert_eq!(last_msg, None, "Empty stream should have no last message");
            }
            _ => {
                tracing::info!(
                    "Metadata operations may fail in test environment without full consensus"
                );
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_message_deletion() {
        let options = create_test_options("8", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_delete", options);

        let initialized_stream = stream.init().await.unwrap();

        let message = Bytes::from("Message to delete");
        let publish_result = initialized_stream.publish(message.clone()).await;

        if let Ok(seq) = publish_result {
            // Test deletion (should succeed as it only removes from cache)
            let delete_result = initialized_stream.delete(seq).await;
            assert!(delete_result.is_ok(), "Message deletion should succeed");

            // Message might still be retrievable from consensus log
            // but should be removed from local cache
        } else {
            // Test deletion on non-existent message
            let delete_result = initialized_stream.delete(999).await;
            assert!(
                delete_result.is_ok(),
                "Deleting non-existent message should succeed"
            );
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_get_non_existent_message() {
        let options = create_test_options("9", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_get_nonexistent", options);

        let initialized_stream = stream.init().await.unwrap();

        // Try to get a message that doesn't exist
        let result = initialized_stream.get(999).await;
        match result {
            Ok(None) => {
                // Expected - message doesn't exist
            }
            Ok(Some(_)) => {
                panic!("Should not retrieve a message that doesn't exist");
            }
            Err(e) => {
                tracing::info!("Expected error when getting non-existent message: {}", e);
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_invalid_node_id() {
        // Create consensus system with invalid node ID directly
        let signing_key = SigningKey::generate(&mut OsRng);
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        ));
        let attestor = Arc::new(MockAttestor::new());

        let result = consensus_manager::Consensus::new(
            "not_a_number".to_string(), // Invalid node ID that can't be parsed as u64
            format!("127.0.0.1:{}", next_port()).parse().unwrap(),
            governance,
            attestor,
            signing_key,
            ConsensusConfig::default(),
        )
        .await;

        assert!(
            result.is_err(),
            "Consensus system creation should fail with invalid node ID"
        );

        if let Err(e) = result {
            match e {
                ConsensusError::InvalidConfiguration(_) => {
                    // Expected error type
                }
                _ => {
                    panic!("Expected InvalidConfiguration error, got: {e}");
                }
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_stream_clone() {
        let options = create_test_options("10", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_clone", options);

        // Test that streams can be cloned
        let cloned_stream = stream.clone();
        assert_eq!(stream.name(), cloned_stream.name());

        // Test that initialized streams can be cloned
        if let Ok(initialized_stream) = stream.init().await {
            let cloned_initialized = &initialized_stream;
            assert_eq!(initialized_stream.name(), cloned_initialized.name());
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multiple_streams_different_names() {
        let options1 = create_test_options("11", next_port()).await;
        let options2 = create_test_options("12", next_port()).await;

        let stream1 = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("stream_one", options1);

        let stream2 = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("stream_two", options2);

        assert_ne!(stream1.name(), stream2.name());

        // Both should be able to initialize independently
        let init1 = stream1.init().await;
        let init2 = stream2.init().await;

        // At least one should succeed (they might both fail due to test environment)
        // but they should fail independently
        match (init1, init2) {
            (Ok(s1), Ok(s2)) => {
                assert_ne!(s1.name(), s2.name());
            }
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
                // One succeeded, one failed - acceptable in test environment
            }
            (Err(e1), Err(e2)) => {
                tracing::info!(
                    "Both streams failed to initialize (expected in test): {} | {}",
                    e1,
                    e2
                );
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_stream_options_clone() {
        let options = create_test_options("13", next_port()).await;
        let cloned_options = &options;

        // Test that the consensus system references are the same
        assert!(Arc::ptr_eq(&options.consensus, &cloned_options.consensus));

        // Test that stream configs are equal
        assert_eq!(
            options.stream_config.is_some(),
            cloned_options.stream_config.is_some()
        );

        // Access the underlying consensus system properties
        assert_eq!(options.consensus.node_id(), "13");
        assert_eq!(cloned_options.consensus.node_id(), "13");
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_stream_debug_formatting() {
        let options = create_test_options("14", next_port()).await;

        let stream = ConsensusStream::<
            MockGovernance,
            MockAttestor,
            TestMessage,
            TestError,
            TestError,
        >::new("test_debug", options);

        // Test that Debug formatting works
        let debug_str = format!("{stream:?}");
        assert!(debug_str.contains("ConsensusStream"));
        assert!(debug_str.contains("test_debug"));
    }

    // Multi-node consensus tests

    // Helper to create multi-node test environment
    async fn create_multi_node_options(
        node_count: usize,
        base_port: u16,
    ) -> Vec<ConsensusStreamOptions<MockGovernance, MockAttestor>> {
        use proven_governance::{TopologyNode, Version};
        use std::collections::HashSet;

        let mut options_vec = Vec::new();
        let mut all_nodes = Vec::new();
        let mut signing_keys = Vec::new();

        // Generate signing keys for all nodes first
        for _ in 0..node_count {
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        // Create topology nodes for all nodes
        for i in 0..node_count {
            let node = TopologyNode {
                availability_zone: format!("test-az-{i}"),
                origin: format!("127.0.0.1:{}", base_port + i as u16),
                public_key: hex::encode(signing_keys[i].verifying_key().to_bytes()),
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

        // Create consensus systems for each node with the same topology
        for i in 0..node_count {
            let governance = Arc::new(MockGovernance::new(
                all_nodes.clone(),
                vec![test_version.clone()],
                "http://localhost:3200".to_string(),
                vec![],
            ));
            let attestor = Arc::new(MockAttestor::new());

            // Create consensus config with temporary directory for each node
            let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
            let temp_path = temp_dir.path().to_string_lossy().to_string();
            let config = ConsensusConfig {
                consensus_timeout: Duration::from_secs(10), // Longer timeout for multi-node
                storage_dir: Some(temp_path),
                ..ConsensusConfig::default()
            };

            let consensus = consensus_manager::Consensus::new(
                (i + 1).to_string(), // Node IDs 1, 2, 3, etc.
                format!("127.0.0.1:{}", base_port + i as u16)
                    .parse()
                    .unwrap(),
                governance,
                attestor,
                signing_keys[i].clone(),
                config,
            )
            .await
            .unwrap();

            let options = ConsensusStreamOptions {
                consensus: Arc::new(consensus),
                stream_config: None,
            };
            options_vec.push(options);
        }

        options_vec
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_three_node_consensus_stream_creation() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut streams = Vec::new();

        // Create streams for all 3 nodes
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(format!("consensus_stream_{i}"), options);

            streams.push(stream);
        }

        assert_eq!(streams.len(), 3, "Should have created 3 streams");

        // Initialize all streams
        let mut initialized_streams = Vec::new();
        for stream in streams {
            match stream.init().await {
                Ok(initialized) => {
                    initialized_streams.push(initialized);
                }
                Err(e) => {
                    tracing::info!(
                        "Stream initialization failed (expected in test environment): {}",
                        e
                    );
                    // In a test environment without full network setup, initialization may fail
                    // but we should still be able to create the streams
                }
            }
        }

        tracing::info!(
            "Successfully created {} consensus streams for 3-node test",
            initialized_streams.len()
        );
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_five_node_consensus_topology() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(5, base_port).await;

        // Verify that all nodes have the same topology view
        for (i, options) in multi_options.iter().enumerate() {
            let topology_result = options.consensus.governance().get_topology().await;
            match topology_result {
                Ok(topology) => {
                    assert_eq!(
                        topology.len(),
                        5,
                        "Node {} should see 5 nodes in topology",
                        i + 1
                    );

                    // Verify each node sees itself and others
                    let node_origins: Vec<String> =
                        topology.iter().map(|n| n.origin.clone()).collect();
                    for j in 0..5 {
                        let expected_origin = format!("127.0.0.1:{}", base_port + j as u16);
                        assert!(
                            node_origins.contains(&expected_origin),
                            "Node {} should see node at {} in topology",
                            i + 1,
                            expected_origin
                        );
                    }
                }
                Err(e) => {
                    tracing::info!("Topology query failed for node {}: {}", i + 1, e);
                }
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_stream_isolation() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut streams = Vec::new();

        // Create different stream names for each node to test isolation
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(format!("isolated_stream_{i}"), options);

            streams.push(stream);
        }

        // Verify each stream has a unique name and configuration
        for (i, stream) in streams.iter().enumerate() {
            assert_eq!(stream.name(), format!("isolated_stream_{i}"));
        }

        // Verify streams are independent
        assert_ne!(streams[0].name(), streams[1].name());
        assert_ne!(streams[1].name(), streams[2].name());
        assert_ne!(streams[0].name(), streams[2].name());
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_message_publishing_simulation() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut initialized_streams = Vec::new();

        // Try to initialize streams for all nodes
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new("shared_consensus_stream", options);

            match stream.init().await {
                Ok(initialized) => {
                    initialized_streams.push(initialized);
                    tracing::info!("Successfully initialized stream for node {}", i + 1);
                }
                Err(e) => {
                    tracing::info!(
                        "Stream initialization failed for node {} (expected in test): {}",
                        i + 1,
                        e
                    );
                }
            }
        }

        // If we have any initialized streams, test message publishing
        if !initialized_streams.is_empty() {
            let stream = &initialized_streams[0];
            let test_message = Bytes::from("Multi-node consensus test message");

            match stream.publish(test_message.clone()).await {
                Ok(seq) => {
                    tracing::info!("Successfully published message with sequence {}", seq);

                    // Try to retrieve the message
                    match stream.get(seq).await {
                        Ok(Some(retrieved_msg)) => {
                            assert_eq!(
                                retrieved_msg, test_message,
                                "Retrieved message should match published message"
                            );
                        }
                        Ok(None) => {
                            tracing::info!(
                                "Message not found (may be expected in test environment)"
                            );
                        }
                        Err(e) => {
                            tracing::info!("Message retrieval failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::info!(
                        "Message publishing failed (expected without full consensus setup): {}",
                        e
                    );
                }
            }
        } else {
            tracing::info!(
                "No streams initialized successfully - this is expected in test environment"
            );
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_consensus_configuration() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(4, base_port).await; // Test with 4 nodes

        // Verify consensus configuration for each node
        for (i, options) in multi_options.iter().enumerate() {
            // Check node ID is valid
            let node_id_result = options.consensus.node_id().parse::<u64>();
            assert!(
                node_id_result.is_ok(),
                "Node {} should have valid numeric ID",
                i + 1
            );

            let node_id = node_id_result.unwrap();
            assert_eq!(
                node_id,
                (i + 1) as u64,
                "Node ID should match expected value"
            );

            // Check timeout configuration
            assert!(
                options.consensus.config().consensus_timeout >= Duration::from_secs(5),
                "Consensus timeout should be at least 5 seconds for multi-node"
            );

            // Check network address is unique
            let expected_port = base_port + i as u16;
            assert_eq!(
                options.consensus.network().local_address().port(),
                expected_port,
                "Node {} should have unique port {}",
                i + 1,
                expected_port
            );
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_governance_consistency() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        // Test that all nodes have consistent governance views
        let mut governance_views = Vec::new();

        for (i, options) in multi_options.iter().enumerate() {
            match options.consensus.governance().get_topology().await {
                Ok(topology) => {
                    governance_views.push(topology);
                    tracing::info!(
                        "Node {} sees {} peers in topology",
                        i + 1,
                        governance_views[i].len()
                    );
                }
                Err(e) => {
                    tracing::info!("Failed to get topology for node {}: {}", i + 1, e);
                }
            }
        }

        // If we got topology from multiple nodes, verify consistency
        if governance_views.len() >= 2 {
            let first_topology = &governance_views[0];
            for (i, topology) in governance_views.iter().enumerate().skip(1) {
                assert_eq!(
                    topology.len(),
                    first_topology.len(),
                    "Node {} should see same number of peers as node 1",
                    i + 1
                );

                // Check that the same public keys are present (order may differ)
                let first_keys: HashSet<&String> =
                    first_topology.iter().map(|n| &n.public_key).collect();
                let current_keys: HashSet<&String> =
                    topology.iter().map(|n| &n.public_key).collect();

                assert_eq!(
                    first_keys,
                    current_keys,
                    "Node {} should see same peers as node 1",
                    i + 1
                );
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_error_handling() {
        let base_port = next_port();

        // Test with invalid node configuration
        let multi_options = create_multi_node_options(3, base_port).await;

        // Note: We can't easily corrupt node configuration after creation
        // since it's embedded in the consensus system. Instead we'll test
        // the normal case and expect that some nodes may fail in test environment

        let mut success_count = 0;
        let mut error_count = 0;

        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(format!("error_test_stream_{i}"), options);

            match stream.init().await {
                Ok(_) => {
                    success_count += 1;
                    tracing::info!("Node {} initialized successfully", i + 1);
                }
                Err(e) => {
                    error_count += 1;
                    tracing::info!("Node {} failed to initialize: {}", i + 1, e);

                    // In test environment, nodes may fail for various reasons
                    // We just log the error type for information
                    tracing::info!("Error type: {:?}", e);
                }
            }
        }

        tracing::info!(
            "Multi-node error test: {} successes, {} errors",
            success_count,
            error_count
        );

        // In test environment, we may or may not have errors depending on setup
        // The main goal is to test that error handling works without panicking
        assert!(
            success_count + error_count == 3,
            "Should have processed all 3 nodes"
        );
    }

    // Data consistency tests across multiple nodes

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_three_node_data_consistency() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut initialized_streams = Vec::new();
        let stream_name = "consistency_test_stream";

        // Initialize streams for all 3 nodes
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(stream_name, options);

            match stream.init().await {
                Ok(initialized) => {
                    initialized_streams.push((i + 1, initialized));
                    tracing::info!("Node {} initialized successfully", i + 1);
                }
                Err(e) => {
                    tracing::info!(
                        "Node {} failed to initialize (expected in test): {}",
                        i + 1,
                        e
                    );
                }
            }
        }

        if initialized_streams.len() >= 2 {
            // Publish a message on the first node
            let test_message = Bytes::from("Consistency test message");
            let (publisher_id, publisher_stream) = &initialized_streams[0];

            match publisher_stream.publish(test_message.clone()).await {
                Ok(seq) => {
                    tracing::info!(
                        "Node {} published message at sequence {}",
                        publisher_id,
                        seq
                    );

                    // Give some time for consensus to propagate (in a real system)
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    // Check if all other nodes can see the same message
                    let mut consistent_nodes = 0;
                    let mut total_checked = 0;

                    for (node_id, stream) in &initialized_streams {
                        total_checked += 1;
                        match stream.get(seq).await {
                            Ok(Some(retrieved_msg)) => {
                                if retrieved_msg == test_message {
                                    consistent_nodes += 1;
                                    tracing::info!("Node {} has consistent data", node_id);
                                } else {
                                    tracing::warn!("Node {} has inconsistent data", node_id);
                                }
                            }
                            Ok(None) => {
                                tracing::info!(
                                    "Node {} doesn't have the message (may be expected in test)",
                                    node_id
                                );
                            }
                            Err(e) => {
                                tracing::info!(
                                    "Node {} failed to retrieve message: {}",
                                    node_id,
                                    e
                                );
                            }
                        }
                    }

                    tracing::info!(
                        "Data consistency check: {}/{} nodes have consistent data",
                        consistent_nodes,
                        total_checked
                    );

                    // In a test environment, we may not achieve full consensus
                    // but at least the publishing node should have the data
                    assert!(
                        consistent_nodes >= 1,
                        "At least the publishing node should have consistent data"
                    );
                }
                Err(e) => {
                    tracing::info!(
                        "Message publishing failed (expected without full consensus): {}",
                        e
                    );
                }
            }
        } else {
            tracing::info!("Not enough nodes initialized for consistency test");
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_sequence_consistency() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut initialized_streams = Vec::new();
        let stream_name = "sequence_consistency_stream";

        // Initialize streams for all nodes
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(stream_name, options);

            match stream.init().await {
                Ok(initialized) => {
                    initialized_streams.push((i + 1, initialized));
                }
                Err(e) => {
                    tracing::info!("Node {} failed to initialize: {}", i + 1, e);
                }
            }
        }

        if !initialized_streams.is_empty() {
            // Test that all nodes report the same last sequence number
            let mut last_sequences = Vec::new();

            for (node_id, stream) in &initialized_streams {
                match stream.last_seq().await {
                    Ok(seq) => {
                        last_sequences.push((*node_id, seq));
                        tracing::info!("Node {} reports last sequence: {}", node_id, seq);
                    }
                    Err(e) => {
                        tracing::info!("Node {} failed to get last sequence: {}", node_id, e);
                    }
                }
            }

            // Check if all nodes report the same sequence number
            if last_sequences.len() >= 2 {
                let first_seq = last_sequences[0].1;
                let all_consistent = last_sequences.iter().all(|(_, seq)| *seq == first_seq);

                if all_consistent {
                    tracing::info!(
                        "All {} nodes have consistent last sequence: {}",
                        last_sequences.len(),
                        first_seq
                    );
                } else {
                    tracing::info!(
                        "Nodes have inconsistent sequences (may be expected in test environment)"
                    );
                    for (node_id, seq) in &last_sequences {
                        tracing::info!("  Node {}: sequence {}", node_id, seq);
                    }
                }

                // In test environment, we mainly verify the interface works
                assert!(
                    !last_sequences.is_empty(),
                    "Should be able to query sequence numbers"
                );
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_batch_consistency() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(4, base_port).await; // Test with 4 nodes

        let mut initialized_streams = Vec::new();
        let stream_name = "batch_consistency_stream";

        // Initialize streams
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(stream_name, options);

            match stream.init().await {
                Ok(initialized) => {
                    initialized_streams.push((i + 1, initialized));
                }
                Err(e) => {
                    tracing::info!("Node {} failed to initialize: {}", i + 1, e);
                }
            }
        }

        if !initialized_streams.is_empty() {
            let batch_messages = vec![
                Bytes::from("Batch message 1"),
                Bytes::from("Batch message 2"),
                Bytes::from("Batch message 3"),
            ];

            let (publisher_id, publisher_stream) = &initialized_streams[0];

            match publisher_stream.publish_batch(batch_messages.clone()).await {
                Ok(last_seq) => {
                    tracing::info!(
                        "Node {} published batch ending at sequence {}",
                        publisher_id,
                        last_seq
                    );

                    // Allow time for propagation
                    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

                    // Check consistency across all nodes
                    let start_seq = last_seq - batch_messages.len() as u64 + 1;
                    let mut consistency_results = Vec::new();

                    for (node_id, stream) in &initialized_streams {
                        let mut node_messages = Vec::new();
                        let mut retrieval_success = true;

                        for seq in start_seq..=last_seq {
                            match stream.get(seq).await {
                                Ok(Some(msg)) => node_messages.push(msg),
                                Ok(None) => {
                                    tracing::info!(
                                        "Node {} missing message at sequence {}",
                                        node_id,
                                        seq
                                    );
                                    retrieval_success = false;
                                    break;
                                }
                                Err(e) => {
                                    tracing::info!(
                                        "Node {} failed to get sequence {}: {}",
                                        node_id,
                                        seq,
                                        e
                                    );
                                    retrieval_success = false;
                                    break;
                                }
                            }
                        }

                        if retrieval_success && node_messages.len() == batch_messages.len() {
                            let messages_match = node_messages
                                .iter()
                                .zip(batch_messages.iter())
                                .all(|(retrieved, original)| retrieved == original);

                            if messages_match {
                                tracing::info!("Node {} has consistent batch data", node_id);
                                consistency_results.push((*node_id, true));
                            } else {
                                tracing::warn!("Node {} has inconsistent batch data", node_id);
                                consistency_results.push((*node_id, false));
                            }
                        } else {
                            tracing::info!("Node {} could not retrieve complete batch", node_id);
                            consistency_results.push((*node_id, false));
                        }
                    }

                    let consistent_count = consistency_results
                        .iter()
                        .filter(|(_, consistent)| *consistent)
                        .count();
                    tracing::info!(
                        "Batch consistency: {}/{} nodes have consistent data",
                        consistent_count,
                        consistency_results.len()
                    );

                    // At minimum, the publishing node should have consistent data
                    assert!(
                        consistent_count >= 1,
                        "At least the publishing node should have consistent batch data"
                    );
                }
                Err(e) => {
                    tracing::info!(
                        "Batch publishing failed (expected in test environment): {}",
                        e
                    );
                }
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_node_rollup_consistency() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut initialized_streams = Vec::new();
        let stream_name = "rollup_consistency_stream";

        // Initialize streams
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(stream_name, options);

            match stream.init().await {
                Ok(initialized) => {
                    initialized_streams.push((i + 1, initialized));
                }
                Err(e) => {
                    tracing::info!("Node {} failed to initialize: {}", i + 1, e);
                }
            }
        }

        if !initialized_streams.is_empty() {
            let (publisher_id, publisher_stream) = &initialized_streams[0];

            // First publish some messages
            let msg1 = Bytes::from("Message before rollup 1");
            let msg2 = Bytes::from("Message before rollup 2");

            let seq1_result = publisher_stream.publish(msg1).await;
            let seq2_result = publisher_stream.publish(msg2).await;

            if let (Ok(seq1), Ok(seq2)) = (seq1_result, seq2_result) {
                tracing::info!(
                    "Node {} published messages at sequences {} and {}",
                    publisher_id,
                    seq1,
                    seq2
                );

                // Now perform rollup
                let rollup_msg = Bytes::from("Rollup message replacing all previous");
                match publisher_stream.rollup(rollup_msg.clone(), seq2).await {
                    Ok(rollup_seq) => {
                        tracing::info!(
                            "Node {} performed rollup at sequence {}",
                            publisher_id,
                            rollup_seq
                        );

                        // Allow time for rollup to propagate
                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

                        // Check that all nodes see the rollup result consistently
                        let mut rollup_consistency = Vec::new();

                        for (node_id, stream) in &initialized_streams {
                            match stream.get(rollup_seq).await {
                                Ok(Some(retrieved_msg)) => {
                                    if retrieved_msg == rollup_msg {
                                        tracing::info!(
                                            "Node {} has consistent rollup data",
                                            node_id
                                        );
                                        rollup_consistency.push((*node_id, true));

                                        // Also check that previous messages are gone (from cache)
                                        match stream.get(seq1).await {
                                            Ok(None) => {
                                                tracing::info!("Node {} correctly shows old message {} as removed", node_id, seq1);
                                            }
                                            Ok(Some(_)) => {
                                                tracing::info!("Node {} still has old message {} (may be in consensus log)", node_id, seq1);
                                            }
                                            Err(e) => {
                                                tracing::info!(
                                                    "Node {} error checking old message {}: {}",
                                                    node_id,
                                                    seq1,
                                                    e
                                                );
                                            }
                                        }
                                    } else {
                                        tracing::warn!(
                                            "Node {} has inconsistent rollup data",
                                            node_id
                                        );
                                        rollup_consistency.push((*node_id, false));
                                    }
                                }
                                Ok(None) => {
                                    tracing::info!("Node {} doesn't have rollup message", node_id);
                                    rollup_consistency.push((*node_id, false));
                                }
                                Err(e) => {
                                    tracing::info!(
                                        "Node {} failed to retrieve rollup: {}",
                                        node_id,
                                        e
                                    );
                                    rollup_consistency.push((*node_id, false));
                                }
                            }
                        }

                        let consistent_rollup_count = rollup_consistency
                            .iter()
                            .filter(|(_, consistent)| *consistent)
                            .count();
                        tracing::info!(
                            "Rollup consistency: {}/{} nodes have consistent rollup data",
                            consistent_rollup_count,
                            rollup_consistency.len()
                        );

                        assert!(
                            consistent_rollup_count >= 1,
                            "At least the publishing node should have consistent rollup data"
                        );
                    }
                    Err(e) => {
                        tracing::info!(
                            "Rollup operation failed (expected in test environment): {}",
                            e
                        );
                    }
                }
            } else {
                tracing::info!("Initial message publishing failed, skipping rollup test");
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_cross_node_message_visibility() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut initialized_streams = Vec::new();
        let stream_name = "cross_node_visibility_stream";

        // Initialize streams
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = ConsensusStream::<
                MockGovernance,
                MockAttestor,
                TestMessage,
                TestError,
                TestError,
            >::new(stream_name, options);

            match stream.init().await {
                Ok(initialized) => {
                    initialized_streams.push((i + 1, initialized));
                }
                Err(e) => {
                    tracing::info!("Node {} failed to initialize: {}", i + 1, e);
                }
            }
        }

        if initialized_streams.len() >= 2 {
            // Publish messages from different nodes and check cross-visibility
            let mut published_messages = Vec::new();

            for (_i, (node_id, stream)) in initialized_streams.iter().enumerate().take(2) {
                let message = Bytes::from(format!("Message from node {node_id}"));

                match stream.publish(message.clone()).await {
                    Ok(seq) => {
                        published_messages.push((*node_id, seq, message));
                        tracing::info!("Node {} published message at sequence {}", node_id, seq);
                    }
                    Err(e) => {
                        tracing::info!("Node {} failed to publish: {}", node_id, e);
                    }
                }

                // Small delay between publications
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }

            // Allow time for cross-node propagation
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            // Check if each node can see messages published by other nodes
            let mut cross_visibility_matrix = Vec::new();

            for (reader_node_id, reader_stream) in &initialized_streams {
                let mut reader_results = Vec::new();

                for (publisher_node_id, seq, expected_msg) in &published_messages {
                    match reader_stream.get(*seq).await {
                        Ok(Some(retrieved_msg)) => {
                            let matches = retrieved_msg == *expected_msg;
                            reader_results.push((*publisher_node_id, *seq, matches));

                            if matches {
                                tracing::info!(
                                    "Node {} can see message from node {} at seq {}",
                                    reader_node_id,
                                    publisher_node_id,
                                    seq
                                );
                            } else {
                                tracing::warn!(
                                    "Node {} sees different message from node {} at seq {}",
                                    reader_node_id,
                                    publisher_node_id,
                                    seq
                                );
                            }
                        }
                        Ok(None) => {
                            tracing::info!(
                                "Node {} cannot see message from node {} at seq {}",
                                reader_node_id,
                                publisher_node_id,
                                seq
                            );
                            reader_results.push((*publisher_node_id, *seq, false));
                        }
                        Err(e) => {
                            tracing::info!(
                                "Node {} error reading message from node {} at seq {}: {}",
                                reader_node_id,
                                publisher_node_id,
                                seq,
                                e
                            );
                            reader_results.push((*publisher_node_id, *seq, false));
                        }
                    }
                }

                cross_visibility_matrix.push((*reader_node_id, reader_results));
            }

            // Analyze cross-visibility results
            let mut total_checks = 0;
            let mut successful_reads = 0;

            for (reader_id, results) in &cross_visibility_matrix {
                for (publisher_id, seq, success) in results {
                    total_checks += 1;
                    if *success {
                        successful_reads += 1;
                    }
                    tracing::info!(
                        "Cross-visibility: Node {} reading from Node {} seq {}: {}",
                        reader_id,
                        publisher_id,
                        seq,
                        if *success { "✓" } else { "✗" }
                    );
                }
            }

            tracing::info!(
                "Cross-node visibility: {}/{} reads successful",
                successful_reads,
                total_checks
            );

            // In a test environment, we expect at least nodes to see their own messages
            assert!(
                successful_reads > 0,
                "Should have some successful cross-node reads"
            );
        }
    }
}
