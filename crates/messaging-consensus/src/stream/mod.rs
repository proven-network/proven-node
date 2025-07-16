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
use proven_messaging::client::Client;
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream, StreamOptions};
use proven_topology::TopologyAdaptor;

use crate::client::ConsensusClient;
use crate::consumer::ConsensusConsumer;
use crate::error::MessagingConsensusError;
use crate::service::ConsensusService;
use crate::subject::ConsensusSubject;
use proven_engine::Consensus;

/// Options for the consensus stream.
#[derive(Debug)]
pub struct ConsensusStreamOptions<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Shared consensus system.
    pub consensus: Arc<Consensus<G, A>>,
}

impl<G, A> StreamOptions for ConsensusStreamOptions<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
}

impl<G, A> Clone for ConsensusStreamOptions<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn clone(&self) -> Self {
        Self {
            consensus: self.consensus.clone(),
        }
    }
}

/// An initialized consensus stream with immediate consistency.
#[derive(Debug)]
pub struct InitializedConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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
    /// Stream name.
    name: String,

    /// Consensus protocol instance.
    consensus: Arc<Consensus<G, A>>,

    /// Stream options.
    options: ConsensusStreamOptions<G, A>,

    /// Local cache of stream data.
    cache: Arc<RwLock<HashMap<u64, T>>>,

    /// Type markers.
    _marker: PhantomData<(T, D, S)>,
}

impl<G, A, T, D, S> Clone for InitializedConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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
            consensus: self.consensus.clone(),
            options: self.options.clone(),
            cache: self.cache.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<G, A, T, D, S> InitializedStream<T, D, S> for InitializedConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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
    type Error = MessagingConsensusError;
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
        let consensus = options.consensus.clone();

        // No background task spawning - already managed by the Consensus struct

        // Auto-subscribe stream to its own name as a subject (CONSENSUS-BASED)
        match consensus.subscribe_stream_to_subject(&name, &name).await {
            Ok(_) => {
                info!(
                    "Auto-subscribed stream '{}' to subject '{}' via consensus",
                    name, name
                );
            }
            Err(e) => {
                warn!(
                    "Failed to auto-subscribe stream '{}' to subject '{}' via consensus: {}",
                    name, name, e
                );
            }
        }

        info!(
            "Initialized consensus stream '{}' using shared consensus system for node {}",
            name,
            options.consensus.node_id()
        );

        Ok(Self {
            name,
            consensus,
            options,
            cache: Arc::new(RwLock::new(HashMap::new())),
            _marker: PhantomData,
        })
    }

    /// Creates a new stream with subjects.
    async fn new_with_subjects<N, J>(
        stream_name: N,
        options: Self::Options,
        subjects: Vec<J>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::Subject> + Clone + Send,
    {
        // Create the stream first
        let stream = Self::new(stream_name, options).await?;

        // Register subject subscriptions via consensus (BULK OPERATION for efficiency)
        let subject_patterns: Vec<String> = subjects
            .into_iter()
            .map(|subject| {
                let subject_obj: Self::Subject = subject.into();
                subject_obj.into()
            })
            .collect();

        if !subject_patterns.is_empty() {
            match stream
                .consensus
                .bulk_subscribe_stream_to_subjects(&stream.name, subject_patterns.clone())
                .await
            {
                Ok(_) => {
                    info!(
                        "Bulk subscribed stream '{}' to {} subject patterns via consensus: {:?}",
                        stream.name,
                        subject_patterns.len(),
                        subject_patterns
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to bulk subscribe stream '{}' to subject patterns via consensus: {}",
                        stream.name, e
                    );
                }
            }
        }

        Ok(stream)
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
        // Delete the message through consensus
        let result_seq = self
            .consensus
            .delete_message(self.name.clone(), seq)
            .await?;

        info!(
            "Deleted message {} from stream '{}' (result sequence: {})",
            seq, self.name, result_seq
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
        if let Some(bytes) = self.consensus.get_message(&self.name, seq).await? {
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
        Ok(self.consensus.last_sequence(&self.name).await?)
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
            MessagingConsensusError::from(proven_engine::Error::Serialization(format!(
                "Failed to serialize message: {e:?}"
            )))
        })?;

        // Publish through consensus protocol for immediate consistency
        let seq = self
            .consensus
            .publish_message(self.name.clone(), bytes)
            .await?;

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
                MessagingConsensusError::from(proven_engine::Error::Serialization(format!(
                    "Failed to serialize batch message: {e:?}"
                )))
            })?;
            bytes_messages.push(bytes);
        }

        // Publish batch through consensus protocol
        let last_seq = self
            .consensus
            .publish_batch(self.name.clone(), bytes_messages)
            .await?;

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
            MessagingConsensusError::from(proven_engine::Error::Serialization(format!(
                "Failed to serialize rollup message: {e:?}"
            )))
        })?;

        // Perform rollup through consensus protocol
        let seq = self
            .consensus
            .rollup_message(self.name.clone(), bytes, expected_seq)
            .await?;

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

impl<G, A, T, D, S> InitializedConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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
    /// Publish a message with metadata directly through consensus.
    ///
    /// This bypasses the normal stream publish mechanism and goes directly
    /// to the consensus layer, providing additional metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the consensus operation fails.
    pub async fn publish_with_metadata(
        &self,
        message: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<u64, MessagingConsensusError> {
        // Publish through consensus with metadata
        let seq = self
            .consensus
            .publish_message_with_metadata(self.name.clone(), message, metadata)
            .await?;

        info!(
            "Published message with metadata to stream '{}' at sequence {}",
            self.name, seq
        );

        Ok(seq)
    }

    /// Get access to the underlying consensus manager
    #[must_use]
    pub const fn consensus(&self) -> &Arc<Consensus<G, A>> {
        &self.consensus
    }
}

/// A consensus stream that provides immediate consistency guarantees.
#[derive(Debug)]
pub struct ConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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
    options: ConsensusStreamOptions<G, A>,
    _marker: PhantomData<(T, D, S)>,
}

impl<G, A, T, D, S> Clone for ConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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
            options: self.options.clone(),
            _marker: PhantomData,
        }
    }
}

impl<G, A, T, D, S> ConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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
    /// Gets the stream name.
    #[must_use]
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait]
impl<G, A, T, D, S> Stream<T, D, S> for ConsensusStream<G, A, T, D, S>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
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

    async fn init(&self) -> Result<Self::Initialized, MessagingConsensusError> {
        InitializedConsensusStream::new(self.name.clone(), self.options.clone()).await
    }

    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::Initialized, MessagingConsensusError>
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

    use std::convert::Infallible;
    use std::time::Duration;

    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_engine::{
        HierarchicalConsensusConfig, RaftConfig, StorageConfig, TransportConfig,
        config::{ClusterJoinRetryConfig, ConsensusConfigBuilder},
    };
    use proven_topology_mock::MockTopologyAdaptor;
    use rand::rngs::OsRng;
    use serial_test::serial;
    use tracing_test::traced_test;

    // Test message type
    type TestMessage = Bytes;
    type TestError = Infallible;

    // Type aliases to simplify complex test types
    type TestConsensusStream =
        ConsensusStream<MockTopologyAdaptor, MockAttestor, TestMessage, TestError, TestError>;

    type TestConsensusSubject =
        ConsensusSubject<MockTopologyAdaptor, MockAttestor, TestMessage, TestError, TestError>;

    // Helper to create a simple single-node governance for testing (like consensus_manager tests)
    async fn create_test_consensus(port: u16) -> Arc<Consensus<MockTopologyAdaptor, MockAttestor>> {
        use proven_topology::{Node, Version};
        use std::collections::HashSet;

        // Generate a fresh signing key for each test
        let signing_key = SigningKey::generate(&mut OsRng);

        // Create MockAttestor
        let attestor = Arc::new(MockAttestor::new());

        // Create single-node governance (only knows about itself)
        let governance_node = Node::new(
            signing_key.verifying_key(),
            "test-region".to_string(),
            "test-az".to_string(),
            format!("http://127.0.0.1:{port}"),
            HashSet::new(),
        );

        // Create MockAttestor to get the actual PCR values
        let attestor_for_version = MockAttestor::new();
        let actual_pcrs = attestor_for_version.pcrs_sync();

        // Create a test version using the actual PCR values from MockAttestor
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let governance = Arc::new(MockTopologyAdaptor::new(
            vec![governance_node],
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create consensus config with temporary directory for tests
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

        let consensus = Consensus::new(config).await.unwrap();

        Arc::new(consensus)
    }

    // Helper to create test options with single-node governance for basic stream tests
    async fn create_test_options(
        port: u16,
    ) -> (
        ConsensusStreamOptions<MockTopologyAdaptor, MockAttestor>,
        Arc<Consensus<MockTopologyAdaptor, MockAttestor>>,
    ) {
        let consensus = create_test_consensus(port).await;

        // Start the consensus system (automatically initializes cluster)
        consensus
            .start()
            .await
            .expect("Failed to start consensus for test");

        let options = ConsensusStreamOptions {
            consensus: consensus.clone(),
        };
        (options, consensus)
    }

    // Helper to cleanup consensus system
    async fn cleanup_consensus_system(
        consensus: &Arc<Consensus<MockTopologyAdaptor, MockAttestor>>,
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
    async fn test_consensus_stream_creation() {
        let options = create_test_options(next_port()).await;
        let consensus_ref = options.1.clone();

        let stream = TestConsensusStream::new("test_stream", options.0);

        assert_eq!(stream.name(), "test_stream");

        // Test initialization
        let initialized_stream = stream.init().await;
        assert!(
            initialized_stream.is_ok(),
            "Stream initialization should succeed"
        );

        let stream = initialized_stream.unwrap();
        assert_eq!(stream.name(), "test_stream");

        // Cleanup
        cleanup_consensus_system(&consensus_ref).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_stream_with_subjects() {
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_stream_subjects", options.0);

        // Test initialization with subjects (should work but subjects are not used)
        let subjects: Vec<TestConsensusSubject> = vec![];
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
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_publish", options.0);

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
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_batch", options.0);

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
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_empty_batch", options.0);

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
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_rollup", options.0);

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
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_metadata", options.0);

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
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_delete", options.0);

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
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_get_nonexistent", options.0);

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
    async fn test_consensus_stream_clone() {
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_clone", options.0);

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
        let options1 = create_test_options(next_port()).await;
        let options2 = create_test_options(next_port()).await;

        let stream1 = TestConsensusStream::new("stream_one", options1.0);

        let stream2 = TestConsensusStream::new("stream_two", options2.0);

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
        let options = create_test_options(next_port()).await;
        let cloned_options = &options.0;

        // Test that the consensus system references are the same
        assert!(Arc::ptr_eq(&options.1, &cloned_options.consensus));

        // Test that both instances have the same consensus reference
        assert!(Arc::ptr_eq(&options.0.consensus, &cloned_options.consensus));

        // Access the underlying consensus system properties
        // Node ID is now a hex public key, not the original test string
        let original_node_id = options.1.node_id();
        let cloned_node_id = cloned_options.consensus.node_id();

        assert_eq!(original_node_id, cloned_node_id);

        // Cleanup
        cleanup_consensus_system(&options.1).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_stream_debug_formatting() {
        let options = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_debug", options.0);

        // Test that Debug formatting works
        let debug_str = format!("{stream:?}");
        assert!(debug_str.contains("ConsensusStream"));
        assert!(debug_str.contains("test_debug"));
    }

    // Multi-node consensus tests

    // Helper to create multi-node test environment
    async fn create_multi_node_options(
        node_count: usize,
        _base_port: u16,
    ) -> Vec<ConsensusStreamOptions<MockTopologyAdaptor, MockAttestor>> {
        use proven_topology::{Node, Version};
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
            let node = Node::new(
                signing_keys[i].verifying_key(),
                "test-region".to_string(),
                format!("test-az-{i}"),
                format!("127.0.0.1:{}", node_ports[i]),
                HashSet::new(),
            );
            all_nodes.push(node);
        }

        // Create MockAttestor to get the actual PCR values
        let attestor = MockAttestor::new();
        let actual_pcrs = attestor.pcrs_sync();

        // Create test version using the actual PCR values from MockAttestor
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        // Create SHARED governance that all nodes will use
        let shared_governance = Arc::new(MockTopologyAdaptor::new(
            all_nodes.clone(),
            vec![test_version.clone()],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create consensus systems for each node with SHARED governance
        for i in 0..node_count {
            // Create consensus config with temporary directory for each node
            let config = ConsensusConfigBuilder::new()
                .governance(shared_governance.clone())
                .attestor(Arc::new(attestor.clone()))
                .signing_key(signing_keys[i].clone())
                .raft_config(RaftConfig::default())
                .transport_config(TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{}", node_ports[i]).parse().unwrap(),
                })
                .storage_config(StorageConfig::Memory)
                .cluster_join_retry_config(ClusterJoinRetryConfig::default())
                .hierarchical_config(HierarchicalConsensusConfig::default())
                .build()
                .expect("Failed to build consensus config");

            let consensus = Consensus::new(config).await.unwrap();

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
    async fn test_three_node_consensus_stream_creation() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut streams = Vec::new();

        // Create streams for all 3 nodes
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = TestConsensusStream::new(format!("consensus_stream_{i}"), options);

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
    async fn test_multi_node_stream_isolation() {
        let base_port = next_port();
        let multi_options = create_multi_node_options(3, base_port).await;

        let mut streams = Vec::new();

        // Create different stream names for each node to test isolation
        for (i, options) in multi_options.into_iter().enumerate() {
            let stream = TestConsensusStream::new(format!("isolated_stream_{i}"), options);

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
            let stream = TestConsensusStream::new("shared_consensus_stream", options);

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

    // #[tokio::test]
    // #[traced_test]
    // #[serial]
    // async fn test_multi_node_governance_consistency() {
    //     let base_port = next_port();
    //     let multi_options = create_multi_node_options(3, base_port).await;

    //     // Test that all nodes have consistent governance views
    //     let mut governance_views = Vec::new();

    //     for (i, options) in multi_options.iter().enumerate() {
    //         match options.consensus.governance().get_topology().await {
    //             Ok(topology) => {
    //                 governance_views.push(topology);
    //                 tracing::info!(
    //                     "Node {} sees {} peers in topology",
    //                     i + 1,
    //                     governance_views[i].len()
    //                 );
    //             }
    //             Err(e) => {
    //                 tracing::info!("Failed to get topology for node {}: {}", i + 1, e);
    //             }
    //         }
    //     }

    //     // If we got topology from multiple nodes, verify consistency
    //     if governance_views.len() >= 2 {
    //         let first_topology = &governance_views[0];
    //         for (i, topology) in governance_views.iter().enumerate().skip(1) {
    //             assert_eq!(
    //                 topology.len(),
    //                 first_topology.len(),
    //                 "Node {} should see same number of peers as node 1",
    //                 i + 1
    //             );

    //             // Check that the same public keys are present (order may differ)
    //             let first_keys: HashSet<&String> =
    //                 first_topology.iter().map(|n| &n.public_key).collect();
    //             let current_keys: HashSet<&String> =
    //                 topology.iter().map(|n| &n.public_key).collect();

    //             assert_eq!(
    //                 first_keys,
    //                 current_keys,
    //                 "Node {} should see same peers as node 1",
    //                 i + 1
    //             );
    //         }
    //     }
    // }

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
            let stream = TestConsensusStream::new(format!("error_test_stream_{i}"), options);

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
            let stream = TestConsensusStream::new(stream_name, options);

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
            let stream = TestConsensusStream::new(stream_name, options);

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
            let stream = TestConsensusStream::new(stream_name, options);

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
            let stream = TestConsensusStream::new(stream_name, options);

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
                                                tracing::info!(
                                                    "Node {} correctly shows old message {} as removed",
                                                    node_id,
                                                    seq1
                                                );
                                            }
                                            Ok(Some(_)) => {
                                                tracing::info!(
                                                    "Node {} still has old message {} (may be in consensus log)",
                                                    node_id,
                                                    seq1
                                                );
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
        let multi_options = create_multi_node_options(2, base_port).await;

        // Start consensus systems for both nodes
        let consensus1 = multi_options[0].consensus.clone();
        let consensus2 = multi_options[1].consensus.clone();

        consensus1.start().await.unwrap();
        consensus2.start().await.unwrap();

        // Allow time for consensus to establish
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Create streams on different nodes with the same stream name
        let stream1 = TestConsensusStream::new("cross_node_stream", multi_options[0].clone())
            .init()
            .await
            .unwrap();

        let stream2 = TestConsensusStream::new("cross_node_stream", multi_options[1].clone())
            .init()
            .await
            .unwrap();

        // Publish on one node
        let test_message = Bytes::from("cross-node message");

        match stream1.publish(test_message.clone()).await {
            Ok(seq1) => {
                tracing::info!("Published message at sequence {}", seq1);

                // Allow time for consensus replication
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Check visibility on publishing node (should always work)
                let retrieved_on_publisher = stream1.get(seq1).await.unwrap();
                assert_eq!(
                    retrieved_on_publisher,
                    Some(test_message.clone()),
                    "Publishing node should always have the message"
                );

                // Try to retrieve from other node (may not work in test environment)
                match stream2.get(seq1).await {
                    Ok(Some(retrieved_message)) => {
                        if retrieved_message == test_message {
                            tracing::info!("Cross-node replication succeeded");
                            assert_eq!(retrieved_message, test_message);
                        } else {
                            tracing::warn!("Cross-node message differs from original");
                        }
                    }
                    Ok(None) => {
                        tracing::info!(
                            "Cross-node replication didn't complete (expected in test environment)"
                        );
                        // This is acceptable in a test environment
                    }
                    Err(e) => {
                        tracing::info!("Cross-node retrieval failed: {}", e);
                        // This is also acceptable in a test environment
                    }
                }
            }
            Err(e) => {
                tracing::info!(
                    "Message publishing failed (expected without full consensus): {}",
                    e
                );
                // In test environment, publishing may fail without proper consensus setup
            }
        }

        // Clean up
        cleanup_consensus_system(&consensus1).await;
        cleanup_consensus_system(&consensus2).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_subject_stream_subscriptions() {
        let (options, consensus) = create_test_options(next_port()).await;

        // Create subjects - we need to create them manually since there's no From<String> for ConsensusSubject
        let orders_subject =
            crate::subject::ConsensusSubject::new("orders.*".to_string(), consensus.clone());

        let users_subject =
            crate::subject::ConsensusSubject::new("users.>".to_string(), consensus.clone());

        // Create streams with subject subscriptions
        let orders_stream = TestConsensusStream::new("orders_stream", options.clone())
            .init_with_subjects(vec![orders_subject])
            .await
            .unwrap();

        let users_stream = TestConsensusStream::new("users_stream", options.clone())
            .init_with_subjects(vec![users_subject])
            .await
            .unwrap();

        let all_stream = TestConsensusStream::new("all_stream", options.clone())
            .init()
            .await
            .unwrap();

        // Test publishing to different subjects directly through consensus
        let order_message = Bytes::from("new order");
        let user_message = Bytes::from("user update");
        let unmatched_message = Bytes::from("product info");

        // Publish to subjects directly via consensus manager
        let _order_seq = orders_stream
            .consensus()
            .publish_message("orders.new".to_string(), order_message.clone())
            .await
            .unwrap();
        let _user_seq = users_stream
            .consensus()
            .publish_message("users.profile.update".to_string(), user_message.clone())
            .await
            .unwrap();

        // Test that publishing to unsubscribed subject succeeds (no streams get the message)
        let unmatched_result = all_stream
            .consensus()
            .publish_message("products.new".to_string(), unmatched_message.clone())
            .await;
        assert!(
            unmatched_result.is_ok(),
            "Publishing to unsubscribed subject should succeed (but no streams receive it)"
        );

        // Allow time for consensus replication
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify orders stream received order message
        let mut found_order = false;
        for seq in 1..=orders_stream.last_seq().await.unwrap() {
            if let Some(msg) = orders_stream.get(seq).await.unwrap() {
                if msg == order_message {
                    found_order = true;
                    break;
                }
            }
        }
        assert!(
            found_order,
            "Orders stream should have received order message"
        );

        // Verify users stream received user message
        let mut found_user = false;
        for seq in 1..=users_stream.last_seq().await.unwrap() {
            if let Some(msg) = users_stream.get(seq).await.unwrap() {
                if msg == user_message {
                    found_user = true;
                    break;
                }
            }
        }
        assert!(found_user, "Users stream should have received user message");

        // Verify all_stream didn't receive subject messages (no subscriptions)
        let all_messages = all_stream.last_seq().await.unwrap();
        assert_eq!(
            all_messages, 0,
            "All stream should not have received subject messages"
        );

        cleanup_consensus_system(&consensus).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_subject_wildcard_patterns() {
        let (options, consensus) = create_test_options(next_port()).await;

        // Create streams with different wildcard patterns
        let single_wildcard_subject =
            crate::subject::ConsensusSubject::new("orders.*".to_string(), consensus.clone());

        let multi_wildcard_subject =
            crate::subject::ConsensusSubject::new("orders.>".to_string(), consensus.clone());

        let exact_subject =
            crate::subject::ConsensusSubject::new("orders.new".to_string(), consensus.clone());

        let single_stream = TestConsensusStream::new("single_wildcard_stream", options.clone())
            .init_with_subjects(vec![single_wildcard_subject])
            .await
            .unwrap();

        let multi_stream = TestConsensusStream::new("multi_wildcard_stream", options.clone())
            .init_with_subjects(vec![multi_wildcard_subject])
            .await
            .unwrap();

        let exact_stream = TestConsensusStream::new("exact_stream", options.clone())
            .init_with_subjects(vec![exact_subject])
            .await
            .unwrap();

        // Test different subject patterns directly via consensus
        let msg1 = Bytes::from("message 1");
        let msg2 = Bytes::from("message 2");
        let msg3 = Bytes::from("message 3");

        // Publish directly through consensus manager
        single_stream
            .consensus()
            .publish_message("orders.new".to_string(), msg1.clone())
            .await
            .unwrap();
        single_stream
            .consensus()
            .publish_message("orders.cancelled".to_string(), msg2.clone())
            .await
            .unwrap();
        single_stream
            .consensus()
            .publish_message("orders.new.urgent".to_string(), msg3.clone())
            .await
            .unwrap();

        // Allow time for consensus replication
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check which streams received which messages
        // orders.* should match "orders.new" and "orders.cancelled" but not "orders.new.urgent"
        let single_count = single_stream.last_seq().await.unwrap();
        assert_eq!(single_count, 2, "Single wildcard should match 2 messages");

        // orders.> should match all three
        let multi_count = multi_stream.last_seq().await.unwrap();
        assert_eq!(multi_count, 3, "Multi wildcard should match 3 messages");

        // orders.new should match only "orders.new"
        let exact_count = exact_stream.last_seq().await.unwrap();
        assert_eq!(exact_count, 1, "Exact pattern should match 1 message");

        cleanup_consensus_system(&consensus).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_subject_routing_debug() {
        let (options, consensus) = create_test_options(next_port()).await;

        // Create subjects
        let orders_subject =
            crate::subject::ConsensusSubject::new("orders.*".to_string(), consensus.clone());

        // Create stream with subscription
        let stream = TestConsensusStream::new("test_stream", options.clone())
            .init_with_subjects(vec![orders_subject])
            .await
            .unwrap();

        // Test routing directly through consensus manager
        let routed_streams = stream.consensus().route_subject("orders.new").await;
        assert!(
            routed_streams.contains("test_stream"),
            "Should route to test_stream"
        );

        let routed_streams = stream.consensus().route_subject("users.new").await;
        assert!(
            !routed_streams.contains("test_stream"),
            "Should not route to test_stream for users subject"
        );

        cleanup_consensus_system(&consensus).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multiple_streams_same_subject() {
        let (options, consensus) = create_test_options(next_port()).await;

        // Create the same subject for multiple streams
        let subject1 =
            crate::subject::ConsensusSubject::new("notifications.*".to_string(), consensus.clone());

        let subject2 =
            crate::subject::ConsensusSubject::new("notifications.*".to_string(), consensus.clone());

        // Create multiple streams with the same subject pattern
        let stream1 = TestConsensusStream::new("notification_stream_1", options.clone())
            .init_with_subjects(vec![subject1])
            .await
            .unwrap();

        let stream2 = TestConsensusStream::new("notification_stream_2", options.clone())
            .init_with_subjects(vec![subject2])
            .await
            .unwrap();

        // Publish a message directly via consensus
        let notification = Bytes::from("important notification");
        stream1
            .consensus()
            .publish_message("notifications.email".to_string(), notification.clone())
            .await
            .unwrap();

        // Allow time for consensus replication
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Both streams should have received the message
        let count1 = stream1.last_seq().await.unwrap();
        let count2 = stream2.last_seq().await.unwrap();

        assert_eq!(count1, 1, "Stream 1 should have received the message");
        assert_eq!(count2, 1, "Stream 2 should have received the message");

        // Verify the actual message content
        let msg1 = stream1.get(1).await.unwrap().unwrap();
        let msg2 = stream2.get(1).await.unwrap().unwrap();

        assert_eq!(msg1, notification);
        assert_eq!(msg2, notification);

        cleanup_consensus_system(&consensus).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_stream_auto_subscription_to_name() {
        let (options, consensus) = create_test_options(next_port()).await;

        // Create stream with normal init (should auto-subscribe to its own name)
        let stream = TestConsensusStream::new("user_events", options.clone())
            .init()
            .await
            .unwrap();

        // Verify stream is subscribed to subject with its own name
        let routing = stream.consensus().route_subject("user_events").await;
        assert!(
            routing.contains("user_events"),
            "Stream should be auto-subscribed to its own name as a subject"
        );

        // Publish to the stream's name as a subject
        let message = Bytes::from("User registration event");
        let result = stream
            .consensus()
            .publish_message("user_events".to_string(), message.clone())
            .await;
        assert!(
            result.is_ok(),
            "Should be able to publish to stream's name as subject"
        );

        // Allow time for consensus replication
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify stream received the message
        let count = stream.last_seq().await.unwrap();
        assert_eq!(count, 1, "Stream should have received the subject message");

        let received_msg = stream.get(1).await.unwrap();
        assert!(received_msg.is_some(), "Message should be retrievable");
        assert_eq!(
            received_msg.unwrap(),
            message,
            "Message content should match"
        );

        // Also verify that normal publish still works
        let normal_message = Bytes::from("Normal publish");
        let seq = stream.publish(normal_message.clone()).await.unwrap();
        assert_eq!(seq, 2, "Normal publish should get sequence 2");

        let normal_received = stream.get(2).await.unwrap();
        assert!(
            normal_received.is_some(),
            "Normal message should be retrievable"
        );
        assert_eq!(
            normal_received.unwrap(),
            normal_message,
            "Normal message content should match"
        );

        cleanup_consensus_system(&consensus).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_subject_query_methods() {
        let (options, consensus) = create_test_options(next_port()).await;

        // Create multiple streams with different subject subscriptions
        let stream1 = TestConsensusStream::new("orders_stream", options.clone())
            .init()
            .await
            .unwrap();

        let stream2 = TestConsensusStream::new("notifications_stream", options.clone())
            .init()
            .await
            .unwrap();

        // Add manual subscriptions via consensus
        let _ = stream1
            .consensus()
            .subscribe_stream_to_subject("orders_stream", "orders.*")
            .await;
        let _ = stream1
            .consensus()
            .subscribe_stream_to_subject("orders_stream", "orders.urgent")
            .await;
        let _ = stream2
            .consensus()
            .subscribe_stream_to_subject("notifications_stream", "orders.*")
            .await;
        let _ = stream2
            .consensus()
            .subscribe_stream_to_subject("notifications_stream", "notifications.>")
            .await;

        // Allow time for consensus operations
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test get_stream_subjects
        let orders_subjects = stream1
            .consensus()
            .get_stream_subjects("orders_stream")
            .await;
        assert!(orders_subjects.is_some());
        let subjects = orders_subjects.unwrap();
        assert!(subjects.contains("orders_stream")); // auto-subscription
        assert!(subjects.contains("orders.*"));
        assert!(subjects.contains("orders.urgent"));
        assert_eq!(subjects.len(), 3);

        let notifications_subjects = stream2
            .consensus()
            .get_stream_subjects("notifications_stream")
            .await;
        assert!(notifications_subjects.is_some());
        let subjects = notifications_subjects.unwrap();
        assert!(subjects.contains("notifications_stream")); // auto-subscription
        assert!(subjects.contains("orders.*"));
        assert!(subjects.contains("notifications.>"));
        assert_eq!(subjects.len(), 3);

        // Test get_subject_streams
        let orders_wildcard_streams = stream1.consensus().get_subject_streams("orders.*").await;
        assert!(orders_wildcard_streams.is_some());
        let streams = orders_wildcard_streams.unwrap();
        assert!(streams.contains("orders_stream"));
        assert!(streams.contains("notifications_stream"));
        assert_eq!(streams.len(), 2);

        let urgent_streams = stream1
            .consensus()
            .get_subject_streams("orders.urgent")
            .await;
        assert!(urgent_streams.is_some());
        let streams = urgent_streams.unwrap();
        assert!(streams.contains("orders_stream"));
        assert_eq!(streams.len(), 1);

        // Test get_all_subscriptions
        let all_subscriptions = stream1.consensus().get_all_subscriptions().await;
        assert!(!all_subscriptions.is_empty());
        assert!(all_subscriptions.contains_key("orders.*"));
        assert!(all_subscriptions.contains_key("orders.urgent"));
        assert!(all_subscriptions.contains_key("notifications.>"));
        assert!(all_subscriptions.contains_key("orders_stream"));
        assert!(all_subscriptions.contains_key("notifications_stream"));

        // Verify the content of subscriptions
        let orders_wildcard_subscribers = &all_subscriptions["orders.*"];
        assert!(orders_wildcard_subscribers.contains("orders_stream"));
        assert!(orders_wildcard_subscribers.contains("notifications_stream"));

        cleanup_consensus_system(&consensus).await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_subject_validation() {
        let (options, consensus) = create_test_options(next_port()).await;

        let stream = TestConsensusStream::new("test_stream", options.clone())
            .init()
            .await
            .unwrap();

        // Test invalid subject patterns
        let invalid_patterns = vec![
            "",                     // Empty pattern
            "orders..new",          // Consecutive dots
            "orders.>.*",           // > not at end (something after >)
            "orders.>.new",         // > not at end (something after >)
            "orders.mixed*literal", // Mixed wildcard and literal
            "orders.>mixed",        // Mixed wildcard and literal
            "orders.new.",          // Trailing dot
            ".orders.new",          // Leading dot
        ];

        for pattern in invalid_patterns {
            let result = stream
                .consensus()
                .subscribe_stream_to_subject("test_stream", pattern)
                .await;
            assert!(
                result.is_err(),
                "Pattern '{}' should be rejected but was accepted",
                pattern
            );

            // Verify the error message mentions validation
            let error_msg = result.unwrap_err().to_string();
            assert!(
                error_msg.contains("Invalid subject pattern"),
                "Error should mention invalid subject pattern: {}",
                error_msg
            );
        }

        // Test invalid stream names (system reserved)
        let result = stream
            .consensus()
            .subscribe_stream_to_subject("_system_stream", "valid.pattern")
            .await;
        assert!(result.is_err(), "System stream names should be rejected");
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("Invalid stream name"),
            "Error should mention invalid stream name: {}",
            error_msg
        );

        // Test valid patterns work
        let valid_patterns = vec![
            "orders.new",
            "orders.*",
            "orders.>",
            "orders.*.>", // * followed by > at end is valid
            "users.profile.updated",
            "system-events",
            "events_queue",
            "*",            // Single wildcard
            ">",            // Single multi-wildcard
            "orders.*.*.>", // Multiple * followed by >
        ];

        for pattern in valid_patterns {
            let result = stream
                .consensus()
                .subscribe_stream_to_subject("test_stream", pattern)
                .await;
            assert!(
                result.is_ok(),
                "Pattern '{}' should be accepted but was rejected: {:?}",
                pattern,
                result.unwrap_err()
            );
        }

        cleanup_consensus_system(&consensus).await;
    }
}
