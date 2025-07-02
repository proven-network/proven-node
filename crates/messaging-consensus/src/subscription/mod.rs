//! Subscription management in the consensus network.
//!
//! Subscriptions in the consensus messaging system.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::time::Duration;
use tracing::{debug, info, warn};

use proven_attestation::Attestor;
use proven_consensus::{consensus_manager::SubscriptionInvoker, ConsensusManager};
use proven_governance::Governance;

use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;

use crate::subscription_responder::ConsensusSubscriptionResponder;

/// A subscription invoker that bridges consensus messages to subscription handlers
#[derive(Debug)]
pub struct ConsensusSubscriptionInvoker<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    X: SubscriptionHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + std::error::Error + Sync + 'static,
    S: Debug + Send + std::error::Error + Sync + 'static,
{
    /// Unique identifier for this subscription
    subscription_id: String,
    /// Subject pattern this subscription is for
    subject_pattern: String,
    /// The subscription handler to invoke
    handler: X,
    /// Type markers
    _marker: std::marker::PhantomData<(G, A, T, D, S)>,
}

impl<G, A, X, T, D, S> ConsensusSubscriptionInvoker<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    X: SubscriptionHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + std::error::Error + Sync + 'static,
    S: Debug + Send + std::error::Error + Sync + 'static,
{
    /// Create a new consensus subscription invoker
    pub const fn new(subscription_id: String, subject_pattern: String, handler: X) -> Self {
        Self {
            subscription_id,
            subject_pattern,
            handler,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<G, A, X, T, D, S> SubscriptionInvoker for ConsensusSubscriptionInvoker<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    X: SubscriptionHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + std::error::Error + Sync + 'static,
    S: Debug + Send + std::error::Error + Sync + 'static,
{
    async fn invoke(
        &self,
        subject: &str,
        message: Bytes,
        _metadata: HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!(
            "Invoking subscription handler for subject '{}' on subscription '{}' (pattern: '{}')",
            subject, self.subscription_id, self.subject_pattern
        );

        // Try to deserialize the message to the expected type
        let typed_message = match T::try_from(message) {
            Ok(msg) => msg,
            Err(e) => {
                warn!(
                    "Failed to deserialize message for subject '{}' on subscription '{}': {:?}",
                    subject, self.subscription_id, e
                );
                return Err(Box::new(e));
            }
        };

        // Create a responder for this message
        let responder = ConsensusSubscriptionResponder::<
            G,
            A,
            X::ResponseType,
            X::ResponseDeserializationError,
            X::ResponseSerializationError,
        >::new(subject.to_string(), self.subscription_id.clone());

        // Invoke the handler
        if let Err(e) = self.handler.handle(typed_message, responder).await {
            warn!(
                "Subscription handler failed for subject '{}' on subscription '{}': {:?}",
                subject, self.subscription_id, e
            );
            return Err(Box::new(e));
        }

        debug!(
            "Successfully processed message for subject '{}' on subscription '{}'",
            subject, self.subscription_id
        );

        Ok(())
    }

    fn subscription_id(&self) -> &str {
        &self.subscription_id
    }

    fn subject_pattern(&self) -> &str {
        &self.subject_pattern
    }
}

/// Inner subscription data that handles cleanup only when the last reference is dropped
#[derive(Debug)]
struct ConsensusSubscriptionInner<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Subject name being subscribed to
    subject_name: String,
    /// Unique subscription identifier for cleanup
    subscription_id: String,
    /// Reference to consensus manager for cleanup
    consensus_manager: std::sync::Arc<ConsensusManager<G, A>>,
    /// Shutdown signal for the background task
    stop_sender: watch::Sender<()>,
}

impl<G, A> Drop for ConsensusSubscriptionInner<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn drop(&mut self) {
        // Signal shutdown to any background tasks
        let _ = self.stop_sender.send(());

        // Unregister the subscription handler from the consensus system
        self.consensus_manager
            .store()
            .unregister_subscription_handler(&self.subscription_id);

        info!(
            "Dropped consensus subscription '{}' for subject '{}'",
            self.subscription_id, self.subject_name
        );
    }
}

/// A consensus subscription that handles messages from a subject.
#[derive(Debug)]
pub struct ConsensusSubscription<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    X: SubscriptionHandler<T, D, S>,
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
    /// Shared subscription data with reference counting
    inner: Arc<ConsensusSubscriptionInner<G, A>>,
    /// Type markers
    _marker: PhantomData<(G, A, X, T, D, S)>,
}

impl<G, A, X, T, D, S> Clone for ConsensusSubscription<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    X: SubscriptionHandler<T, D, S>,
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
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

/// Options for consensus subscriptions.
#[derive(Clone, Debug)]
pub struct ConsensusSubscriptionOptions {
    /// Maximum number of messages to buffer before blocking
    pub buffer_size: usize,
    /// Timeout for message processing
    pub message_timeout: Duration,
    /// Whether to start from the beginning or only new messages
    pub start_from_beginning: bool,
}

impl Default for ConsensusSubscriptionOptions {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            message_timeout: Duration::from_secs(30),
            start_from_beginning: false,
        }
    }
}

impl SubscriptionOptions for ConsensusSubscriptionOptions {}

// Drop implementation is handled by ConsensusSubscriptionInner
// When all clones are dropped, the inner Arc will call its Drop implementation

#[async_trait]
impl<G, A, X, T, D, S> Subscription<X, T, D, S> for ConsensusSubscription<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    X: SubscriptionHandler<T, D, S>,
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
    type Error = crate::error::MessagingConsensusError;
    type Options = ConsensusSubscriptionOptions;
    type Subject = crate::subject::ConsensusSubject<G, A, T, D, S>;

    async fn new(
        subject: Self::Subject,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let subject_name = String::from(subject.clone());
        let (stop_sender, _stop_receiver) = watch::channel(());

        debug!(
            "Creating consensus subscription for subject '{}' with options: {:?}",
            subject_name, options
        );

        // Generate a unique identifier for this ephemeral subscription
        let subscription_id = format!(
            "subscription_{}_{}",
            subject_name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        // Create the subscription invoker that will handle incoming messages
        let invoker = ConsensusSubscriptionInvoker::<G, A, X, T, D, S>::new(
            subscription_id.clone(),
            subject_name.clone(),
            handler,
        );

        // Register the subscription invoker with the consensus system
        // This is ephemeral - just a callback registration, no persistent stream creation
        let consensus_manager = subject.consensus_manager();
        consensus_manager
            .store()
            .register_subscription_handler(std::sync::Arc::new(invoker));

        info!(
            "Created ephemeral consensus subscription for subject '{}' with id '{}'",
            subject_name, subscription_id
        );

        let inner = ConsensusSubscriptionInner {
            subject_name,
            subscription_id,
            consensus_manager: subject.consensus_manager().clone(),
            stop_sender,
        };

        let subscription = Self {
            inner: Arc::new(inner),
            _marker: PhantomData,
        };

        Ok(subscription)
    }
}

impl<G, A, X, T, D, S> ConsensusSubscription<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    X: SubscriptionHandler<T, D, S>,
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
    /// Get the subject name for this subscription
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn subject_name(&self) -> &str {
        &self.inner.subject_name
    }

    /// Get the subscription ID for this subscription
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn subscription_id(&self) -> &str {
        &self.inner.subscription_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::Infallible;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_bootable::Bootable;
    use proven_consensus::ConsensusConfig;
    use proven_governance_mock::MockGovernance;
    use rand::rngs::OsRng;
    use serial_test::serial;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tracing_test::traced_test;

    use crate::subject::ConsensusSubject;
    use crate::Consensus;
    use proven_messaging::subscription_handler::SubscriptionHandler;

    // Test message type
    type TestMessage = Bytes;
    type TestError = Infallible;

    // Test handler that records received messages
    #[derive(Debug, Clone)]
    struct TestSubscriptionHandler {
        name: String,
        sender: mpsc::UnboundedSender<(String, TestMessage)>, // (subject, message)
        call_count: Arc<AtomicUsize>,
    }

    impl TestSubscriptionHandler {
        fn new(name: impl Into<String>) -> (Self, mpsc::UnboundedReceiver<(String, TestMessage)>) {
            let (sender, receiver) = mpsc::unbounded_channel();
            let handler = Self {
                name: name.into(),
                sender,
                call_count: Arc::new(AtomicUsize::new(0)),
            };
            (handler, receiver)
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SubscriptionHandler<TestMessage, TestError, TestError> for TestSubscriptionHandler {
        type Error = TestError;
        type ResponseType = Bytes;
        type ResponseDeserializationError = TestError;
        type ResponseSerializationError = TestError;

        async fn handle<R>(
            &self,
            message: TestMessage,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: proven_messaging::subscription_responder::SubscriptionResponder<
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
        {
            // Increment call count
            self.call_count.fetch_add(1, Ordering::SeqCst);

            // Try to extract subject from responder - use Any for downcasting
            let subject = (&responder as &dyn std::any::Any)
                .downcast_ref::<crate::subscription_responder::ConsensusSubscriptionResponder<
                    MockGovernance,
                    MockAttestor,
                    Self::ResponseType,
                    Self::ResponseDeserializationError,
                    Self::ResponseSerializationError,
                >>()
                .map_or_else(
                    || format!("test.subject.{}", self.name),
                    |consensus_responder| consensus_responder.subject_name().to_string(),
                );

            // Send the message to the test receiver
            let _ = self.sender.send((subject, message));

            // Always use no_reply for simplicity in tests
            Ok(responder.no_reply().await)
        }
    }

    // Helper to create a test consensus system
    async fn create_test_consensus(
        node_id: &str,
        port: u16,
    ) -> Arc<Consensus<MockGovernance, MockAttestor>> {
        use proven_governance::{TopologyNode, Version};
        use std::collections::HashSet;

        let signing_key = SigningKey::generate(&mut OsRng);
        let attestor = Arc::new(MockAttestor::new());

        let public_key = hex::encode(signing_key.verifying_key().to_bytes());
        let topology_node = TopologyNode {
            availability_zone: "test-az".to_string(),
            origin: format!("127.0.0.1:{port}"),
            public_key,
            region: "test-region".to_string(),
            specializations: HashSet::new(),
        };

        let attestor_for_version = MockAttestor::new();
        let actual_pcrs = attestor_for_version.pcrs_sync();

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

        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        let config = ConsensusConfig {
            storage_dir: Some(temp_path),
            ..ConsensusConfig::default()
        };

        let consensus = Consensus::new(
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

    fn next_port() -> u16 {
        proven_util::port_allocator::allocate_port()
    }

    // Helper to create a test subject
    fn create_test_subject(
        name: &str,
        consensus: &Arc<Consensus<MockGovernance, MockAttestor>>,
    ) -> ConsensusSubject<MockGovernance, MockAttestor, TestMessage, TestError, TestError> {
        crate::subject::ConsensusSubject::new(
            name.to_string(),
            consensus.governance().as_ref().clone(),
            consensus.attestor().as_ref().clone(),
            consensus.consensus_manager().clone(),
        )
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_consensus_subject_creation() {
        let consensus = create_test_consensus("1", next_port()).await;

        let subject = create_test_subject("orders.new", &consensus);

        // Test conversion to String to verify name
        let subject_name: String = subject.into();
        assert_eq!(subject_name, "orders.new");

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_ephemeral_subscription_creation() {
        let consensus = create_test_consensus("2", next_port()).await;

        let subject = create_test_subject("orders.*", &consensus);
        let (handler, _receiver) = TestSubscriptionHandler::new("test_handler");
        let options = ConsensusSubscriptionOptions::default();

        // Create subscription
        let subscription = ConsensusSubscription::new(subject, options, handler).await;
        assert!(subscription.is_ok(), "Subscription creation should succeed");

        let subscription = subscription.unwrap();
        assert_eq!(subscription.subject_name(), "orders.*");

        // Cleanup
        drop(subscription); // Should trigger cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_subscription_receives_matching_messages() {
        let consensus = create_test_consensus("3", next_port()).await;

        let subject = create_test_subject("orders.*", &consensus);
        let (handler, mut receiver) = TestSubscriptionHandler::new("order_handler");
        let options = ConsensusSubscriptionOptions::default();

        // Create subscription
        let _subscription = ConsensusSubscription::new(subject, options, handler.clone())
            .await
            .unwrap();

        // Wait a bit for subscription to be registered
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish message to matching subject
        let message_data = Bytes::from("test order message");
        let result = consensus
            .consensus_manager()
            .publish("orders.new".to_string(), message_data.clone());
        assert!(result.is_ok(), "Publishing should succeed");

        // Wait for message to be processed
        let received = timeout(Duration::from_secs(2), receiver.recv()).await;
        assert!(received.is_ok(), "Should receive message within timeout");

        let (received_subject, received_message) = received.unwrap().unwrap();
        assert_eq!(received_subject, "orders.new");
        assert_eq!(received_message, message_data);
        assert_eq!(handler.call_count(), 1);

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_subscription_ignores_non_matching_messages() {
        let consensus = create_test_consensus("4", next_port()).await;

        let subject = create_test_subject("orders.*", &consensus);
        let (handler, mut receiver) = TestSubscriptionHandler::new("order_handler");
        let options = ConsensusSubscriptionOptions::default();

        let _subscription = ConsensusSubscription::new(subject, options, handler.clone())
            .await
            .unwrap();

        // Wait for subscription registration
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish to non-matching subject
        let message_data = Bytes::from("test user message");
        let result = consensus
            .consensus_manager()
            .publish("users.new".to_string(), message_data);
        assert!(result.is_ok(), "Publishing should succeed");

        // Should NOT receive the message
        let received = timeout(Duration::from_millis(500), receiver.recv()).await;
        assert!(received.is_err(), "Should not receive non-matching message");
        assert_eq!(handler.call_count(), 0);

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multiple_subscriptions_same_subject() {
        let consensus = create_test_consensus("5", next_port()).await;

        // Create two subscriptions to the same subject pattern
        let subject1 = create_test_subject("orders.*", &consensus);
        let subject2 = create_test_subject("orders.*", &consensus);

        let (handler1, mut receiver1) = TestSubscriptionHandler::new("handler1");
        let (handler2, mut receiver2) = TestSubscriptionHandler::new("handler2");

        let options = ConsensusSubscriptionOptions::default();

        let _subscription1 =
            ConsensusSubscription::new(subject1, options.clone(), handler1.clone())
                .await
                .unwrap();
        let _subscription2 = ConsensusSubscription::new(subject2, options, handler2.clone())
            .await
            .unwrap();

        // Wait for subscriptions to be registered
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish one message
        let message_data = Bytes::from("broadcast message");
        let result = consensus
            .consensus_manager()
            .publish("orders.new".to_string(), message_data.clone());
        assert!(result.is_ok());

        // Both handlers should receive the message
        let received1 = timeout(Duration::from_secs(2), receiver1.recv()).await;
        let received2 = timeout(Duration::from_secs(2), receiver2.recv()).await;

        assert!(received1.is_ok(), "Handler 1 should receive message");
        assert!(received2.is_ok(), "Handler 2 should receive message");

        assert_eq!(handler1.call_count(), 1);
        assert_eq!(handler2.call_count(), 1);

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_wildcard_pattern_matching() {
        let consensus = create_test_consensus("6", next_port()).await;

        // Test single wildcard pattern
        let subject = create_test_subject("orders.*", &consensus);
        let (handler, mut receiver) = TestSubscriptionHandler::new("wildcard_handler");
        let options = ConsensusSubscriptionOptions::default();

        let _subscription = ConsensusSubscription::new(subject, options, handler.clone())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Test various matching subjects
        let test_subjects = vec!["orders.new", "orders.cancelled", "orders.shipped"];

        for test_subject in test_subjects {
            let message_data = Bytes::from(format!("message for {test_subject}"));
            let result = consensus
                .consensus_manager()
                .publish(test_subject.to_string(), message_data.clone());
            assert!(result.is_ok());

            let received = timeout(Duration::from_secs(1), receiver.recv()).await;
            assert!(
                received.is_ok(),
                "Should receive message for {test_subject}"
            );
        }

        assert_eq!(handler.call_count(), 3);

        // Test non-matching subject (too many tokens)
        let result = consensus.consensus_manager().publish(
            "orders.new.urgent".to_string(),
            Bytes::from("should not match"),
        );
        assert!(result.is_ok());

        // Should not receive this message
        let received = timeout(Duration::from_millis(300), receiver.recv()).await;
        assert!(
            received.is_err(),
            "Should not receive message with too many tokens"
        );
        assert_eq!(handler.call_count(), 3); // Still 3

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_multi_wildcard_pattern_matching() {
        let consensus = create_test_consensus("7", next_port()).await;

        // Test multi-wildcard pattern
        let subject = create_test_subject("orders.>", &consensus);
        let (handler, mut receiver) = TestSubscriptionHandler::new("multi_wildcard_handler");
        let options = ConsensusSubscriptionOptions::default();

        let _subscription = ConsensusSubscription::new(subject, options, handler.clone())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Test various matching subjects with different token counts
        let test_subjects = vec![
            "orders.new",
            "orders.new.urgent",
            "orders.cancelled.refund.processed",
        ];

        for test_subject in test_subjects {
            let message_data = Bytes::from(format!("message for {test_subject}"));
            let result = consensus
                .consensus_manager()
                .publish(test_subject.to_string(), message_data);
            assert!(result.is_ok());

            let received = timeout(Duration::from_secs(1), receiver.recv()).await;
            assert!(
                received.is_ok(),
                "Should receive message for {test_subject}"
            );
        }

        assert_eq!(handler.call_count(), 3);

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_subscription_cleanup_on_drop() {
        let consensus = create_test_consensus("8", next_port()).await;

        let subject = create_test_subject("cleanup.test", &consensus);
        let (handler, mut receiver) = TestSubscriptionHandler::new("cleanup_handler");
        let options = ConsensusSubscriptionOptions::default();

        // Create subscription in a scope
        let _subscription_id = {
            let subscription = ConsensusSubscription::new(subject, options, handler.clone())
                .await
                .unwrap();
            let id = subscription.subscription_id().to_string();
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Publish message - should be received
            let result = consensus
                .consensus_manager()
                .publish("cleanup.test".to_string(), Bytes::from("before drop"));
            assert!(result.is_ok());

            let received = timeout(Duration::from_secs(1), receiver.recv()).await;
            assert!(received.is_ok(), "Should receive message before drop");
            assert_eq!(handler.call_count(), 1);

            id
        }; // subscription is dropped here

        // Wait a bit for cleanup to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish another message - should NOT be received
        let result = consensus
            .consensus_manager()
            .publish("cleanup.test".to_string(), Bytes::from("after drop"));
        assert!(result.is_ok());

        let received = timeout(Duration::from_millis(500), receiver.recv()).await;
        assert!(received.is_err(), "Should not receive message after drop");
        assert_eq!(handler.call_count(), 1); // Still 1, no new calls

        // Test that no more messages are received - this verifies cleanup worked
        let no_message_received = timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(
            no_message_received.is_err(),
            "Should not receive any additional messages after drop"
        );

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_mixed_streams_and_subscriptions() {
        let consensus = create_test_consensus("9", next_port()).await;

        // Create a stream subscription
        let result = consensus
            .consensus_manager()
            .subscribe_stream_to_subject("test_stream", "mixed.test");
        assert!(result.is_ok(), "Stream subscription should succeed");

        // Create an ephemeral subscription
        let subject = create_test_subject("mixed.test", &consensus);
        let (handler, mut receiver) = TestSubscriptionHandler::new("mixed_handler");
        let options = ConsensusSubscriptionOptions::default();

        let _subscription = ConsensusSubscription::new(subject, options, handler.clone())
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish message
        let message_data = Bytes::from("mixed routing test");
        let result = consensus
            .consensus_manager()
            .publish("mixed.test".to_string(), message_data.clone());
        assert!(result.is_ok());

        // Ephemeral subscription should receive it
        let received = timeout(Duration::from_secs(1), receiver.recv()).await;
        assert!(
            received.is_ok(),
            "Ephemeral subscription should receive message"
        );
        assert_eq!(handler.call_count(), 1);

        // Stream should also have the message (check via consensus manager)
        let stream_messages = consensus
            .consensus_manager()
            .store()
            .get_stream_messages("test_stream");
        assert!(
            !stream_messages.is_empty(),
            "Stream should contain the message"
        );

        // Cleanup
        let _ = consensus.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_subscription_clone_and_independence() {
        let consensus = create_test_consensus("10", next_port()).await;

        let subject = create_test_subject("clone.test", &consensus);
        let (handler, mut receiver) = TestSubscriptionHandler::new("clone_handler");
        let options = ConsensusSubscriptionOptions::default();

        let subscription1 = ConsensusSubscription::new(subject, options, handler.clone())
            .await
            .unwrap();
        let subscription2 = subscription1.clone();

        // Both should have the same subject but be independent objects
        assert_eq!(subscription1.subject_name(), subscription2.subject_name());
        assert_eq!(
            subscription1.subscription_id(),
            subscription2.subscription_id()
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish message
        let result = consensus
            .consensus_manager()
            .publish("clone.test".to_string(), Bytes::from("clone test"));
        assert!(result.is_ok());

        let received = timeout(Duration::from_secs(1), receiver.recv()).await;
        assert!(received.is_ok(), "Should receive message");

        // Drop one clone - subscription should still work
        drop(subscription1);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = consensus
            .consensus_manager()
            .publish("clone.test".to_string(), Bytes::from("after clone drop"));
        assert!(result.is_ok());

        let received = timeout(Duration::from_secs(1), receiver.recv()).await;
        assert!(
            received.is_ok(),
            "Should still receive message with remaining clone"
        );

        // Drop the last one - should cleanup
        drop(subscription2);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let result = consensus
            .consensus_manager()
            .publish("clone.test".to_string(), Bytes::from("after all drops"));
        assert!(result.is_ok());

        let received = timeout(Duration::from_millis(500), receiver.recv()).await;
        assert!(
            received.is_err(),
            "Should not receive message after all drops"
        );

        // Cleanup
        let _ = consensus.shutdown().await;
    }
}
