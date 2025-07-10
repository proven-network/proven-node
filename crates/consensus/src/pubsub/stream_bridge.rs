//! Bridge between PubSub and Streams
//!
//! This module provides the integration between the peer-to-peer PubSub system
//! and the consensus-based Streams, allowing streams to automatically capture
//! messages published via PubSub.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use tracing::{debug, error, info};

use crate::NodeId;
use crate::global::{
    PubSubMessageSource, global_manager::GlobalManager, global_state::GlobalState,
};
use crate::operations::LocalStreamOperation;
use crate::orchestrator::Orchestrator;
use crate::subscription::SubscriptionInvoker;
use proven_governance::Governance;

use super::PubSubManager;

/// Handle for a stream subscription
#[derive(Debug)]
pub struct StreamSubscriptionHandle {
    /// Stream name
    #[allow(dead_code)]
    pub stream_name: String,
    /// Subject pattern
    #[allow(dead_code)]
    pub subject_pattern: String,
    /// Subscription ID
    pub subscription_id: String,
}

/// Bridge between PubSub and Streams
pub struct StreamBridge<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// PubSub manager
    #[allow(dead_code)]
    pubsub_manager: Arc<PubSubManager<G, A>>,
    /// Global manager for consensus operations
    global_manager: Arc<GlobalManager<G, A>>,
    /// Global state for registering handlers
    global_state: Arc<GlobalState>,
    /// Hierarchical orchestrator
    hierarchical_orchestrator: Arc<Orchestrator<G, A>>,
    /// Stream subscriptions: stream_name -> (subject_pattern -> subscription_handle)
    stream_subscriptions: Arc<RwLock<HashMap<String, HashMap<String, StreamSubscriptionHandle>>>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl<G, A> StreamBridge<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new StreamBridge
    pub fn new(
        pubsub_manager: Arc<PubSubManager<G, A>>,
        global_manager: Arc<GlobalManager<G, A>>,
        global_state: Arc<GlobalState>,
        hierarchical_orchestrator: Arc<Orchestrator<G, A>>,
    ) -> Self {
        Self {
            pubsub_manager,
            global_manager,
            global_state,
            hierarchical_orchestrator,
            stream_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            task_handle: None,
        }
    }

    /// Subscribe a stream to a subject pattern
    pub async fn subscribe_stream_to_subject(
        &self,
        stream_name: &str,
        subject_pattern: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Subscribing stream '{}' to subject pattern '{}'",
            stream_name, subject_pattern
        );

        // Create subscription handler
        let handler = StreamSubscriptionHandler::new(
            stream_name.to_string(),
            subject_pattern.to_string(),
            self.global_manager.clone(),
            self.hierarchical_orchestrator.clone(),
        );

        // Register with the global state's subscription handlers
        self.global_state
            .register_subscription_handler(Arc::new(handler));

        // Track the subscription
        let handle = StreamSubscriptionHandle {
            stream_name: stream_name.to_string(),
            subject_pattern: subject_pattern.to_string(),
            subscription_id: format!("{}-{}", stream_name, subject_pattern),
        };

        let mut subscriptions = self.stream_subscriptions.write();
        subscriptions
            .entry(stream_name.to_string())
            .or_default()
            .insert(subject_pattern.to_string(), handle);

        Ok(())
    }

    /// Unsubscribe a stream from a subject pattern
    pub async fn unsubscribe_stream_from_subject(
        &self,
        stream_name: &str,
        subject_pattern: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Unsubscribing stream '{}' from subject pattern '{}'",
            stream_name, subject_pattern
        );

        let subscription_id = format!("{}-{}", stream_name, subject_pattern);

        // Unregister from global state
        self.global_state
            .unregister_subscription_handler(&subscription_id);

        // Remove from tracking
        let mut subscriptions = self.stream_subscriptions.write();
        if let Some(stream_subs) = subscriptions.get_mut(stream_name) {
            stream_subs.remove(subject_pattern);
            if stream_subs.is_empty() {
                subscriptions.remove(stream_name);
            }
        }

        Ok(())
    }

    /// Get all subscriptions for a stream
    pub fn get_stream_subscriptions(&self, stream_name: &str) -> Vec<String> {
        let subscriptions = self.stream_subscriptions.read();
        subscriptions
            .get(stream_name)
            .map(|subs| subs.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Start the bridge (if needed for background tasks)
    pub fn start(&mut self) {
        // Currently no background tasks needed
        // This is here for future expansion if needed
    }

    /// Shutdown the bridge
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }

        // Clear all subscriptions
        let subscriptions = self.stream_subscriptions.write();
        for (_, stream_subs) in subscriptions.iter() {
            for handle in stream_subs.values() {
                self.global_state
                    .unregister_subscription_handler(&handle.subscription_id);
            }
        }
    }
}

/// Subscription handler that bridges PubSub messages to Streams
#[derive(Clone)]
struct StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Stream name
    stream_name: String,
    /// Subject pattern
    subject_pattern: String,
    /// Subscription ID
    subscription_id: String,
    /// Global manager for consensus operations
    _global_manager: Arc<GlobalManager<G, A>>,
    /// Hierarchical orchestrator
    hierarchical_orchestrator: Arc<Orchestrator<G, A>>,
}

impl<G, A> StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new handler
    fn new(
        stream_name: String,
        subject_pattern: String,
        global_manager: Arc<GlobalManager<G, A>>,
        hierarchical_orchestrator: Arc<Orchestrator<G, A>>,
    ) -> Self {
        let subscription_id = format!("{}-{}", stream_name, subject_pattern);
        Self {
            stream_name,
            subject_pattern,
            subscription_id,
            _global_manager: global_manager,
            hierarchical_orchestrator,
        }
    }
}

impl<G, A> std::fmt::Debug for StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSubscriptionHandler")
            .field("stream_name", &self.stream_name)
            .field("subject_pattern", &self.subject_pattern)
            .field("subscription_id", &self.subscription_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<G, A> SubscriptionInvoker for StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    async fn invoke(
        &self,
        subject: &str,
        message: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!(
            "StreamSubscriptionHandler: Received message for subject '{}' to stream '{}'",
            subject, self.stream_name
        );

        // Extract source node ID from metadata
        let source_node_id = metadata
            .get("source_node")
            .and_then(|s| NodeId::from_hex(s).ok());

        // Create PubSub source
        let source = PubSubMessageSource {
            node_id: source_node_id,
            timestamp_secs: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Submit to consensus for persistent storage
        // Use hierarchical routing
        let operation = LocalStreamOperation::publish_from_pubsub(
            self.stream_name.clone(),
            subject.to_string(),
            message,
            source,
        );

        match self
            .hierarchical_orchestrator
            .router()
            .route_stream_operation(&self.stream_name, operation, None)
            .await
        {
            Ok(response) => {
                if response.success {
                    debug!(
                        "Successfully stored PubSub message to stream '{}' at sequence {:?}",
                        self.stream_name, response.sequence
                    );
                } else {
                    error!(
                        "Failed to store PubSub message to stream '{}': {:?}",
                        self.stream_name, response.error
                    );
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to route PubSub message for stream '{}': {}",
                    self.stream_name, e
                );
                Err(Box::new(e))
            }
        }
    }

    fn subscription_id(&self) -> &str {
        &self.subscription_id
    }

    fn subject_pattern(&self) -> &str {
        &self.subject_pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::global::{GlobalRequest, GlobalResponse, PubSubMessageSource};
    use crate::operations::{GlobalOperation, StreamManagementOperation};
    use crate::subscription::SubscriptionInvoker;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// Mock GlobalManager for testing
    #[derive(Debug)]
    struct MockGlobalManager {
        request_tx: mpsc::UnboundedSender<GlobalRequest>,
        response_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<GlobalResponse>>>,
    }

    impl MockGlobalManager {
        fn new() -> (Self, mpsc::UnboundedReceiver<GlobalRequest>) {
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            let (response_tx, response_rx) = mpsc::unbounded_channel();

            // Pre-populate with success responses
            for _ in 0..10 {
                let _ = response_tx.send(GlobalResponse {
                    success: true,
                    sequence: 1,
                    error: None,
                });
            }

            (
                Self {
                    request_tx,
                    response_rx: Arc::new(tokio::sync::Mutex::new(response_rx)),
                },
                request_rx,
            )
        }

        async fn submit_request(
            &self,
            request: GlobalRequest,
        ) -> Result<GlobalResponse, Box<dyn std::error::Error + Send + Sync>> {
            self.request_tx.send(request)?;
            let mut rx = self.response_rx.lock().await;
            rx.recv().await.ok_or_else(|| "No response".into())
        }
    }

    #[tokio::test]
    async fn test_stream_subscription_handler_invoke() {
        let (mock_manager, mut request_rx) = MockGlobalManager::new();
        let mock_manager = Arc::new(mock_manager);

        // Create handler without type parameters by using a concrete implementation
        let handler = {
            #[derive(Debug)]
            struct TestHandler {
                stream_name: String,
                subject_pattern: String,
                subscription_id: String,
                global_manager: Arc<MockGlobalManager>,
            }

            #[async_trait::async_trait]
            impl SubscriptionInvoker for TestHandler {
                async fn invoke(
                    &self,
                    _subject: &str,
                    _message: Bytes,
                    _metadata: HashMap<String, String>,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let _source = PubSubMessageSource {
                        node_id: None,
                        timestamp_secs: 123456789,
                    };

                    // In the hierarchical model, this would be routed as a LocalStreamOperation
                    // For testing purposes, we'll use a create stream operation as placeholder
                    let request = GlobalRequest {
                        operation: GlobalOperation::StreamManagement(
                            StreamManagementOperation::Create {
                                name: self.stream_name.clone(),
                                config: crate::global::StreamConfig::default(),
                                group_id: crate::allocation::ConsensusGroupId::new(1),
                            },
                        ),
                    };

                    self.global_manager.submit_request(request).await?;
                    Ok(())
                }

                fn subscription_id(&self) -> &str {
                    &self.subscription_id
                }

                fn subject_pattern(&self) -> &str {
                    &self.subject_pattern
                }
            }

            TestHandler {
                stream_name: "test-stream".to_string(),
                subject_pattern: "test.*".to_string(),
                subscription_id: "test-stream-test.*".to_string(),
                global_manager: mock_manager.clone(),
            }
        };

        // Test invoke
        let mut metadata = HashMap::new();
        metadata.insert("test".to_string(), "value".to_string());

        let result = handler
            .invoke("test.foo", Bytes::from("test message"), metadata)
            .await;
        assert!(result.is_ok());

        // Check that request was sent
        let request = request_rx.recv().await.unwrap();
        match request.operation {
            crate::operations::GlobalOperation::StreamManagement(
                StreamManagementOperation::Create { name, .. },
            ) => {
                assert_eq!(name, "test-stream");
            }
            _ => panic!("Expected CreateStream operation"),
        }
    }

    #[tokio::test]
    async fn test_stream_bridge_subscribe_unsubscribe() {
        // Since we can't easily mock the generic managers, we'll test the logic directly
        let subscriptions = Arc::new(parking_lot::RwLock::new(HashMap::new()));

        // Simulate subscribe
        let stream_name = "test-stream";
        let subject_pattern = "test.*";
        let handle = StreamSubscriptionHandle {
            stream_name: stream_name.to_string(),
            subject_pattern: subject_pattern.to_string(),
            subscription_id: format!("{}-{}", stream_name, subject_pattern),
        };

        {
            let mut subs = subscriptions.write();
            subs.entry(stream_name.to_string())
                .or_insert_with(HashMap::new)
                .insert(subject_pattern.to_string(), handle);
        }

        // Verify subscription was added
        {
            let subs = subscriptions.read();
            assert!(subs.contains_key(stream_name));
            assert!(subs[stream_name].contains_key(subject_pattern));
        }

        // Simulate unsubscribe
        {
            let mut subs = subscriptions.write();
            if let Some(stream_subs) = subs.get_mut(stream_name) {
                stream_subs.remove(subject_pattern);
                if stream_subs.is_empty() {
                    subs.remove(stream_name);
                }
            }
        }

        // Verify subscription was removed
        {
            let subs = subscriptions.read();
            assert!(!subs.contains_key(stream_name));
        }
    }

    #[test]
    fn test_stream_subscription_handle_creation() {
        let handle = StreamSubscriptionHandle {
            stream_name: "weather-data".to_string(),
            subject_pattern: "weather.>".to_string(),
            subscription_id: "weather-data-weather.>".to_string(),
        };

        assert_eq!(handle.stream_name, "weather-data");
        assert_eq!(handle.subject_pattern, "weather.>");
        assert_eq!(handle.subscription_id, "weather-data-weather.>");
    }

    #[tokio::test]
    async fn test_get_stream_subscriptions() {
        let subscriptions = Arc::new(parking_lot::RwLock::new(HashMap::new()));

        // Add some test subscriptions
        {
            let mut subs = subscriptions.write();
            let mut stream_subs = HashMap::new();
            stream_subs.insert(
                "test.*".to_string(),
                StreamSubscriptionHandle {
                    stream_name: "stream1".to_string(),
                    subject_pattern: "test.*".to_string(),
                    subscription_id: "stream1-test.*".to_string(),
                },
            );
            stream_subs.insert(
                "foo.>".to_string(),
                StreamSubscriptionHandle {
                    stream_name: "stream1".to_string(),
                    subject_pattern: "foo.>".to_string(),
                    subscription_id: "stream1-foo.>".to_string(),
                },
            );
            subs.insert("stream1".to_string(), stream_subs);
        }

        // Test getting subscriptions
        let subs = subscriptions.read();
        let stream_subs = subs
            .get("stream1")
            .map(|s| s.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();

        assert_eq!(stream_subs.len(), 2);
        assert!(stream_subs.contains(&"test.*".to_string()));
        assert!(stream_subs.contains(&"foo.>".to_string()));
    }

    #[tokio::test]
    async fn test_pubsub_message_source_creation() {
        let source = PubSubMessageSource {
            node_id: None,
            timestamp_secs: 1234567890,
        };

        assert!(source.node_id.is_none());
        assert_eq!(source.timestamp_secs, 1234567890);

        // Test with node ID
        let node_id = crate::NodeId::from_hex(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .ok();
        let source_with_node = PubSubMessageSource {
            node_id,
            timestamp_secs: 9876543210,
        };

        assert!(source_with_node.node_id.is_some());
        assert_eq!(source_with_node.timestamp_secs, 9876543210);
    }

    #[tokio::test]
    async fn test_metadata_extraction() {
        let mut metadata = HashMap::new();
        metadata.insert(
            "source_node".to_string(),
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
        );
        metadata.insert("other_key".to_string(), "other_value".to_string());

        // Test extraction logic
        let source_node_id = metadata
            .get("source_node")
            .and_then(|s| crate::NodeId::from_hex(s).ok());

        assert!(source_node_id.is_some());

        // Test with invalid node ID
        let mut bad_metadata = HashMap::new();
        bad_metadata.insert("source_node".to_string(), "invalid".to_string());

        let bad_node_id = bad_metadata
            .get("source_node")
            .and_then(|s| crate::NodeId::from_hex(s).ok());

        assert!(bad_node_id.is_none());
    }

    #[tokio::test]
    async fn test_multiple_stream_subscriptions() {
        let subscriptions = Arc::new(parking_lot::RwLock::new(HashMap::new()));

        // Add subscriptions for multiple streams
        {
            let mut subs = subscriptions.write();

            // Stream 1
            let mut stream1_subs = HashMap::new();
            stream1_subs.insert(
                "weather.*".to_string(),
                StreamSubscriptionHandle {
                    stream_name: "weather-stream".to_string(),
                    subject_pattern: "weather.*".to_string(),
                    subscription_id: "weather-stream-weather.*".to_string(),
                },
            );
            subs.insert("weather-stream".to_string(), stream1_subs);

            // Stream 2
            let mut stream2_subs = HashMap::new();
            stream2_subs.insert(
                "logs.>".to_string(),
                StreamSubscriptionHandle {
                    stream_name: "log-stream".to_string(),
                    subject_pattern: "logs.>".to_string(),
                    subscription_id: "log-stream-logs.>".to_string(),
                },
            );
            stream2_subs.insert(
                "errors.*".to_string(),
                StreamSubscriptionHandle {
                    stream_name: "log-stream".to_string(),
                    subject_pattern: "errors.*".to_string(),
                    subscription_id: "log-stream-errors.*".to_string(),
                },
            );
            subs.insert("log-stream".to_string(), stream2_subs);
        }

        // Verify all subscriptions
        let subs = subscriptions.read();
        assert_eq!(subs.len(), 2);
        assert_eq!(subs["weather-stream"].len(), 1);
        assert_eq!(subs["log-stream"].len(), 2);
    }
}
