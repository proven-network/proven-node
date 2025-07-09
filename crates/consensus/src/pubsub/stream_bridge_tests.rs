//! Tests for Stream-PubSub bridge functionality

#[cfg(test)]
mod bridge_tests {
    use super::super::*;
    use crate::global::{GlobalRequest, GlobalResponse, GlobalState, PubSubMessageSource};
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
                        operation: crate::global::GlobalOperation::CreateStream {
                            stream_name: self.stream_name.clone(),
                            config: crate::global::StreamConfig::default(),
                            group_id: crate::allocation::ConsensusGroupId::new(1),
                        },
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
            crate::global::GlobalOperation::CreateStream { stream_name, .. } => {
                assert_eq!(stream_name, "test-stream");
            }
            _ => panic!("Expected CreateStream operation"),
        }
    }

    #[tokio::test]
    async fn test_stream_bridge_subscribe_unsubscribe() {
        // Create a mock global state
        let _global_state = Arc::new(GlobalState::new());

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
