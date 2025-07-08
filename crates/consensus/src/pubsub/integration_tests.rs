//! Integration tests for PubSub and Stream integration

#[cfg(test)]
mod integration_tests {
    use crate::global::{
        GlobalOperation, GlobalRequest, PubSubMessageSource, RetentionPolicy, StorageType,
        StreamConfig, StreamStore,
    };
    use crate::pubsub::StreamBridge;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_end_to_end_stream_pubsub_integration() {
        // This test demonstrates the full flow of:
        // 1. Creating a stream
        // 2. Subscribing it to a subject pattern
        // 3. Publishing to PubSub
        // 4. Having the message automatically stored in the stream

        let store = Arc::new(StreamStore::new());

        // Step 1: Create a stream with PubSub bridge enabled
        let config = StreamConfig {
            max_messages: Some(1000),
            max_bytes: None,
            max_age_secs: Some(3600),
            storage_type: StorageType::Memory,
            retention_policy: RetentionPolicy::Limits,
            pubsub_bridge_enabled: true,
        };

        let response = store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "weather-data".to_string(),
                    config,
                },
                1,
            )
            .await;
        assert!(response.success);

        // Step 2: Subscribe the stream to weather topics
        let response = store
            .apply_operation(
                &GlobalOperation::SubscribeToSubject {
                    stream_name: "weather-data".to_string(),
                    subject_pattern: "weather.>".to_string(),
                },
                2,
            )
            .await;
        assert!(response.success);

        // Step 3: Register a subscription handler that will capture PubSub messages
        let handler = TestStreamHandler {
            stream_store: store.clone(),
        };
        store.register_subscription_handler(Arc::new(handler));

        // Step 4: Simulate publishing a message that would come from PubSub
        let source = PubSubMessageSource {
            node_id: None,
            timestamp_secs: 1234567890,
        };

        let response = store
            .apply_operation(
                &GlobalOperation::PublishFromPubSub {
                    stream_name: "weather-data".to_string(),
                    subject: "weather.temperature.city.sf".to_string(),
                    data: Bytes::from("72F"),
                    source,
                },
                3,
            )
            .await;
        assert!(response.success);

        // Step 5: Verify the message was stored
        let message = store.get_message("weather-data", 1).await;
        assert!(message.is_some());
        assert_eq!(message.unwrap(), Bytes::from("72F"));

        // Step 6: Query by subject pattern
        let messages = store
            .get_messages_by_subject("weather-data", "weather.temperature.*")
            .await;
        assert_eq!(messages.len(), 1);

        // Verify the source information
        match &messages[0].source {
            crate::global::state_machine::MessageSource::PubSub { details } => {
                assert_eq!(details.subject, "weather.temperature.city.sf");
            }
            _ => panic!("Expected PubSub source"),
        }
    }

    #[tokio::test]
    async fn test_multiple_streams_same_subject() {
        // Test that multiple streams can subscribe to the same subject pattern
        let store = Arc::new(StreamStore::new());

        // Create two streams
        for stream_name in &["stream-a", "stream-b"] {
            let response = store
                .apply_operation(
                    &GlobalOperation::CreateStream {
                        stream_name: stream_name.to_string(),
                        config: StreamConfig::default(),
                    },
                    1,
                )
                .await;
            assert!(response.success);
        }

        // Subscribe both to the same pattern
        for (i, stream_name) in ["stream-a", "stream-b"].iter().enumerate() {
            let response = store
                .apply_operation(
                    &GlobalOperation::SubscribeToSubject {
                        stream_name: stream_name.to_string(),
                        subject_pattern: "events.*".to_string(),
                    },
                    i as u64 + 2,
                )
                .await;
            assert!(response.success);
        }

        // Publish to both streams via PubSub bridge (simulating how PubSub would route to both)
        let source = PubSubMessageSource {
            node_id: None,
            timestamp_secs: 1234567890,
        };

        for stream_name in &["stream-a", "stream-b"] {
            let response = store
                .apply_operation(
                    &GlobalOperation::PublishFromPubSub {
                        stream_name: stream_name.to_string(),
                        subject: "events.login".to_string(),
                        data: Bytes::from("user123"),
                        source: source.clone(),
                    },
                    4,
                )
                .await;
            assert!(response.success);
        }

        // Verify both streams received the message
        for stream_name in &["stream-a", "stream-b"] {
            let message = store.get_message(stream_name, 1).await;
            assert!(message.is_some());
            assert_eq!(message.unwrap(), Bytes::from("user123"));
        }
    }

    #[tokio::test]
    async fn test_stream_lifecycle_with_pubsub() {
        let store = Arc::new(StreamStore::new());

        // Create stream
        let response = store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "lifecycle-stream".to_string(),
                    config: StreamConfig::default(),
                },
                1,
            )
            .await;
        assert!(response.success);

        // Subscribe to subjects
        let subjects = vec!["logs.*", "metrics.>", "alerts.critical"];
        for (i, subject) in subjects.iter().enumerate() {
            let response = store
                .apply_operation(
                    &GlobalOperation::SubscribeToSubject {
                        stream_name: "lifecycle-stream".to_string(),
                        subject_pattern: subject.to_string(),
                    },
                    i as u64 + 2,
                )
                .await;
            assert!(response.success);
        }

        // Publish various messages via PubSub bridge (simulating routing behavior)
        let test_messages = vec![
            ("logs.error", "Error: Connection failed"),
            ("metrics.cpu.usage", "85%"),
            ("alerts.critical", "Disk space low"),
            ("logs.info", "Service started"),
            ("other.topic", "Should not be stored"),
        ];

        let source = PubSubMessageSource {
            node_id: None,
            timestamp_secs: 1234567890,
        };

        for (i, (subject, data)) in test_messages.iter().enumerate() {
            // Check which streams this subject should route to
            let target_streams = store.route_subject(subject).await;

            // Only publish to streams that should receive this message
            for stream_name in target_streams {
                let response = store
                    .apply_operation(
                        &GlobalOperation::PublishFromPubSub {
                            stream_name,
                            subject: subject.to_string(),
                            data: Bytes::from(*data),
                            source: source.clone(),
                        },
                        i as u64 + 5,
                    )
                    .await;
                assert!(response.success);
            }
        }

        // Check routed subjects
        let routed = store.route_subject("logs.error").await;
        assert!(routed.contains("lifecycle-stream"));

        let routed = store.route_subject("other.topic").await;
        assert!(!routed.contains("lifecycle-stream"));

        // Unsubscribe from one pattern
        let response = store
            .apply_operation(
                &GlobalOperation::UnsubscribeFromSubject {
                    stream_name: "lifecycle-stream".to_string(),
                    subject_pattern: "logs.*".to_string(),
                },
                10,
            )
            .await;
        assert!(response.success);

        // Verify logs no longer route to stream
        let routed = store.route_subject("logs.error").await;
        assert!(!routed.contains("lifecycle-stream"));

        // But metrics still do
        let routed = store.route_subject("metrics.cpu.usage").await;
        assert!(routed.contains("lifecycle-stream"));

        // Delete the stream
        let response = store
            .apply_operation(
                &GlobalOperation::DeleteStream {
                    stream_name: "lifecycle-stream".to_string(),
                },
                11,
            )
            .await;
        assert!(response.success);

        // Verify stream is gone
        let config = store.get_stream_config("lifecycle-stream").await;
        assert!(config.is_none());
    }

    // Test handler for simulating PubSub messages
    struct TestStreamHandler {
        stream_store: Arc<StreamStore>,
    }

    #[async_trait::async_trait]
    impl crate::subscription::SubscriptionInvoker for TestStreamHandler {
        async fn invoke(
            &self,
            subject: &str,
            message: Bytes,
            _metadata: std::collections::HashMap<String, String>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // In a real system, this would submit to consensus
            // For testing, we'll just verify it would work
            println!(
                "TestStreamHandler: Would store message for subject {} to matching streams",
                subject
            );
            Ok(())
        }

        fn subscription_id(&self) -> &str {
            "test-handler"
        }

        fn subject_pattern(&self) -> &str {
            ">" // Match everything for testing
        }
    }

    impl std::fmt::Debug for TestStreamHandler {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestStreamHandler").finish()
        }
    }
}
