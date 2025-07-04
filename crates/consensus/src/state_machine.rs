//! State machine for consensus operations

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::subject::{SubjectRouter, subject_matches_pattern};
use crate::subscription::{SubscriptionHandlerMap, SubscriptionInvoker};
use crate::types::{MessagingOperation, MessagingResponse};

/// Simple state machine for messaging operations
#[derive(Debug, Clone)]
pub struct StreamStore {
    /// Stream data storage
    pub(crate) streams: Arc<RwLock<HashMap<String, StreamData>>>,
    /// Subject router for advanced pattern matching
    pub(crate) subject_router: Arc<RwLock<SubjectRouter>>,
    /// Active subscription handlers organized by subject pattern
    pub(crate) subscription_handlers: SubscriptionHandlerMap,
}

/// Data for a single stream
#[derive(Debug, Clone)]
pub struct StreamData {
    /// Messages in the stream
    pub messages: BTreeMap<u64, MessageData>,
    /// Next sequence number
    pub next_sequence: u64,
    /// Subject subscriptions for this stream
    pub subscriptions: HashSet<String>,
}

/// Individual message data
#[derive(Debug, Clone)]
pub struct MessageData {
    /// Message content
    pub data: Bytes,
    /// Optional metadata
    #[allow(dead_code)]
    pub metadata: Option<HashMap<String, String>>,
    /// Timestamp
    #[allow(dead_code)]
    pub timestamp: u64,
}

impl Default for StreamStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamStore {
    /// Create a new stream store
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            subject_router: Arc::new(RwLock::new(SubjectRouter::new())),
            subscription_handlers: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    /// Apply a messaging operation to the state machine
    pub async fn apply_operation(
        &self,
        operation: &MessagingOperation,
        sequence: u64,
    ) -> MessagingResponse {
        match operation {
            MessagingOperation::PublishToStream { stream, data } => {
                self.publish_to_stream(stream, data.clone(), None, sequence)
                    .await
            }
            MessagingOperation::PublishToStreamWithMetadata {
                stream,
                data,
                metadata,
            } => {
                self.publish_to_stream(stream, data.clone(), Some(metadata.clone()), sequence)
                    .await
            }
            MessagingOperation::PublishBatchToStream { stream, messages } => {
                self.publish_batch_to_stream(stream, messages, sequence)
                    .await
            }
            MessagingOperation::Publish { subject, data } => {
                self.publish_to_subject(subject, data.clone(), None, sequence)
                    .await
            }
            MessagingOperation::PublishWithMetadata {
                subject,
                data,
                metadata,
            } => {
                self.publish_to_subject(subject, data.clone(), Some(metadata.clone()), sequence)
                    .await
            }
            MessagingOperation::RollupStream {
                stream,
                data,
                expected_seq,
            } => {
                self.rollup_stream(stream, data.clone(), *expected_seq, sequence)
                    .await
            }
            MessagingOperation::DeleteFromStream {
                stream,
                sequence: msg_seq,
            } => self.delete_from_stream(stream, *msg_seq, sequence).await,
            MessagingOperation::SubscribeToSubject {
                stream_name,
                subject_pattern,
            } => {
                self.subscribe_to_subject(stream_name, subject_pattern, sequence)
                    .await
            }
            MessagingOperation::UnsubscribeFromSubject {
                stream_name,
                subject_pattern,
            } => {
                self.unsubscribe_from_subject(stream_name, subject_pattern, sequence)
                    .await
            }
            MessagingOperation::RemoveStreamSubscriptions { stream_name } => {
                self.remove_stream_subscriptions(stream_name, sequence)
                    .await
            }
            MessagingOperation::BulkSubscribeToSubjects {
                stream_name,
                subject_patterns,
            } => {
                self.bulk_subscribe_to_subjects(stream_name, subject_patterns, sequence)
                    .await
            }
            MessagingOperation::BulkUnsubscribeFromSubjects {
                stream_name,
                subject_patterns,
            } => {
                self.bulk_unsubscribe_from_subjects(stream_name, subject_patterns, sequence)
                    .await
            }
        }
    }

    /// Publish a message to a stream
    async fn publish_to_stream(
        &self,
        stream: &str,
        data: Bytes,
        metadata: Option<HashMap<String, String>>,
        sequence: u64,
    ) -> MessagingResponse {
        // Validate stream name
        if let Err(e) = crate::subject::validate_stream_name(stream) {
            return MessagingResponse {
                success: false,
                sequence,
                error: Some(e.to_string()),
            };
        }
        let mut streams = self.streams.write().await;
        let stream_data = streams
            .entry(stream.to_string())
            .or_insert_with(|| StreamData {
                messages: BTreeMap::new(),
                next_sequence: 1,
                subscriptions: HashSet::new(),
            });

        let message_seq = stream_data.next_sequence;
        stream_data.next_sequence += 1;

        let message = MessageData {
            data,
            metadata,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        stream_data.messages.insert(message_seq, message);

        debug!("Published message {} to stream {}", message_seq, stream);

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Publish multiple messages to a stream
    async fn publish_batch_to_stream(
        &self,
        stream: &str,
        messages: &[Bytes],
        sequence: u64,
    ) -> MessagingResponse {
        let mut streams = self.streams.write().await;
        let stream_data = streams
            .entry(stream.to_string())
            .or_insert_with(|| StreamData {
                messages: BTreeMap::new(),
                next_sequence: 1,
                subscriptions: HashSet::new(),
            });

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for data in messages {
            let message_seq = stream_data.next_sequence;
            stream_data.next_sequence += 1;

            let message = MessageData {
                data: data.clone(),
                metadata: None,
                timestamp,
            };

            stream_data.messages.insert(message_seq, message);
        }

        debug!("Published {} messages to stream {}", messages.len(), stream);

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Publish to subject (route to subscribed streams)
    async fn publish_to_subject(
        &self,
        subject: &str,
        data: Bytes,
        metadata: Option<HashMap<String, String>>,
        sequence: u64,
    ) -> MessagingResponse {
        let target_streams = self.route_subject(subject).await;

        // Publish to all matching streams
        for stream in &target_streams {
            let _ = self
                .publish_to_stream(stream, data.clone(), metadata.clone(), sequence)
                .await;
        }

        // Invoke subscription handlers asynchronously
        let self_clone = self.clone();
        let subject_clone = subject.to_string();
        let data_clone = data.clone();
        let metadata_clone = metadata.clone();
        tokio::spawn(async move {
            self_clone
                .invoke_subscription_handlers(&subject_clone, &data_clone, metadata_clone)
                .await;
        });

        debug!(
            "Published to subject {} (routed to {} streams)",
            subject,
            target_streams.len()
        );

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Rollup operation (simplified implementation)
    async fn rollup_stream(
        &self,
        stream: &str,
        data: Bytes,
        expected_seq: u64,
        sequence: u64,
    ) -> MessagingResponse {
        let mut streams = self.streams.write().await;

        if let Some(stream_data) = streams.get_mut(stream) {
            if stream_data.next_sequence - 1 == expected_seq {
                // Expected sequence matches, perform rollup
                stream_data.messages.clear();
                stream_data.next_sequence = 1;

                let message = MessageData {
                    data,
                    metadata: None,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                stream_data.messages.insert(1, message);
                stream_data.next_sequence = 2;

                MessagingResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            } else {
                MessagingResponse {
                    success: false,
                    sequence,
                    error: Some(format!(
                        "Expected sequence {}, but stream is at {}",
                        expected_seq,
                        stream_data.next_sequence - 1
                    )),
                }
            }
        } else {
            MessagingResponse {
                success: false,
                sequence,
                error: Some("Stream not found".to_string()),
            }
        }
    }

    /// Delete a message from a stream
    async fn delete_from_stream(
        &self,
        stream: &str,
        message_seq: u64,
        sequence: u64,
    ) -> MessagingResponse {
        let mut streams = self.streams.write().await;

        if let Some(stream_data) = streams.get_mut(stream) {
            if stream_data.messages.remove(&message_seq).is_some() {
                MessagingResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            } else {
                MessagingResponse {
                    success: false,
                    sequence,
                    error: Some("Message not found".to_string()),
                }
            }
        } else {
            MessagingResponse {
                success: false,
                sequence,
                error: Some("Stream not found".to_string()),
            }
        }
    }

    /// Subscribe a stream to a subject pattern
    async fn subscribe_to_subject(
        &self,
        stream_name: &str,
        subject_pattern: &str,
        sequence: u64,
    ) -> MessagingResponse {
        // Validate subject pattern
        if let Err(e) = crate::subject::validate_subject_pattern(subject_pattern) {
            return MessagingResponse {
                success: false,
                sequence,
                error: Some(e.to_string()),
            };
        }

        // Update stream subscriptions
        {
            let mut streams = self.streams.write().await;
            let stream_data =
                streams
                    .entry(stream_name.to_string())
                    .or_insert_with(|| StreamData {
                        messages: BTreeMap::new(),
                        next_sequence: 1,
                        subscriptions: HashSet::new(),
                    });
            stream_data
                .subscriptions
                .insert(subject_pattern.to_string());
        }

        // Update subject router
        {
            let mut router = self.subject_router.write().await;
            router.subscribe_stream(stream_name, subject_pattern);
        }

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Unsubscribe a stream from a subject pattern
    async fn unsubscribe_from_subject(
        &self,
        stream_name: &str,
        subject_pattern: &str,
        sequence: u64,
    ) -> MessagingResponse {
        // Update stream subscriptions
        {
            let mut streams = self.streams.write().await;
            if let Some(stream_data) = streams.get_mut(stream_name) {
                stream_data.subscriptions.remove(subject_pattern);
            }
        }

        // Update subject router
        {
            let mut router = self.subject_router.write().await;
            router.unsubscribe_stream(stream_name, subject_pattern);
        }

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Remove all subscriptions for a stream
    async fn remove_stream_subscriptions(
        &self,
        stream_name: &str,
        sequence: u64,
    ) -> MessagingResponse {
        // Clear stream subscriptions
        {
            let mut streams = self.streams.write().await;
            if let Some(stream_data) = streams.get_mut(stream_name) {
                stream_data.subscriptions.clear();
            }
        }

        // Remove from subject router
        {
            let mut router = self.subject_router.write().await;
            router.remove_stream(stream_name);
        }

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Bulk subscribe to multiple subjects
    async fn bulk_subscribe_to_subjects(
        &self,
        stream_name: &str,
        subject_patterns: &[String],
        sequence: u64,
    ) -> MessagingResponse {
        for pattern in subject_patterns {
            let _ = self
                .subscribe_to_subject(stream_name, pattern, sequence)
                .await;
        }

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Bulk unsubscribe from multiple subjects
    async fn bulk_unsubscribe_from_subjects(
        &self,
        stream_name: &str,
        subject_patterns: &[String],
        sequence: u64,
    ) -> MessagingResponse {
        for pattern in subject_patterns {
            let _ = self
                .unsubscribe_from_subject(stream_name, pattern, sequence)
                .await;
        }

        MessagingResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Route a subject to subscribed streams
    pub async fn route_subject(&self, subject: &str) -> HashSet<String> {
        let router = self.subject_router.read().await;
        router.route_subject(subject)
    }

    /// Invoke all subscription handlers that match the published subject
    async fn invoke_subscription_handlers(
        &self,
        subject: &str,
        data: &Bytes,
        metadata: Option<HashMap<String, String>>,
    ) {
        let handlers = self.subscription_handlers.read();

        // Find all subscription handlers that match this subject
        for (subject_pattern, pattern_handlers) in handlers.iter() {
            if subject_matches_pattern(subject, subject_pattern) {
                for handler in pattern_handlers {
                    let handler_clone = handler.clone();
                    let subject_clone = subject.to_string();
                    let data_clone = data.clone();
                    let mut handler_metadata = Self::create_message_metadata(subject);

                    // If we have subject metadata, merge it in
                    if let Some(subject_meta) = &metadata {
                        handler_metadata.extend(subject_meta.clone());
                    }

                    tokio::spawn(async move {
                        if let Err(e) = handler_clone
                            .invoke(&subject_clone, data_clone, handler_metadata)
                            .await
                        {
                            warn!(
                                "Subscription handler failed for subscription {}: {}",
                                handler_clone.subscription_id(),
                                e
                            );
                        }
                    });
                }
            }
        }
    }

    /// Create metadata for a message
    fn create_message_metadata(subject: &str) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("subject".to_string(), subject.to_string());
        metadata.insert(
            "timestamp".to_string(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string(),
        );
        metadata.insert("message_id".to_string(), uuid::Uuid::new_v4().to_string());
        metadata
    }

    /// Register a subscription handler for a subject pattern
    pub fn register_subscription_handler(&self, handler: Arc<dyn SubscriptionInvoker>) {
        let subject_pattern = handler.subject_pattern().to_string();
        let subscription_id = handler.subscription_id().to_string();

        self.subscription_handlers
            .write()
            .entry(subject_pattern.clone())
            .or_default()
            .push(handler);

        debug!(
            "Registered subscription handler '{}' for subject pattern: {}",
            subscription_id, subject_pattern
        );
    }

    /// Unregister a specific subscription handler by ID
    pub fn unregister_subscription_handler(&self, subscription_id: &str) {
        let mut handlers = self.subscription_handlers.write();
        let mut pattern_to_remove = None;

        for (pattern, handler_list) in handlers.iter_mut() {
            handler_list.retain(|h| h.subscription_id() != subscription_id);
            if handler_list.is_empty() {
                pattern_to_remove = Some(pattern.clone());
            }
        }

        if let Some(pattern) = pattern_to_remove {
            handlers.remove(&pattern);
            debug!(
                "Removed empty handler list for subject pattern: {}",
                pattern
            );
        }

        debug!("Unregistered subscription handler: {}", subscription_id);
    }

    /// Unregister all subscription handlers for a subject pattern
    pub fn unregister_subject_handlers(&self, subject_pattern: &str) {
        let removed = self.subscription_handlers.write().remove(subject_pattern);
        if removed.is_some() {
            debug!(
                "Unregistered all subscription handlers for subject pattern: {}",
                subject_pattern
            );
        }
    }

    /// Get a message from a stream
    pub async fn get_message(&self, stream: &str, seq: u64) -> Option<Bytes> {
        let streams = self.streams.read().await;
        streams
            .get(stream)
            .and_then(|stream_data| stream_data.messages.get(&seq))
            .map(|msg| msg.data.clone())
    }

    /// Get the last sequence number for a stream
    pub async fn last_sequence(&self, stream: &str) -> u64 {
        let streams = self.streams.read().await;
        streams
            .get(stream)
            .map(|stream_data| stream_data.next_sequence - 1)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MessagingOperation;
    use bytes::Bytes;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_stream_store_basic_operations() {
        let store = StreamStore::new();

        // Test publishing to a stream
        let data = Bytes::from("test message");
        let response = store
            .apply_operation(
                &MessagingOperation::PublishToStream {
                    stream: "test-stream".to_string(),
                    data: data.clone(),
                },
                1,
            )
            .await;

        assert!(response.success);
        assert_eq!(response.sequence, 1);

        // Test getting the message
        let retrieved = store.get_message("test-stream", 1).await;
        assert_eq!(retrieved, Some(data));

        // Test last sequence
        let last_seq = store.last_sequence("test-stream").await;
        assert_eq!(last_seq, 1);
    }

    #[tokio::test]
    async fn test_stream_store_metadata() {
        let store = StreamStore::new();

        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());

        let data = Bytes::from("test message with metadata");
        let response = store
            .apply_operation(
                &MessagingOperation::PublishToStreamWithMetadata {
                    stream: "test-stream".to_string(),
                    data: data.clone(),
                    metadata: metadata.clone(),
                },
                1,
            )
            .await;

        assert!(response.success);
        assert_eq!(response.sequence, 1);
    }

    #[tokio::test]
    async fn test_stream_store_batch_operations() {
        let store = StreamStore::new();

        let messages = vec![
            Bytes::from("message 1"),
            Bytes::from("message 2"),
            Bytes::from("message 3"),
        ];

        let response = store
            .apply_operation(
                &MessagingOperation::PublishBatchToStream {
                    stream: "test-stream".to_string(),
                    messages: messages.clone(),
                },
                1,
            )
            .await;

        assert!(response.success);

        // Check that all messages were stored
        let last_seq = store.last_sequence("test-stream").await;
        assert_eq!(last_seq, 3);

        // Check individual messages
        for i in 1..=3 {
            let retrieved = store.get_message("test-stream", i).await;
            assert_eq!(retrieved, Some(messages[(i - 1) as usize].clone()));
        }
    }

    #[tokio::test]
    async fn test_stream_store_subject_routing() {
        let store = StreamStore::new();

        // Subscribe stream to subject pattern
        let response = store
            .apply_operation(
                &MessagingOperation::SubscribeToSubject {
                    stream_name: "test-stream".to_string(),
                    subject_pattern: "foo.*".to_string(),
                },
                1,
            )
            .await;
        assert!(response.success);

        // Publish to subject
        let data = Bytes::from("subject message");
        let response = store
            .apply_operation(
                &MessagingOperation::Publish {
                    subject: "foo.bar".to_string(),
                    data: data.clone(),
                },
                2,
            )
            .await;
        assert!(response.success);

        // Check that message was routed to stream
        let last_seq = store.last_sequence("test-stream").await;
        assert_eq!(last_seq, 1);

        let retrieved = store.get_message("test-stream", 1).await;
        assert_eq!(retrieved, Some(data));
    }

    #[tokio::test]
    async fn test_stream_store_rollup() {
        let store = StreamStore::new();

        // Publish some messages first
        for i in 1..=3 {
            let response = store
                .apply_operation(
                    &MessagingOperation::PublishToStream {
                        stream: "test-stream".to_string(),
                        data: Bytes::from(format!("message {}", i)),
                    },
                    i,
                )
                .await;
            assert!(response.success);
        }

        // Rollup the stream
        let rollup_data = Bytes::from("rollup data");
        let response = store
            .apply_operation(
                &MessagingOperation::RollupStream {
                    stream: "test-stream".to_string(),
                    data: rollup_data.clone(),
                    expected_seq: 3,
                },
                4,
            )
            .await;
        assert!(response.success);

        // Check that rollup worked
        let last_seq = store.last_sequence("test-stream").await;
        assert_eq!(last_seq, 1); // Should be reset to 1

        let retrieved = store.get_message("test-stream", 1).await;
        assert_eq!(retrieved, Some(rollup_data));

        // Old messages should be gone
        let old_message = store.get_message("test-stream", 2).await;
        assert_eq!(old_message, None);
    }

    #[tokio::test]
    async fn test_stream_store_validation() {
        let store = StreamStore::new();

        // Test invalid stream name
        let response = store
            .apply_operation(
                &MessagingOperation::PublishToStream {
                    stream: "invalid.stream".to_string(),
                    data: Bytes::from("test"),
                },
                1,
            )
            .await;
        assert!(!response.success);
        assert!(response.error.is_some());

        // Test invalid subject pattern
        let response = store
            .apply_operation(
                &MessagingOperation::SubscribeToSubject {
                    stream_name: "test-stream".to_string(),
                    subject_pattern: "invalid..pattern".to_string(),
                },
                1,
            )
            .await;
        assert!(!response.success);
        assert!(response.error.is_some());
    }

    #[tokio::test]
    async fn test_stream_store_deletion() {
        let store = StreamStore::new();

        // Publish some messages
        for i in 1..=3 {
            let response = store
                .apply_operation(
                    &MessagingOperation::PublishToStream {
                        stream: "test-stream".to_string(),
                        data: Bytes::from(format!("message {}", i)),
                    },
                    i,
                )
                .await;
            assert!(response.success);
        }

        // Delete middle message
        let response = store
            .apply_operation(
                &MessagingOperation::DeleteFromStream {
                    stream: "test-stream".to_string(),
                    sequence: 2,
                },
                4,
            )
            .await;
        assert!(response.success);

        // Check deletion
        let message1 = store.get_message("test-stream", 1).await;
        let message2 = store.get_message("test-stream", 2).await;
        let message3 = store.get_message("test-stream", 3).await;

        assert!(message1.is_some());
        assert!(message2.is_none());
        assert!(message3.is_some());
    }
}
