//! State machine for consensus operations

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use super::{GlobalOperation, GlobalResponse, PubSubMessageSource, StreamConfig};
use crate::error::{ConsensusError, ConsensusResult};
use crate::pubsub::{SubjectRouter, subject_matches_pattern};
use crate::subscription::{SubscriptionHandlerMap, SubscriptionInvoker};

/// Simple state machine for messaging operations
#[derive(Debug, Clone)]
pub struct StreamStore {
    /// Stream data storage
    pub(crate) streams: Arc<RwLock<HashMap<String, StreamData>>>,
    /// Subject router for advanced pattern matching
    pub(crate) subject_router: Arc<RwLock<SubjectRouter>>,
    /// Active subscription handlers organized by subject pattern
    pub(crate) subscription_handlers: SubscriptionHandlerMap,
    /// Stream configurations
    pub(crate) stream_configs: Arc<RwLock<HashMap<String, StreamConfig>>>,
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
    /// Message source
    pub source: MessageSource,
}

/// Details for PubSub-sourced messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PubSubDetails {
    /// Node that published the message
    pub node_id: Option<crate::NodeId>,
    /// Original subject
    pub subject: String,
}

/// Source of a message
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageSource {
    /// Direct consensus operation
    Consensus,
    /// From PubSub bridge
    PubSub {
        /// Node that published the message and original subject
        details: Box<PubSubDetails>,
    },
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
            stream_configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Apply a messaging operation to the state machine
    pub async fn apply_operation(
        &self,
        operation: &GlobalOperation,
        sequence: u64,
    ) -> GlobalResponse {
        match operation {
            GlobalOperation::PublishToStream {
                stream,
                data,
                metadata,
            } => {
                self.publish_to_stream(stream, data.clone(), metadata.clone(), sequence)
                    .await
            }
            GlobalOperation::PublishBatchToStream { stream, messages } => {
                self.publish_batch_to_stream(stream, messages, sequence)
                    .await
            }
            GlobalOperation::RollupStream {
                stream,
                data,
                expected_seq,
            } => {
                self.rollup_stream(stream, data.clone(), *expected_seq, sequence)
                    .await
            }
            GlobalOperation::DeleteFromStream {
                stream,
                sequence: msg_seq,
            } => self.delete_from_stream(stream, *msg_seq, sequence).await,
            GlobalOperation::SubscribeToSubject {
                stream_name,
                subject_pattern,
            } => {
                self.subscribe_to_subject(stream_name, subject_pattern, sequence)
                    .await
            }
            GlobalOperation::UnsubscribeFromSubject {
                stream_name,
                subject_pattern,
            } => {
                self.unsubscribe_from_subject(stream_name, subject_pattern, sequence)
                    .await
            }
            GlobalOperation::RemoveStreamSubscriptions { stream_name } => {
                self.remove_stream_subscriptions(stream_name, sequence)
                    .await
            }
            GlobalOperation::BulkSubscribeToSubjects {
                stream_name,
                subject_patterns,
            } => {
                self.bulk_subscribe_to_subjects(stream_name, subject_patterns, sequence)
                    .await
            }
            GlobalOperation::BulkUnsubscribeFromSubjects {
                stream_name,
                subject_patterns,
            } => {
                self.bulk_unsubscribe_from_subjects(stream_name, subject_patterns, sequence)
                    .await
            }
            GlobalOperation::CreateStream {
                stream_name,
                config,
            } => {
                self.create_stream(stream_name, config.clone(), sequence)
                    .await
            }
            GlobalOperation::UpdateStreamConfig {
                stream_name,
                config,
            } => {
                self.update_stream_config(stream_name, config.clone(), sequence)
                    .await
            }
            GlobalOperation::DeleteStream { stream_name } => {
                self.delete_stream(stream_name, sequence).await
            }
            GlobalOperation::PublishFromPubSub {
                stream_name,
                subject,
                data,
                source,
            } => {
                self.publish_from_pubsub(
                    stream_name,
                    subject,
                    data.clone(),
                    source.clone(),
                    sequence,
                )
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
    ) -> GlobalResponse {
        // Validate stream name
        if let Err(e) = validate_stream_name(stream) {
            return GlobalResponse {
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
            source: MessageSource::Consensus,
        };

        stream_data.messages.insert(message_seq, message);

        debug!("Published message {} to stream {}", message_seq, stream);

        GlobalResponse {
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
    ) -> GlobalResponse {
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
                source: MessageSource::Consensus,
            };

            stream_data.messages.insert(message_seq, message);
        }

        debug!("Published {} messages to stream {}", messages.len(), stream);

        GlobalResponse {
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
    ) -> GlobalResponse {
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
                    source: MessageSource::Consensus,
                };

                stream_data.messages.insert(1, message);
                stream_data.next_sequence = 2;

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            } else {
                GlobalResponse {
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
            GlobalResponse {
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
    ) -> GlobalResponse {
        let mut streams = self.streams.write().await;

        if let Some(stream_data) = streams.get_mut(stream) {
            if stream_data.messages.remove(&message_seq).is_some() {
                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            } else {
                GlobalResponse {
                    success: false,
                    sequence,
                    error: Some("Message not found".to_string()),
                }
            }
        } else {
            GlobalResponse {
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
    ) -> GlobalResponse {
        // Validate subject pattern
        if let Err(e) = crate::pubsub::validate_subject_pattern(subject_pattern) {
            return GlobalResponse {
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

        GlobalResponse {
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
    ) -> GlobalResponse {
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

        GlobalResponse {
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
    ) -> GlobalResponse {
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

        GlobalResponse {
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
    ) -> GlobalResponse {
        for pattern in subject_patterns {
            let _ = self
                .subscribe_to_subject(stream_name, pattern, sequence)
                .await;
        }

        GlobalResponse {
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
    ) -> GlobalResponse {
        for pattern in subject_patterns {
            let _ = self
                .unsubscribe_from_subject(stream_name, pattern, sequence)
                .await;
        }

        GlobalResponse {
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

    /// Create a new stream with configuration
    async fn create_stream(
        &self,
        stream_name: &str,
        config: StreamConfig,
        sequence: u64,
    ) -> GlobalResponse {
        // Validate stream name
        if let Err(e) = validate_stream_name(stream_name) {
            return GlobalResponse {
                success: false,
                sequence,
                error: Some(e.to_string()),
            };
        }

        let mut configs = self.stream_configs.write().await;
        let mut streams = self.streams.write().await;

        // Check if stream already exists
        if configs.contains_key(stream_name) {
            return GlobalResponse {
                success: false,
                sequence,
                error: Some(format!("Stream '{}' already exists", stream_name)),
            };
        }

        // Create stream configuration
        configs.insert(stream_name.to_string(), config);

        // Initialize stream data if it doesn't exist
        streams
            .entry(stream_name.to_string())
            .or_insert_with(|| StreamData {
                messages: BTreeMap::new(),
                next_sequence: 1,
                subscriptions: HashSet::new(),
            });

        debug!("Created stream '{}' with configuration", stream_name);

        GlobalResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Update stream configuration
    async fn update_stream_config(
        &self,
        stream_name: &str,
        config: StreamConfig,
        sequence: u64,
    ) -> GlobalResponse {
        let mut configs = self.stream_configs.write().await;

        // Check if stream exists
        if !configs.contains_key(stream_name) {
            return GlobalResponse {
                success: false,
                sequence,
                error: Some(format!("Stream '{}' does not exist", stream_name)),
            };
        }

        // Update configuration
        configs.insert(stream_name.to_string(), config);

        debug!("Updated configuration for stream '{}'", stream_name);

        GlobalResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Delete a stream
    async fn delete_stream(&self, stream_name: &str, sequence: u64) -> GlobalResponse {
        let mut configs = self.stream_configs.write().await;
        let mut streams = self.streams.write().await;
        let mut router = self.subject_router.write().await;

        // Remove configuration
        if configs.remove(stream_name).is_none() {
            return GlobalResponse {
                success: false,
                sequence,
                error: Some(format!("Stream '{}' does not exist", stream_name)),
            };
        }

        // Remove stream data
        streams.remove(stream_name);

        // Remove from subject router
        router.remove_stream(stream_name);

        debug!("Deleted stream '{}'", stream_name);

        GlobalResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Publish from PubSub to a stream
    async fn publish_from_pubsub(
        &self,
        stream_name: &str,
        subject: &str,
        data: Bytes,
        source: PubSubMessageSource,
        sequence: u64,
    ) -> GlobalResponse {
        let mut streams = self.streams.write().await;

        // Check if stream exists
        let stream_data = match streams.get_mut(stream_name) {
            Some(data) => data,
            None => {
                return GlobalResponse {
                    success: false,
                    sequence,
                    error: Some(format!("Stream '{}' not found", stream_name)),
                };
            }
        };

        let message_seq = stream_data.next_sequence;
        stream_data.next_sequence += 1;

        let message = MessageData {
            data,
            metadata: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            source: MessageSource::PubSub {
                details: Box::new(PubSubDetails {
                    node_id: source.node_id,
                    subject: subject.to_string(),
                }),
            },
        };

        stream_data.messages.insert(message_seq, message);

        debug!(
            "Published PubSub message {} to stream '{}' from subject '{}'",
            message_seq, stream_name, subject
        );

        GlobalResponse {
            success: true,
            sequence,
            error: None,
        }
    }

    /// Get messages in a range
    pub async fn get_messages_range(&self, stream: &str, start: u64, end: u64) -> Vec<MessageData> {
        let streams = self.streams.read().await;
        if let Some(stream_data) = streams.get(stream) {
            stream_data
                .messages
                .range(start..=end)
                .map(|(_, msg)| msg.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get messages by subject filter
    pub async fn get_messages_by_subject(
        &self,
        stream: &str,
        subject_pattern: &str,
    ) -> Vec<MessageData> {
        let streams = self.streams.read().await;
        if let Some(stream_data) = streams.get(stream) {
            stream_data
                .messages
                .values()
                .filter(|msg| {
                    if let MessageSource::PubSub { details } = &msg.source {
                        subject_matches_pattern(&details.subject, subject_pattern)
                    } else {
                        false
                    }
                })
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get stream configuration
    pub async fn get_stream_config(&self, stream_name: &str) -> Option<StreamConfig> {
        let configs = self.stream_configs.read().await;
        configs.get(stream_name).cloned()
    }
}

/// Validates a stream name
pub fn validate_stream_name(name: &str) -> ConsensusResult<()> {
    if name.is_empty() {
        return Err(ConsensusError::InvalidStreamName(
            "Stream name cannot be empty".to_string(),
        ));
    }
    if name.contains(['*', '>', '.']) {
        return Err(ConsensusError::InvalidStreamName(
            "Stream name cannot contain wildcards or dots".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::{GlobalOperation, RetentionPolicy, StorageType};
    use bytes::Bytes;
    use std::collections::HashMap;

    #[test]
    fn test_validate_stream_name() {
        assert!(validate_stream_name("foo").is_ok());
        assert!(validate_stream_name("foo_bar").is_ok());
        assert!(validate_stream_name("stream-123").is_ok());

        assert!(validate_stream_name("").is_err());
        assert!(validate_stream_name("foo.bar").is_err());
        assert!(validate_stream_name("foo*").is_err());
        assert!(validate_stream_name("foo>").is_err());
    }

    #[tokio::test]
    async fn test_stream_store_basic_operations() {
        let store = StreamStore::new();

        // Test publishing to a stream
        let data = Bytes::from("test message");
        let response = store
            .apply_operation(
                &GlobalOperation::PublishToStream {
                    stream: "test-stream".to_string(),
                    data: data.clone(),
                    metadata: None,
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
                &GlobalOperation::PublishToStream {
                    stream: "test-stream".to_string(),
                    data: data.clone(),
                    metadata: Some(metadata.clone()),
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
                &GlobalOperation::PublishBatchToStream {
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
                &GlobalOperation::SubscribeToSubject {
                    stream_name: "test-stream".to_string(),
                    subject_pattern: "foo.*".to_string(),
                },
                1,
            )
            .await;
        assert!(response.success);

        // Test that the subscription was created correctly
        let routed_streams = store.route_subject("foo.bar").await;
        assert!(routed_streams.contains("test-stream"));

        // Test that non-matching subjects don't route to the stream
        let routed_streams = store.route_subject("bar.baz").await;
        assert!(!routed_streams.contains("test-stream"));

        // Publish message via PubSub bridge (this is how messages get into streams from subjects now)
        let data = Bytes::from("subject message");
        let source = PubSubMessageSource {
            node_id: None,
            timestamp_secs: 1234567890,
        };
        let response = store
            .apply_operation(
                &GlobalOperation::PublishFromPubSub {
                    stream_name: "test-stream".to_string(),
                    subject: "foo.bar".to_string(),
                    data: data.clone(),
                    source,
                },
                2,
            )
            .await;
        assert!(response.success);

        // Check that message was stored in stream
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
                    &GlobalOperation::PublishToStream {
                        stream: "test-stream".to_string(),
                        data: Bytes::from(format!("message {}", i)),
                        metadata: None,
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
                &GlobalOperation::RollupStream {
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
                &GlobalOperation::PublishToStream {
                    stream: "invalid.stream".to_string(),
                    data: Bytes::from("test"),
                    metadata: None,
                },
                1,
            )
            .await;
        assert!(!response.success);
        assert!(response.error.is_some());

        // Test invalid subject pattern
        let response = store
            .apply_operation(
                &GlobalOperation::SubscribeToSubject {
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
                    &GlobalOperation::PublishToStream {
                        stream: "test-stream".to_string(),
                        data: Bytes::from(format!("message {}", i)),
                        metadata: None,
                    },
                    i,
                )
                .await;
            assert!(response.success);
        }

        // Delete middle message
        let response = store
            .apply_operation(
                &GlobalOperation::DeleteFromStream {
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

    #[tokio::test]
    async fn test_stream_configuration() {
        let store = StreamStore::new();

        // Create stream with configuration
        let config = StreamConfig {
            max_messages: Some(100),
            max_bytes: Some(1024 * 1024),
            max_age_secs: Some(3600),
            storage_type: StorageType::Memory,
            retention_policy: RetentionPolicy::Limits,
            pubsub_bridge_enabled: true,
        };

        let response = store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "config-stream".to_string(),
                    config: config.clone(),
                },
                1,
            )
            .await;
        assert!(response.success);

        // Verify configuration was stored
        let stored_config = store.get_stream_config("config-stream").await;
        assert!(stored_config.is_some());
        let stored_config = stored_config.unwrap();
        assert_eq!(stored_config.max_messages, Some(100));
        assert!(stored_config.pubsub_bridge_enabled);

        // Try to create duplicate stream
        let response = store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "config-stream".to_string(),
                    config,
                },
                2,
            )
            .await;
        assert!(!response.success);
        assert!(response.error.is_some());
    }

    #[tokio::test]
    async fn test_pubsub_to_stream() {
        let store = StreamStore::new();

        // Create stream first
        let response = store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "pubsub-stream".to_string(),
                    config: StreamConfig::default(),
                },
                1,
            )
            .await;
        assert!(response.success);

        // Publish from PubSub
        let source = PubSubMessageSource {
            node_id: None,
            timestamp_secs: 123456789,
        };

        let response = store
            .apply_operation(
                &GlobalOperation::PublishFromPubSub {
                    stream_name: "pubsub-stream".to_string(),
                    subject: "test.subject".to_string(),
                    data: Bytes::from("pubsub message"),
                    source,
                },
                2,
            )
            .await;
        assert!(response.success);

        // Verify message was stored
        let message = store.get_message("pubsub-stream", 1).await;
        assert!(message.is_some());
        assert_eq!(message.unwrap(), Bytes::from("pubsub message"));

        // Get message details to check source
        let messages = store.get_messages_range("pubsub-stream", 1, 1).await;
        assert_eq!(messages.len(), 1);
        match &messages[0].source {
            MessageSource::PubSub { details } => {
                assert_eq!(details.subject, "test.subject");
            }
            _ => panic!("Expected PubSub source"),
        }
    }

    #[tokio::test]
    async fn test_stream_queries() {
        let store = StreamStore::new();

        // Create stream
        let _ = store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "query-stream".to_string(),
                    config: StreamConfig::default(),
                },
                1,
            )
            .await;

        // Add some messages from different sources
        for i in 1..=5 {
            if i % 2 == 0 {
                // Even messages from PubSub
                let source = PubSubMessageSource {
                    node_id: None,
                    timestamp_secs: 123456789 + i,
                };
                let _ = store
                    .apply_operation(
                        &GlobalOperation::PublishFromPubSub {
                            stream_name: "query-stream".to_string(),
                            subject: format!("test.{}", if i == 2 { "foo" } else { "bar" }),
                            data: Bytes::from(format!("pubsub message {}", i)),
                            source,
                        },
                        i,
                    )
                    .await;
            } else {
                // Odd messages from consensus
                let _ = store
                    .apply_operation(
                        &GlobalOperation::PublishToStream {
                            stream: "query-stream".to_string(),
                            data: Bytes::from(format!("consensus message {}", i)),
                            metadata: None,
                        },
                        i,
                    )
                    .await;
            }
        }

        // Test range query
        let messages = store.get_messages_range("query-stream", 2, 4).await;
        assert_eq!(messages.len(), 3);

        // Test subject filter
        let messages = store
            .get_messages_by_subject("query-stream", "test.*")
            .await;
        assert_eq!(messages.len(), 2); // Only PubSub messages match

        let messages = store
            .get_messages_by_subject("query-stream", "test.foo")
            .await;
        assert_eq!(messages.len(), 1); // Only one message with test.foo
    }
}
