//! Stream-based storage for consensus messages

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tracing::{debug, warn};

use crate::consensus_subject::{subject_matches_pattern, SubjectRouter};
use crate::error::ConsensusResult;
use crate::messaging::{MessagingOperation, MessagingRequest, MessagingResponse};
use crate::subscription::SubscriptionInvoker;

/// Type alias for subscription handler storage
type SubscriptionHandlerMap = Arc<RwLock<HashMap<String, Vec<Arc<dyn SubscriptionInvoker>>>>>;

/// Stream-based storage for consensus messages
#[derive(Debug, Clone)]
pub struct StreamStore {
    /// Messages by stream and sequence
    messages: Arc<RwLock<BTreeMap<String, BTreeMap<u64, Bytes>>>>,
    /// Sequence counters per stream
    sequences: Arc<RwLock<BTreeMap<String, u64>>>,
    /// Subject router for routing messages to streams
    subject_router: Arc<RwLock<SubjectRouter>>,
    /// Active subscription handlers organized by subject pattern
    subscription_handlers: SubscriptionHandlerMap,
}

impl Default for StreamStore {
    fn default() -> Self {
        Self {
            messages: Arc::new(RwLock::new(BTreeMap::new())),
            sequences: Arc::new(RwLock::new(BTreeMap::new())),
            subject_router: Arc::new(RwLock::new(SubjectRouter::new())),
            subscription_handlers: SubscriptionHandlerMap::new(RwLock::new(HashMap::new())),
        }
    }
}

impl StreamStore {
    /// Create a new stream store
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a messaging request to the store
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    #[allow(clippy::significant_drop_tightening)]
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::too_many_lines)]
    pub fn apply_request(&self, request: &MessagingRequest) -> ConsensusResult<MessagingResponse> {
        let mut messages = self.messages.write();
        let mut sequences = self.sequences.write();

        match &request.operation {
            MessagingOperation::PublishToStream { stream, data } => {
                let stream_messages = messages.entry(stream.clone()).or_default();
                let current_seq = sequences.entry(stream.clone()).or_insert(0);
                *current_seq += 1;
                let seq = *current_seq;
                stream_messages.insert(seq, data.clone());

                debug!("Applied publish to stream {} at sequence {}", stream, seq);
                Ok(MessagingResponse {
                    sequence: seq,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::PublishToStreamWithMetadata {
                stream,
                data,
                metadata,
            } => {
                let stream_messages = messages.entry(stream.clone()).or_default();
                let current_seq = sequences.entry(stream.clone()).or_insert(0);
                *current_seq += 1;
                let seq = *current_seq;
                stream_messages.insert(seq, data.clone());

                debug!(
                    "Applied publish with metadata to stream {} at sequence {} (metadata: {:?})",
                    stream, seq, metadata
                );
                Ok(MessagingResponse {
                    sequence: seq,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::PublishBatchToStream {
                stream,
                messages: batch,
            } => {
                let stream_messages = messages.entry(stream.clone()).or_default();
                let current_seq = sequences.entry(stream.clone()).or_insert(0);
                let start_seq = *current_seq;
                for data in batch {
                    *current_seq += 1;
                    stream_messages.insert(*current_seq, data.clone());
                }

                debug!(
                    "Applied batch publish to stream {} sequences {}-{}",
                    stream,
                    start_seq + 1,
                    *current_seq
                );
                Ok(MessagingResponse {
                    sequence: *current_seq,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::DeleteFromStream { stream, sequence } => {
                let stream_messages = messages.entry(stream.clone()).or_default();
                if stream_messages.remove(sequence).is_some() {
                    debug!(
                        "Deleted message at sequence {} from stream {}",
                        sequence, stream
                    );
                    Ok(MessagingResponse {
                        sequence: *sequence,
                        success: true,
                        error: None,
                    })
                } else {
                    debug!(
                        "Failed to delete message at sequence {} from stream {} (not found)",
                        sequence, stream
                    );
                    Ok(MessagingResponse {
                        sequence: *sequence,
                        success: false,
                        error: Some(format!("Message at sequence {sequence} not found")),
                    })
                }
            }
            MessagingOperation::RollupStream {
                stream,
                data,
                expected_seq,
            } => {
                let stream_messages = messages.entry(stream.clone()).or_default();
                let current_seq = sequences.entry(stream.clone()).or_insert(0);
                if *current_seq != *expected_seq {
                    return Ok(MessagingResponse {
                        sequence: *current_seq,
                        success: false,
                        error: Some(format!(
                            "Expected sequence {} but current is {}",
                            expected_seq, *current_seq
                        )),
                    });
                }

                // Clear all previous messages and add the rollup
                stream_messages.clear();
                *current_seq += 1;
                let seq = *current_seq;
                stream_messages.insert(seq, data.clone());

                debug!("Applied rollup to stream {} at sequence {}", stream, seq);
                Ok(MessagingResponse {
                    sequence: seq,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::Publish { subject, data } => {
                // NEW SIMPLIFIED SUBJECT PUBLISHING WITH CONSENSUS-BASED ROUTING
                let router = self.subject_router.read();
                let matching_streams = router.route_subject(subject);
                drop(router); // Release the lock early

                // Publish to all matching streams (if any exist)
                let mut last_sequence = 0;
                let mut _publish_count = 0;

                for stream_name in &matching_streams {
                    if let Some(stream_messages) = messages.get_mut(stream_name) {
                        let current_seq = sequences.entry(stream_name.clone()).or_insert(0);
                        *current_seq += 1;
                        stream_messages.insert(*current_seq, data.clone());
                        last_sequence = *current_seq;
                        _publish_count += 1;

                        debug!(
                            "Published message to stream {} via subject {} at sequence {}",
                            stream_name, subject, *current_seq
                        );
                    } else {
                        // Create new stream if it doesn't exist
                        let mut new_stream = BTreeMap::new();
                        new_stream.insert(1, data.clone());
                        messages.insert(stream_name.clone(), new_stream);
                        sequences.insert(stream_name.clone(), 1);
                        last_sequence = 1;
                        _publish_count += 1;

                        debug!(
                            "Created new stream {} and published message via subject {} at sequence 1",
                            stream_name, subject
                        );
                    }
                }

                // Always invoke subscription handlers for real-time processing
                let store_clone = self.clone();
                let subject_clone = subject.clone();
                let data_clone = data.clone();
                tokio::spawn(async move {
                    store_clone.invoke_subscription_handlers(
                        &subject_clone,
                        &data_clone,
                        &matching_streams,
                    );
                });

                // Always return success - publishing to an unlistened subject is not an error
                Ok(MessagingResponse {
                    sequence: last_sequence,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::PublishToSubject { subject, data } => {
                // DEPRECATED: Delegate to new Publish operation for backward compatibility
                warn!(
                    "PublishToSubject is deprecated, use Publish operation instead for subject: {}",
                    subject
                );

                let new_request = MessagingRequest {
                    operation: MessagingOperation::Publish {
                        subject: subject.clone(),
                        data: data.clone(),
                    },
                };

                self.apply_request(&new_request)
            }
            MessagingOperation::SubscribeToSubject {
                stream_name,
                subject_pattern,
            } => {
                // Validate stream name first
                if let Err(e) = crate::consensus_subject::validate_stream_name(stream_name) {
                    return Ok(MessagingResponse {
                        sequence: 0,
                        success: false,
                        error: Some(format!("Invalid stream name: {e}")),
                    });
                }

                // Validate subject pattern before subscribing
                if let Err(e) = crate::consensus_subject::validate_subject_pattern(subject_pattern)
                {
                    return Ok(MessagingResponse {
                        sequence: 0,
                        success: false,
                        error: Some(format!("Invalid subject pattern: {e}")),
                    });
                }

                let mut router = self.subject_router.write();
                router.subscribe_stream(stream_name, subject_pattern);
                Ok(MessagingResponse {
                    sequence: 0,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::UnsubscribeFromSubject {
                stream_name,
                subject_pattern,
            } => {
                let mut router = self.subject_router.write();
                router.unsubscribe_stream(stream_name, subject_pattern);
                Ok(MessagingResponse {
                    sequence: 0,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::RemoveStreamSubscriptions { stream_name } => {
                let mut router = self.subject_router.write();
                router.remove_stream(stream_name);
                Ok(MessagingResponse {
                    sequence: 0,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::BulkSubscribeToSubjects {
                stream_name,
                subject_patterns,
            } => {
                // Validate stream name first
                if let Err(e) = crate::consensus_subject::validate_stream_name(stream_name) {
                    return Ok(MessagingResponse {
                        sequence: 0,
                        success: false,
                        error: Some(format!("Invalid stream name: {e}")),
                    });
                }

                // Validate all subject patterns before subscribing to any
                for subject_pattern in subject_patterns {
                    if let Err(e) =
                        crate::consensus_subject::validate_subject_pattern(subject_pattern)
                    {
                        return Ok(MessagingResponse {
                            sequence: 0,
                            success: false,
                            error: Some(format!(
                                "Invalid subject pattern '{subject_pattern}': {e}"
                            )),
                        });
                    }
                }

                let mut router = self.subject_router.write();
                for subject_pattern in subject_patterns {
                    router.subscribe_stream(stream_name, subject_pattern);
                }
                Ok(MessagingResponse {
                    sequence: 0,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::BulkUnsubscribeFromSubjects {
                stream_name,
                subject_patterns,
            } => {
                let mut router = self.subject_router.write();
                for subject_pattern in subject_patterns {
                    router.unsubscribe_stream(stream_name, subject_pattern);
                }
                Ok(MessagingResponse {
                    sequence: 0,
                    success: true,
                    error: None,
                })
            }
        }
    }

    /// Get a message by stream and sequence
    #[must_use]
    pub fn get_message(&self, stream: &str, seq: u64) -> Option<Bytes> {
        let messages = self.messages.read();
        messages.get(stream)?.get(&seq).cloned()
    }

    /// Get the last sequence number for a stream
    #[must_use]
    pub fn last_sequence(&self, stream: &str) -> u64 {
        let sequences = self.sequences.read();
        sequences.get(stream).copied().unwrap_or(0)
    }

    /// Get all messages for a stream
    #[must_use]
    pub fn get_stream_messages(&self, stream: &str) -> BTreeMap<u64, Bytes> {
        let messages = self.messages.read();
        messages.get(stream).cloned().unwrap_or_default()
    }

    /// Subscribe a stream to a subject pattern
    pub fn subscribe_stream_to_subject(
        &self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) {
        let mut router = self.subject_router.write();
        router.subscribe_stream(stream_name, subject_pattern);
    }

    /// Unsubscribe a stream from a subject pattern
    pub fn unsubscribe_stream_from_subject(&self, stream_name: &str, subject_pattern: &str) {
        let mut router = self.subject_router.write();
        router.unsubscribe_stream(stream_name, subject_pattern);
    }

    /// Remove all subscriptions for a stream
    pub fn remove_stream_subscriptions(&self, stream_name: &str) {
        let mut router = self.subject_router.write();
        router.remove_stream(stream_name);
    }

    /// Get all subject patterns that a stream is subscribed to
    #[must_use]
    pub fn get_stream_subjects(
        &self,
        stream_name: &str,
    ) -> Option<std::collections::HashSet<String>> {
        let router = self.subject_router.read();
        router.get_stream_subjects(stream_name).cloned()
    }

    /// Get all streams subscribed to a specific subject pattern
    #[must_use]
    pub fn get_subject_streams(
        &self,
        subject_pattern: &str,
    ) -> Option<std::collections::HashSet<String>> {
        let router = self.subject_router.read();
        router.get_subject_streams(subject_pattern).cloned()
    }

    /// Get a summary of all current subject subscriptions
    #[must_use]
    pub fn get_all_subscriptions(
        &self,
    ) -> std::collections::HashMap<String, std::collections::HashSet<String>> {
        let router = self.subject_router.read();
        router.get_subscriptions().clone()
    }

    /// Get all streams that should receive messages for a given subject
    #[must_use]
    pub fn route_subject(&self, subject: &str) -> HashSet<String> {
        let router = self.subject_router.read();
        router.route_subject(subject)
    }

    /// Invoke all subscription handlers that match the published subject
    pub fn invoke_subscription_handlers(
        &self,
        subject: &str,
        data: &Bytes,
        _matching_streams: &HashSet<String>,
    ) {
        let handlers = self.subscription_handlers.read();

        // Find all subscription handlers that match this subject
        for (subject_pattern, pattern_handlers) in handlers.iter() {
            if subject_matches_pattern(subject, subject_pattern) {
                for handler in pattern_handlers {
                    let handler_clone = handler.clone();
                    let subject_clone = subject.to_string();
                    let data_clone = data.clone();
                    let metadata = Self::create_message_metadata(subject, "subscription");

                    tokio::spawn(async move {
                        if let Err(e) = handler_clone
                            .invoke(&subject_clone, data_clone, metadata)
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

    /// Create metadata for a message being sent to subscription handlers
    fn create_message_metadata(subject: &str, stream_name: &str) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("subject".to_string(), subject.to_string());
        metadata.insert("target_stream".to_string(), stream_name.to_string());

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        metadata.insert("timestamp".to_string(), timestamp.to_string());
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

        drop(handlers);

        debug!("Unregistered subscription handler: {}", subscription_id);
    }
}
