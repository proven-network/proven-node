//! OpenRaft-based consensus implementation for messaging

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use super::consensus_subject::{
    subject_matches_pattern, validate_stream_name, validate_subject_pattern, SubjectRouter,
};
use async_trait::async_trait;
use bytes::Bytes;
use openraft::Config;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid;

use crate::error::{ConsensusError, ConsensusResult};

/// Type alias for subscription handler storage
type SubscriptionHandlerMap = Arc<RwLock<HashMap<String, Vec<Arc<dyn SubscriptionInvoker>>>>>;

/// Trait for subscription handlers that can be invoked when messages are published to subjects
#[async_trait]
pub trait SubscriptionInvoker: Send + Sync + std::fmt::Debug {
    /// Invoke the subscription handler with a message
    async fn invoke(
        &self,
        subject: &str,
        message: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Get the subscription ID for this invoker
    fn subscription_id(&self) -> &str;

    /// Get the subject pattern this subscription is interested in
    fn subject_pattern(&self) -> &str;
}

/// Messaging request for consensus operations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagingRequest {
    /// Operation type
    pub operation: MessagingOperation,
}

/// Types of messaging operations that can be performed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessagingOperation {
    /// Publish a single message directly to a stream
    PublishToStream {
        /// Target stream name
        stream: String,
        /// Message data to publish
        data: Bytes,
    },
    /// Publish a single message with metadata directly to a stream
    PublishToStreamWithMetadata {
        /// Target stream name
        stream: String,
        /// Message data to publish
        data: Bytes,
        /// Associated metadata
        metadata: std::collections::HashMap<String, String>,
    },
    /// Publish multiple messages as a batch directly to a stream
    PublishBatchToStream {
        /// Target stream name
        stream: String,
        /// List of messages to publish
        messages: Vec<Bytes>,
    },
    /// Delete a specific message by sequence number from a stream
    DeleteFromStream {
        /// Target stream name
        stream: String,
        /// Sequence number to delete
        sequence: u64,
    },
    /// Rollup operation (replace all previous messages) on a stream
    RollupStream {
        /// Target stream name
        stream: String,
        /// New rollup data
        data: Bytes,
        /// Expected current sequence number
        expected_seq: u64,
    },
    /// Publish to a subject (NEW - simplified subject publishing)
    Publish {
        /// Subject to publish to
        subject: String,
        /// Message data to publish
        data: Bytes,
    },
    /// Publish to a subject (DEPRECATED - use Publish instead)
    PublishToSubject {
        /// Subject to publish to
        subject: String,
        /// Message data to publish
        data: Bytes,
    },
    /// Subscribe a stream to a subject pattern (NEW - consensus operation)
    SubscribeToSubject {
        /// Stream name to subscribe
        stream_name: String,
        /// Subject pattern to subscribe to  
        subject_pattern: String,
    },
    /// Unsubscribe a stream from a subject pattern (NEW - consensus operation)
    UnsubscribeFromSubject {
        /// Stream name to unsubscribe
        stream_name: String,
        /// Subject pattern to unsubscribe from
        subject_pattern: String,
    },
    /// Remove all subject subscriptions for a stream (NEW - consensus operation)
    RemoveStreamSubscriptions {
        /// Stream name to remove all subscriptions for
        stream_name: String,
    },
    /// Subscribe a stream to multiple subject patterns in one operation (NEW - bulk operation)
    BulkSubscribeToSubjects {
        /// Stream name to subscribe
        stream_name: String,
        /// List of subject patterns to subscribe to
        subject_patterns: Vec<String>,
    },
    /// Unsubscribe a stream from multiple subject patterns in one operation (NEW - bulk operation)
    BulkUnsubscribeFromSubjects {
        /// Stream name to unsubscribe
        stream_name: String,
        /// List of subject patterns to unsubscribe from
        subject_patterns: Vec<String>,
    },
}

/// Response from a messaging operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagingResponse {
    /// Assigned sequence number
    pub sequence: u64,
    /// Success flag
    pub success: bool,
    /// Optional error message
    pub error: Option<String>,
}

openraft::declare_raft_types!(
    /// Types for the application using RaftTypeConfig
    pub TypeConfig:
        D = MessagingRequest,
        R = MessagingResponse,
        NodeId = String,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Configuration for the consensus manager
#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    /// Timeout for consensus operations
    pub consensus_timeout: Duration,
    /// Whether to require all nodes for consensus (true) or just majority (false)
    pub require_all_nodes: bool,
    /// `OpenRaft` configuration
    pub raft_config: Arc<Config>,
    /// Directory path for persistent storage
    pub storage_dir: Option<String>,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        let raft_config = Config {
            heartbeat_interval: 500,    // 500ms
            election_timeout_min: 1500, // 1.5s
            election_timeout_max: 3000, // 3s
            ..Config::default()
        };

        Self {
            consensus_timeout: Duration::from_secs(30),
            require_all_nodes: false,
            raft_config: Arc::new(raft_config.validate().unwrap()),
            storage_dir: None, // Default to None, will use temporary directory
        }
    }
}

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
                // The subject router state is kept consistent across all nodes through consensus operations
                let router = self.subject_router.read();
                let matching_streams = router.route_subject(subject);
                drop(router); // Release the lock early

                // Publish to all matching streams (if any exist)
                let mut last_sequence = 0;
                let mut publish_count = 0;

                for stream_name in &matching_streams {
                    if let Some(stream_messages) = messages.get_mut(stream_name) {
                        let current_seq = sequences.entry(stream_name.clone()).or_insert(0);
                        *current_seq += 1;
                        stream_messages.insert(*current_seq, data.clone());
                        last_sequence = *current_seq;
                        publish_count += 1;

                        debug!(
                            "Published message to stream {} via subject {} at sequence {} (new Publish operation)",
                            stream_name, subject, *current_seq
                        );
                    } else {
                        // Create new stream if it doesn't exist
                        let mut new_stream = BTreeMap::new();
                        new_stream.insert(1, data.clone());
                        messages.insert(stream_name.clone(), new_stream);
                        sequences.insert(stream_name.clone(), 1);
                        last_sequence = 1;
                        publish_count += 1;

                        debug!(
                            "Created new stream {} and published message via subject {} at sequence 1 (new Publish operation)",
                            stream_name, subject
                        );
                    }
                }

                // Always invoke subscription handlers for real-time processing, regardless of stream count
                // Note: This is spawned as async task to not block consensus
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

                if publish_count > 0 {
                    debug!(
                        "Published message to {} streams via subject {} using new Publish operation",
                        publish_count, subject
                    );
                } else {
                    debug!(
                        "Published message to subject {} (no streams subscribed, but subscription handlers may be invoked)",
                        subject
                    );
                }

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

                // Create new request with Publish operation
                let new_request = MessagingRequest {
                    operation: MessagingOperation::Publish {
                        subject: subject.clone(),
                        data: data.clone(),
                    },
                };

                // Delegate to new Publish operation
                self.apply_request(&new_request)
            }
            MessagingOperation::SubscribeToSubject {
                stream_name,
                subject_pattern,
            } => {
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

    /// Delete a message from a stream
    #[must_use]
    pub fn delete_message(&self, stream: &str, seq: u64) -> bool {
        let mut messages = self.messages.write();
        messages
            .get_mut(stream)
            .is_some_and(|stream_messages| stream_messages.remove(&seq).is_some())
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

    /// Get all streams that should receive messages for a given subject
    #[must_use]
    pub fn route_subject(&self, subject: &str) -> std::collections::HashSet<String> {
        let router = self.subject_router.read();
        router.route_subject(subject)
    }

    /// Remove all subscriptions for a stream
    pub fn remove_stream_subscriptions(&self, stream_name: &str) {
        let mut router = self.subject_router.write();
        router.remove_stream(stream_name);
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
            // Check if this subject matches the subscription pattern
            if subject_matches_pattern(subject, subject_pattern) {
                for handler in pattern_handlers {
                    let handler_clone = handler.clone();
                    let subject_clone = subject.to_string();
                    let data_clone = data.clone();
                    let metadata = Self::create_message_metadata(subject, "subscription");

                    // Spawn non-blocking async task
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

        // Use system time for timestamp
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

/// OpenRaft-based consensus manager
pub struct ConsensusManager<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Node identifier (public key)
    node_id: String,
    /// Consensus configuration
    config: ConsensusConfig,
    /// `OpenRaft` instance
    raft: Arc<RwLock<Option<openraft::Raft<TypeConfig>>>>,
    /// Stream store for message storage
    store: StreamStore,
    /// Type markers
    _marker: std::marker::PhantomData<(G, A)>,
}

impl<G, A> std::fmt::Debug for ConsensusManager<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusManager")
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .field("raft_initialized", &self.raft.read().is_some())
            .field("store", &self.store)
            .finish()
    }
}

impl<G, A> ConsensusManager<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Create a new consensus manager
    pub fn new(node_id: &str, config: ConsensusConfig) -> Self {
        info!("Creating OpenRaft consensus manager for node: {}", node_id);

        Self {
            node_id: node_id.to_string(),
            config,
            raft: Arc::new(RwLock::new(None)), // Will be initialized later with storage and network
            store: StreamStore::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Initialize the Raft instance with storage and network
    ///
    /// # Errors
    ///
    /// Returns an error if Raft initialization fails.
    pub async fn initialize_raft(
        &mut self,
        log_storage: impl openraft::storage::RaftLogStorage<TypeConfig>,
        state_machine: impl openraft::storage::RaftStateMachine<TypeConfig>,
        network: impl openraft::RaftNetworkFactory<TypeConfig>,
    ) -> ConsensusResult<()> {
        let raft = openraft::Raft::new(
            self.node_id.clone(),
            self.config.raft_config.clone(),
            network,
            log_storage,
            state_machine,
        )
        .await
        .map_err(|e| ConsensusError::InvalidMessage(format!("Failed to create Raft: {e}")))?;

        *self.raft.write() = Some(raft);
        info!("OpenRaft instance initialized for node {}", self.node_id);
        Ok(())
    }

    /// Initialize Raft with our consensus implementations
    ///
    /// # Errors
    ///
    /// Returns an error if Raft initialization fails.
    pub async fn initialize_raft_with_consensus(
        &mut self,
        storage: crate::storage::MessagingStorage,
        network: Arc<crate::network::ConsensusNetwork<G, A>>,
        network_tx: tokio::sync::mpsc::UnboundedSender<(String, crate::network::ConsensusMessage)>,
    ) -> ConsensusResult<()> {
        use crate::raft_network::ConsensusRaftNetworkFactory;
        use crate::raft_state_machine::ConsensusStateMachine;

        // Create the state machine with our stream store
        let state_machine = ConsensusStateMachine::new(Arc::new(self.store.clone()));

        // Create the network factory
        let network_factory = ConsensusRaftNetworkFactory::new(network, network_tx);

        // Initialize Raft with our implementations
        self.initialize_raft(storage, state_machine, network_factory)
            .await
    }

    /// Check if Raft is initialized
    #[must_use]
    pub fn is_raft_initialized(&self) -> bool {
        self.raft.read().is_some()
    }

    /// Publish a message through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn publish_message(&self, stream: String, data: Bytes) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStream { stream, data },
        };

        self.propose_request(&request)
    }

    /// Publish a message with metadata through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn publish_message_with_metadata(
        &self,
        stream: String,
        data: Bytes,
        metadata: std::collections::HashMap<String, String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStreamWithMetadata {
                stream,
                data,
                metadata,
            },
        };

        self.propose_request(&request)
    }

    /// Publish multiple messages as a batch through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn publish_batch(&self, stream: String, messages: Vec<Bytes>) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishBatchToStream { stream, messages },
        };

        self.propose_request(&request)
    }

    /// Rollup operation through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn rollup_message(
        &self,
        stream: String,
        data: Bytes,
        expected_seq: u64,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::RollupStream {
                stream,
                data,
                expected_seq,
            },
        };

        self.propose_request(&request)
    }

    /// Delete a message from a stream through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn delete_message(&self, stream: String, sequence: u64) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::DeleteFromStream { stream, sequence },
        };

        self.propose_request(&request)
    }

    /// Publish a message to a subject through consensus
    ///
    /// Publish to a subject through consensus (NEW - preferred method)
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn publish(&self, subject: String, data: Bytes) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::Publish { subject, data },
        };

        self.propose_request(&request)
    }

    /// Subscribe a stream to a subject pattern through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn subscribe_stream_to_subject(
        &self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) -> ConsensusResult<u64> {
        let stream_name = stream_name.into();
        let subject_pattern = subject_pattern.into();

        // Validate stream name and subject pattern
        if let Err(e) = validate_stream_name(&stream_name) {
            return Err(ConsensusError::InvalidMessage(format!(
                "Invalid stream name '{stream_name}': {e}"
            )));
        }

        if let Err(e) = validate_subject_pattern(&subject_pattern) {
            return Err(ConsensusError::InvalidMessage(format!(
                "Invalid subject pattern '{subject_pattern}': {e}"
            )));
        }

        let request = MessagingRequest {
            operation: MessagingOperation::SubscribeToSubject {
                stream_name,
                subject_pattern,
            },
        };

        self.propose_request(&request)
    }

    /// Unsubscribe a stream from a subject pattern through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn unsubscribe_stream_from_subject(
        &self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::UnsubscribeFromSubject {
                stream_name: stream_name.into(),
                subject_pattern: subject_pattern.into(),
            },
        };

        self.propose_request(&request)
    }

    /// Remove all subject subscriptions for a stream through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn remove_stream_subscriptions(
        &self,
        stream_name: impl Into<String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::RemoveStreamSubscriptions {
                stream_name: stream_name.into(),
            },
        };

        self.propose_request(&request)
    }

    /// Get a message by sequence number from local store
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub fn get_message(&self, stream: &str, seq: u64) -> ConsensusResult<Option<Bytes>> {
        debug!("Getting message for stream {} sequence: {}", stream, seq);
        Ok(self.store.get_message(stream, seq))
    }

    /// Propose a message through consensus (alias for `publish_message`)
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn propose(&self, stream: String, data: Bytes) -> ConsensusResult<u64> {
        self.publish_message(stream, data)
    }

    /// Get the last sequence number for a stream
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub fn last_sequence(&self, stream: &str) -> ConsensusResult<u64> {
        Ok(self.store.last_sequence(stream))
    }

    /// Get configuration
    #[must_use]
    pub const fn config(&self) -> &ConsensusConfig {
        &self.config
    }

    /// Get node ID
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get node ID as string
    #[must_use]
    pub fn node_id_str(&self) -> String {
        self.node_id.clone()
    }

    /// Get the stream store
    #[must_use]
    pub const fn store(&self) -> &StreamStore {
        &self.store
    }

    /// Check if this node is the leader
    #[must_use]
    pub fn is_leader(&self) -> bool {
        let raft = self.raft.read();
        raft.as_ref().is_some_and(|raft| {
            let metrics_guard = raft.metrics();
            let metrics = metrics_guard.borrow();
            matches!(metrics.current_leader, Some(ref leader_id) if leader_id == &self.node_id)
        })
    }

    /// Get current Raft metrics
    #[must_use]
    pub fn metrics(&self) -> Option<openraft::RaftMetrics<TypeConfig>> {
        let raft = self.raft.read();
        raft.as_ref().map(|r| r.metrics().borrow().clone())
    }

    /// Propose a request through Raft consensus
    fn propose_request(&self, request: &MessagingRequest) -> ConsensusResult<u64> {
        let raft = {
            let raft_guard = self.raft.read();
            raft_guard
                .as_ref()
                .ok_or_else(|| ConsensusError::InvalidMessage("Raft not initialized".to_string()))?
                .clone()
        };

        debug!("Proposing request through Raft: {:?}", request);

        // Use tokio::task::block_in_place to handle async in sync context
        let response = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { raft.client_write(request.clone()).await })
        });

        match response {
            Ok(client_write_response) => {
                info!(
                    "Request successfully committed: sequence {}",
                    client_write_response.data.sequence
                );
                Ok(client_write_response.data.sequence)
            }
            Err(e) => {
                warn!("Failed to commit request through Raft: {}", e);
                Err(ConsensusError::InvalidMessage(format!(
                    "Consensus failed: {e}"
                )))
            }
        }
    }

    /// Initialize the cluster as a single-node cluster or join existing cluster
    ///
    /// # Errors
    ///
    /// Returns an error if cluster initialization fails.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn initialize_cluster(&self) -> ConsensusResult<()> {
        // Check if we're already in a cluster and get raft instance
        let (is_already_in_cluster, raft_instance) = {
            let raft_guard = self.raft.read();
            let raft = raft_guard.as_ref().ok_or_else(|| {
                ConsensusError::InvalidMessage("Raft not initialized".to_string())
            })?;

            let metrics = raft.metrics().borrow().clone();
            let membership = metrics.membership_config.membership();
            let already_in_cluster =
                membership.voter_ids().count() > 0 || membership.learner_ids().count() > 0;

            (already_in_cluster, raft.clone())
        }; // Drop the RwLock guard here

        if is_already_in_cluster {
            info!("Node {} already in cluster", self.node_id);
            return Ok(());
        }

        // Initialize as single-node cluster
        info!("Initializing new cluster with node {}", self.node_id);
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.node_id.clone(),
            openraft::BasicNode {
                addr: format!("node-{}", self.node_id),
            },
        );

        raft_instance.initialize(nodes).await.map_err(|e| {
            ConsensusError::InvalidMessage(format!("Failed to initialize cluster: {e}"))
        })?;

        info!("Cluster initialized successfully");
        Ok(())
    }

    /// Add a peer to the cluster as a learner
    ///
    /// # Errors
    ///
    /// Returns an error if adding the learner fails.
    pub async fn add_learner(&self, peer_id: String, peer_addr: String) -> ConsensusResult<()> {
        let raft_instance = {
            let raft = self.raft.read();
            raft.as_ref()
                .ok_or_else(|| ConsensusError::InvalidMessage("Raft not initialized".to_string()))?
                .clone()
        };

        info!("Adding peer {} as learner", peer_id);
        let node = openraft::BasicNode { addr: peer_addr };

        raft_instance
            .add_learner(peer_id.clone(), node, true)
            .await
            .map_err(|e| ConsensusError::InvalidMessage(format!("Failed to add learner: {e}")))?;

        info!("Peer {} added as learner", peer_id);
        Ok(())
    }

    /// Promote learners to voters
    ///
    /// # Errors
    ///
    /// Returns an error if membership change fails.
    pub async fn change_membership(&self, voters: BTreeSet<String>) -> ConsensusResult<()> {
        let raft_instance = {
            let raft = self.raft.read();
            raft.as_ref()
                .ok_or_else(|| ConsensusError::InvalidMessage("Raft not initialized".to_string()))?
                .clone()
        };

        info!("Changing membership to voters: {:?}", voters);

        raft_instance
            .change_membership(voters, false)
            .await
            .map_err(|e| {
                ConsensusError::InvalidMessage(format!("Failed to change membership: {e}"))
            })?;

        info!("Membership changed successfully");
        Ok(())
    }

    /// Remove a node from the cluster
    ///
    /// # Errors
    ///
    /// Returns an error if node removal fails.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn remove_node(&self, node_id: String) -> ConsensusResult<()> {
        let (new_voters, raft_instance) = {
            let raft_guard = self.raft.read();
            let raft = raft_guard.as_ref().ok_or_else(|| {
                ConsensusError::InvalidMessage("Raft not initialized".to_string())
            })?;

            // Get current membership and remove the node
            let metrics = raft.metrics().borrow().clone();
            let current_membership = metrics.membership_config.membership();

            let mut new_voters: BTreeSet<String> = current_membership.voter_ids().collect();
            new_voters.remove(&node_id);

            if new_voters.is_empty() {
                return Err(ConsensusError::InvalidMessage(
                    "Cannot remove last voter from cluster".to_string(),
                ));
            }

            (new_voters, raft.clone())
        };

        info!("Removing node {} from cluster", node_id);

        raft_instance
            .change_membership(new_voters, false)
            .await
            .map_err(|e| ConsensusError::InvalidMessage(format!("Failed to remove node: {e}")))?;

        info!("Node {} removed from cluster", node_id);
        Ok(())
    }

    /// Get current cluster membership information
    ///
    /// # Errors
    ///
    /// Returns an error if Raft is not initialized.
    #[allow(clippy::significant_drop_tightening)]
    pub fn get_cluster_membership(&self) -> ConsensusResult<(BTreeSet<String>, BTreeSet<String>)> {
        let (voters, learners) = {
            let raft_guard = self.raft.read();
            let raft = raft_guard.as_ref().ok_or_else(|| {
                ConsensusError::InvalidMessage("Raft not initialized".to_string())
            })?;

            let metrics = raft.metrics().borrow().clone();
            let membership = metrics.membership_config.membership();

            let voters: BTreeSet<String> = membership.voter_ids().collect();
            let learners: BTreeSet<String> = membership.learner_ids().collect();

            (voters, learners)
        };

        Ok((voters, learners))
    }

    /// Check if the cluster has a healthy quorum
    ///
    /// # Errors
    ///
    /// Returns an error if Raft is not initialized.
    #[allow(clippy::significant_drop_tightening)]
    pub fn has_quorum(&self) -> ConsensusResult<bool> {
        let has_leader = {
            let raft_guard = self.raft.read();
            let raft = raft_guard.as_ref().ok_or_else(|| {
                ConsensusError::InvalidMessage("Raft not initialized".to_string())
            })?;

            let metrics = raft.metrics().borrow().clone();
            metrics.current_leader.is_some()
        };

        // Check if we have a leader and if the cluster is functional
        // A cluster has quorum if there's a current leader
        Ok(has_leader)
    }

    /// Get all streams that should receive messages for a given subject
    ///
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn route_subject(&self, subject: &str) -> std::collections::HashSet<String> {
        self.store.route_subject(subject)
    }

    /// Get all subject patterns that a stream is subscribed to
    ///
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn get_stream_subjects(
        &self,
        stream_name: &str,
    ) -> Option<std::collections::HashSet<String>> {
        let router = self.store.subject_router.read();
        router.get_stream_subjects(stream_name).cloned()
    }

    /// Get all streams subscribed to a specific subject pattern
    ///
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn get_subject_streams(
        &self,
        subject_pattern: &str,
    ) -> Option<std::collections::HashSet<String>> {
        let router = self.store.subject_router.read();
        router.get_subject_streams(subject_pattern).cloned()
    }

    /// Get a summary of all current subject subscriptions
    ///
    /// Returns a map from subject patterns to the streams subscribed to them.
    /// This reads from the consensus-synchronized routing table.
    #[must_use]
    pub fn get_all_subscriptions(
        &self,
    ) -> std::collections::HashMap<String, std::collections::HashSet<String>> {
        let router = self.store.subject_router.read();
        router.get_subscriptions().clone()
    }

    /// Subscribe a stream to multiple subject patterns in one consensus operation
    ///
    /// This is more efficient than multiple individual subscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn bulk_subscribe_stream_to_subjects(
        &self,
        stream_name: impl Into<String>,
        subject_patterns: Vec<String>,
    ) -> ConsensusResult<u64> {
        let stream_name = stream_name.into();

        // Validate stream name
        if let Err(e) = validate_stream_name(&stream_name) {
            return Err(ConsensusError::InvalidMessage(format!(
                "Invalid stream name '{stream_name}': {e}"
            )));
        }

        // Validate all subject patterns
        for pattern in &subject_patterns {
            if let Err(e) = validate_subject_pattern(pattern) {
                return Err(ConsensusError::InvalidMessage(format!(
                    "Invalid subject pattern '{pattern}': {e}"
                )));
            }
        }

        let request = MessagingRequest {
            operation: MessagingOperation::BulkSubscribeToSubjects {
                stream_name,
                subject_patterns,
            },
        };

        self.propose_request(&request)
    }

    /// Unsubscribe a stream from multiple subject patterns in one consensus operation
    ///
    /// This is more efficient than multiple individual unsubscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub fn bulk_unsubscribe_stream_from_subjects(
        &self,
        stream_name: impl Into<String>,
        subject_patterns: Vec<String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::BulkUnsubscribeFromSubjects {
                stream_name: stream_name.into(),
                subject_patterns,
            },
        };

        self.propose_request(&request)
    }
}
