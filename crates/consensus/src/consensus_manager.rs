//! OpenRaft-based consensus implementation for messaging

use std::collections::{BTreeMap, HashMap, HashSet};
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
        NodeId = u64,
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
    /// Node identifier
    node_id: u64,
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
        // Parse node_id as u64 for OpenRaft
        let node_id_u64 = node_id.parse::<u64>().unwrap_or(1);

        info!(
            "Creating OpenRaft consensus manager for node: {}",
            node_id_u64
        );

        Self {
            node_id: node_id_u64,
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
            self.node_id,
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
        network_tx: tokio::sync::mpsc::UnboundedSender<(u64, crate::network::ConsensusMessage)>,
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
    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get node ID as string
    #[must_use]
    pub fn node_id_str(&self) -> String {
        self.node_id.to_string()
    }

    /// Get the stream store
    #[must_use]
    pub const fn store(&self) -> &StreamStore {
        &self.store
    }

    /// Check if this node is the leader (currently always false - no Raft)
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        // TODO: Implement with actual Raft
        false
    }

    /// Get current Raft metrics (currently returns None - no Raft)
    #[must_use]
    pub const fn metrics(&self) -> Option<openraft::RaftMetrics<TypeConfig>> {
        // TODO: Implement with actual Raft
        None
    }

    /// Propose a request through Raft consensus
    fn propose_request(&self, request: &MessagingRequest) -> ConsensusResult<u64> {
        // For now, bypass Raft and apply directly to local store
        // This is a temporary workaround until full Raft integration is implemented
        debug!(
            "Applying request directly to local store (Raft bypass): {:?}",
            request
        );

        let response = self.store.apply_request(request)?;
        if response.success {
            Ok(response.sequence)
        } else {
            Err(ConsensusError::InvalidMessage(
                response
                    .error
                    .unwrap_or_else(|| "Request failed".to_string()),
            ))
        }
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
