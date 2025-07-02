//! OpenRaft-based consensus implementation for messaging

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::Config;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::{ConsensusError, ConsensusResult};

/// Messaging request for consensus operations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagingRequest {
    /// Stream name
    pub stream: String,
    /// Operation type
    pub operation: MessagingOperation,
}

/// Types of messaging operations that can be performed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessagingOperation {
    /// Publish a single message
    Publish {
        /// Message data to publish
        data: Bytes,
    },
    /// Publish multiple messages as a batch
    PublishBatch {
        /// List of messages to publish
        messages: Vec<Bytes>,
    },
    /// Rollup operation (replace all previous messages)
    Rollup {
        /// New rollup data
        data: Bytes,
        /// Expected current sequence number
        expected_seq: u64,
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
}

impl Default for StreamStore {
    fn default() -> Self {
        Self {
            messages: Arc::new(RwLock::new(BTreeMap::new())),
            sequences: Arc::new(RwLock::new(BTreeMap::new())),
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
    pub fn apply_request(&self, request: &MessagingRequest) -> ConsensusResult<MessagingResponse> {
        let mut messages = self.messages.write();
        let mut sequences = self.sequences.write();

        let stream_messages = messages.entry(request.stream.clone()).or_default();
        let current_seq = sequences.entry(request.stream.clone()).or_insert(0);

        match &request.operation {
            MessagingOperation::Publish { data } => {
                *current_seq += 1;
                let seq = *current_seq;
                stream_messages.insert(seq, data.clone());

                debug!(
                    "Applied publish to stream {} at sequence {}",
                    request.stream, seq
                );
                Ok(MessagingResponse {
                    sequence: seq,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::PublishBatch { messages: batch } => {
                let start_seq = *current_seq;
                for data in batch {
                    *current_seq += 1;
                    stream_messages.insert(*current_seq, data.clone());
                }

                debug!(
                    "Applied batch publish to stream {} sequences {}-{}",
                    request.stream,
                    start_seq + 1,
                    *current_seq
                );
                Ok(MessagingResponse {
                    sequence: *current_seq,
                    success: true,
                    error: None,
                })
            }
            MessagingOperation::Rollup { data, expected_seq } => {
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

                debug!(
                    "Applied rollup to stream {} at sequence {}",
                    request.stream, seq
                );
                Ok(MessagingResponse {
                    sequence: seq,
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
    ) -> ConsensusResult<()> {
        use crate::raft_network::ConsensusRaftNetworkFactory;
        use crate::raft_state_machine::ConsensusStateMachine;

        // Create the state machine with our stream store
        let state_machine = ConsensusStateMachine::new(Arc::new(self.store.clone()));

        // Create the network factory
        let network_factory = ConsensusRaftNetworkFactory::new(network);

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
            stream,
            operation: MessagingOperation::Publish { data },
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
            stream,
            operation: MessagingOperation::PublishBatch { messages },
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
            stream,
            operation: MessagingOperation::Rollup { data, expected_seq },
        };

        self.propose_request(&request)
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
        debug!("Getting last sequence for stream: {}", stream);
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
}
