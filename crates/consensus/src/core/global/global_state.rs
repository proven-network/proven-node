//! Global consensus state management
//!
//! This module contains the pure state container for global consensus operations.
//! All validation is handled by operation validators, and the state machine
//! only applies pre-validated operations.

use crate::{
    ConsensusGroupId, NodeId,
    config::StreamConfig,
    pubsub::{
        SubjectRouter, subject_matches_pattern,
        subscription::{SubscriptionHandlerMap, SubscriptionInvoker},
    },
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Callback type for stream creation notifications
type StreamCreationCallback = Arc<dyn Fn(String, StreamConfig, ConsensusGroupId) + Send + Sync>;

/// Global consensus state machine
///
/// This is a pure state container that applies pre-validated operations.
/// All validation should be done by operation validators before operations
/// reach this state machine.
#[derive(Clone)]
pub struct GlobalState {
    /// Stream data storage
    pub(crate) streams: Arc<RwLock<HashMap<String, StreamData>>>,
    /// Subject router for advanced pattern matching
    pub(crate) subject_router: Arc<RwLock<SubjectRouter>>,
    /// Active subscription handlers organized by subject pattern
    pub(crate) subscription_handlers: SubscriptionHandlerMap,
    /// Stream configurations
    pub(crate) stream_configs: Arc<RwLock<HashMap<String, StreamConfig>>>,
    /// Consensus groups and their members
    pub(crate) consensus_groups: Arc<RwLock<HashMap<ConsensusGroupId, ConsensusGroupInfo>>>,
    /// Stream creation notification callback
    pub(crate) stream_creation_callback: Arc<RwLock<Option<StreamCreationCallback>>>,
}

/// Information about a consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusGroupInfo {
    /// Group identifier
    pub id: ConsensusGroupId,
    /// Member node IDs
    pub members: Vec<NodeId>,
    /// When the group was created
    pub created_at: u64,
    /// Number of streams allocated to this group
    pub stream_count: usize,
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
    pub node_id: Option<NodeId>,
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

impl Default for GlobalState {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalState {
    /// Create a new global state
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            subject_router: Arc::new(RwLock::new(SubjectRouter::new())),
            subscription_handlers: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            stream_configs: Arc::new(RwLock::new(HashMap::new())),
            consensus_groups: Arc::new(RwLock::new(HashMap::new())),
            stream_creation_callback: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the stream creation callback
    pub async fn set_stream_creation_callback<F>(&self, callback: F)
    where
        F: Fn(String, StreamConfig, ConsensusGroupId) + Send + Sync + 'static,
    {
        let mut cb = self.stream_creation_callback.write().await;
        *cb = Some(Arc::new(callback));
    }

    // NOTE: The apply_operation methods have been replaced by the operation handler system.
    // Operations are now processed through the OperationHandlerRegistry which provides
    // better separation of concerns and extensibility.

    // Read-only accessor methods

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

    /// Get consensus groups that a node belongs to
    pub async fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupInfo> {
        let groups = self.consensus_groups.read().await;
        groups
            .values()
            .filter(|group| group.members.contains(node_id))
            .cloned()
            .collect()
    }

    /// Get all consensus groups
    pub async fn get_all_groups(&self) -> Vec<ConsensusGroupInfo> {
        let groups = self.consensus_groups.read().await;
        groups.values().cloned().collect()
    }

    /// Get a specific consensus group
    pub async fn get_group(&self, group_id: ConsensusGroupId) -> Option<ConsensusGroupInfo> {
        let groups = self.consensus_groups.read().await;
        groups.get(&group_id).cloned()
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

    /// Get stream info including its consensus group
    pub async fn get_stream(&self, stream_name: &str) -> Option<StreamInfo> {
        let configs = self.stream_configs.read().await;
        configs.get(stream_name).map(|config| StreamInfo {
            name: stream_name.to_string(),
            group_id: config.consensus_group.unwrap_or_default(),
            config: config.clone(),
        })
    }

    /// Get all streams in a consensus group
    pub async fn get_streams_in_group(&self, group_id: ConsensusGroupId) -> Vec<String> {
        let configs = self.stream_configs.read().await;
        configs
            .iter()
            .filter_map(|(name, config)| {
                if config.consensus_group == Some(group_id) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Stream information including consensus group
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Stream name
    pub name: String,
    /// Consensus group ID
    pub group_id: ConsensusGroupId,
    /// Stream configuration
    pub config: StreamConfig,
}
