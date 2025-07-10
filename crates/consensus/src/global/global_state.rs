//! Global consensus state management
//!
//! This module contains the pure state container for global consensus operations.
//! All validation is handled by operation validators, and the state machine
//! only applies pre-validated operations.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use super::{GlobalResponse, StreamConfig};
use crate::{
    NodeId,
    allocation::ConsensusGroupId,
    operations::{
        GlobalOperation, group_ops::GroupOperation, node_ops::NodeOperation,
        routing_ops::RoutingOperation, stream_management_ops::StreamManagementOperation,
    },
    pubsub::{SubjectRouter, subject_matches_pattern},
    subscription::{SubscriptionHandlerMap, SubscriptionInvoker},
};

/// Global consensus state machine
///
/// This is a pure state container that applies pre-validated operations.
/// All validation should be done by operation validators before operations
/// reach this state machine.
#[derive(Debug, Clone)]
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
        }
    }

    /// Apply a global operation to the state machine
    ///
    /// NOTE: This assumes the operation has already been validated.
    /// All validation should be done by operation validators.
    pub async fn apply_operation(
        &self,
        operation: &GlobalOperation,
        sequence: u64,
    ) -> GlobalResponse {
        match operation {
            GlobalOperation::StreamManagement(op) => {
                self.apply_stream_operation(op, sequence).await
            }
            GlobalOperation::Group(op) => self.apply_group_operation(op, sequence).await,
            GlobalOperation::Node(op) => self.apply_node_operation(op, sequence).await,
            GlobalOperation::Routing(op) => self.apply_routing_operation(op, sequence).await,
        }
    }

    /// Apply a stream management operation
    async fn apply_stream_operation(
        &self,
        operation: &StreamManagementOperation,
        sequence: u64,
    ) -> GlobalResponse {
        match operation {
            StreamManagementOperation::Create {
                name,
                config,
                group_id,
            } => {
                let mut configs = self.stream_configs.write().await;
                let mut streams = self.streams.write().await;
                let mut groups = self.consensus_groups.write().await;

                // Create stream configuration with the assigned group
                let mut stream_config = config.clone();
                stream_config.consensus_group = Some(*group_id);
                configs.insert(name.clone(), stream_config);

                // Initialize stream data
                streams.entry(name.clone()).or_insert_with(|| StreamData {
                    messages: BTreeMap::new(),
                    next_sequence: 1,
                    subscriptions: HashSet::new(),
                });

                // Update group stream count
                if let Some(group) = groups.get_mut(group_id) {
                    group.stream_count += 1;
                }

                debug!("Created stream '{}' in group {:?}", name, group_id);

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            StreamManagementOperation::UpdateConfig { name, config } => {
                let mut configs = self.stream_configs.write().await;

                if let Some(existing_config) = configs.get_mut(name) {
                    // Preserve group allocation
                    let group_id = existing_config.consensus_group;
                    *existing_config = config.clone();
                    existing_config.consensus_group = group_id;

                    debug!("Updated configuration for stream '{}'", name);

                    GlobalResponse {
                        success: true,
                        sequence,
                        error: None,
                    }
                } else {
                    GlobalResponse {
                        success: false,
                        sequence,
                        error: Some(format!("Stream '{}' not found", name)),
                    }
                }
            }

            StreamManagementOperation::Delete { name } => {
                let mut configs = self.stream_configs.write().await;
                let mut streams = self.streams.write().await;
                let mut router = self.subject_router.write().await;
                let mut groups = self.consensus_groups.write().await;

                // Get the group before deletion
                let group_id = configs.get(name).and_then(|c| c.consensus_group);

                // Remove configuration
                configs.remove(name);
                streams.remove(name);
                router.remove_stream(name);

                // Update group stream count
                if let Some(gid) = group_id {
                    if let Some(group) = groups.get_mut(&gid) {
                        group.stream_count = group.stream_count.saturating_sub(1);
                    }
                }

                debug!("Deleted stream '{}'", name);

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            StreamManagementOperation::Reallocate { name, target_group } => {
                let mut configs = self.stream_configs.write().await;
                let mut groups = self.consensus_groups.write().await;

                if let Some(config) = configs.get_mut(name) {
                    // Update group stream counts
                    if let Some(old_group_id) = config.consensus_group {
                        if let Some(old_group) = groups.get_mut(&old_group_id) {
                            old_group.stream_count = old_group.stream_count.saturating_sub(1);
                        }
                    }

                    if let Some(new_group) = groups.get_mut(target_group) {
                        new_group.stream_count += 1;
                    }

                    config.consensus_group = Some(*target_group);

                    debug!("Reallocated stream '{}' to group {:?}", name, target_group);

                    GlobalResponse {
                        success: true,
                        sequence,
                        error: None,
                    }
                } else {
                    GlobalResponse {
                        success: false,
                        sequence,
                        error: Some(format!("Stream '{}' not found", name)),
                    }
                }
            }

            StreamManagementOperation::Migrate {
                name,
                from_group: _,
                to_group: _,
                state,
            } => {
                // Migration state tracking would be handled by a separate component
                debug!(
                    "Recording migration state for stream '{}': {:?}",
                    name, state
                );

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            StreamManagementOperation::UpdateAllocation { name, new_group } => {
                // This is similar to Reallocate - inline the logic to avoid recursion
                let mut configs = self.stream_configs.write().await;
                let mut groups = self.consensus_groups.write().await;

                if let Some(config) = configs.get_mut(name) {
                    // Update group stream counts
                    if let Some(old_group_id) = config.consensus_group {
                        if let Some(old_group) = groups.get_mut(&old_group_id) {
                            old_group.stream_count = old_group.stream_count.saturating_sub(1);
                        }
                    }

                    if let Some(new_group_obj) = groups.get_mut(new_group) {
                        new_group_obj.stream_count += 1;
                    }

                    config.consensus_group = Some(*new_group);

                    debug!(
                        "Updated allocation for stream '{}' to group {:?}",
                        name, new_group
                    );

                    GlobalResponse {
                        success: true,
                        sequence,
                        error: None,
                    }
                } else {
                    GlobalResponse {
                        success: false,
                        sequence,
                        error: Some(format!("Stream '{}' not found", name)),
                    }
                }
            }
        }
    }

    /// Apply a group operation
    async fn apply_group_operation(
        &self,
        operation: &GroupOperation,
        sequence: u64,
    ) -> GlobalResponse {
        match operation {
            GroupOperation::Create {
                group_id,
                initial_members,
            } => {
                let mut groups = self.consensus_groups.write().await;

                let group_info = ConsensusGroupInfo {
                    id: *group_id,
                    members: initial_members.clone(),
                    created_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    stream_count: 0,
                };

                groups.insert(*group_id, group_info);

                debug!(
                    "Created consensus group {:?} with {} members",
                    group_id,
                    initial_members.len()
                );

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            GroupOperation::Delete { group_id } => {
                let mut groups = self.consensus_groups.write().await;
                groups.remove(group_id);

                debug!("Deleted consensus group {:?}", group_id);

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            GroupOperation::UpdateMembers { group_id, members } => {
                let mut groups = self.consensus_groups.write().await;

                if let Some(group) = groups.get_mut(group_id) {
                    group.members = members.clone();

                    debug!("Updated members for group {:?}", group_id);

                    GlobalResponse {
                        success: true,
                        sequence,
                        error: None,
                    }
                } else {
                    GlobalResponse {
                        success: false,
                        sequence,
                        error: Some(format!("Group {:?} not found", group_id)),
                    }
                }
            }
        }
    }

    /// Apply a node operation
    async fn apply_node_operation(
        &self,
        operation: &NodeOperation,
        sequence: u64,
    ) -> GlobalResponse {
        match operation {
            NodeOperation::AssignToGroup { node_id, group_id } => {
                let mut groups = self.consensus_groups.write().await;

                if let Some(group) = groups.get_mut(group_id) {
                    if !group.members.contains(node_id) {
                        group.members.push(node_id.clone());
                    }

                    debug!("Assigned node {:?} to group {:?}", node_id, group_id);

                    GlobalResponse {
                        success: true,
                        sequence,
                        error: None,
                    }
                } else {
                    GlobalResponse {
                        success: false,
                        sequence,
                        error: Some(format!("Group {:?} not found", group_id)),
                    }
                }
            }

            NodeOperation::RemoveFromGroup { node_id, group_id } => {
                let mut groups = self.consensus_groups.write().await;

                if let Some(group) = groups.get_mut(group_id) {
                    group.members.retain(|id| id != node_id);

                    debug!("Removed node {:?} from group {:?}", node_id, group_id);

                    GlobalResponse {
                        success: true,
                        sequence,
                        error: None,
                    }
                } else {
                    GlobalResponse {
                        success: false,
                        sequence,
                        error: Some(format!("Group {:?} not found", group_id)),
                    }
                }
            }

            NodeOperation::UpdateGroups { node_id, group_ids } => {
                let mut groups = self.consensus_groups.write().await;

                // Remove node from all groups
                for group in groups.values_mut() {
                    group.members.retain(|id| id != node_id);
                }

                // Add to specified groups
                for group_id in group_ids {
                    if let Some(group) = groups.get_mut(group_id) {
                        if !group.members.contains(node_id) {
                            group.members.push(node_id.clone());
                        }
                    }
                }

                debug!("Updated node {:?} group assignments", node_id);

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            NodeOperation::Decommission { node_id } => {
                let mut groups = self.consensus_groups.write().await;

                // Remove node from all groups
                for group in groups.values_mut() {
                    group.members.retain(|id| id != node_id);
                }

                debug!("Decommissioned node {:?}", node_id);

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }
        }
    }

    /// Apply a routing operation
    async fn apply_routing_operation(
        &self,
        operation: &RoutingOperation,
        sequence: u64,
    ) -> GlobalResponse {
        match operation {
            RoutingOperation::Subscribe {
                stream_name,
                subject_pattern,
            } => {
                let mut streams = self.streams.write().await;
                let mut router = self.subject_router.write().await;

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
                router.subscribe_stream(stream_name, subject_pattern);

                debug!(
                    "Stream '{}' subscribed to '{}'",
                    stream_name, subject_pattern
                );

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            RoutingOperation::Unsubscribe {
                stream_name,
                subject_pattern,
            } => {
                let mut streams = self.streams.write().await;
                let mut router = self.subject_router.write().await;

                if let Some(stream_data) = streams.get_mut(stream_name) {
                    stream_data.subscriptions.remove(subject_pattern);
                }

                router.unsubscribe_stream(stream_name, subject_pattern);

                debug!(
                    "Stream '{}' unsubscribed from '{}'",
                    stream_name, subject_pattern
                );

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            RoutingOperation::RemoveAllSubscriptions { stream_name } => {
                let mut streams = self.streams.write().await;
                let mut router = self.subject_router.write().await;

                if let Some(stream_data) = streams.get_mut(stream_name) {
                    stream_data.subscriptions.clear();
                }

                router.remove_stream(stream_name);

                debug!("Removed all subscriptions for stream '{}'", stream_name);

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            RoutingOperation::BulkSubscribe {
                stream_name,
                subject_patterns,
            } => {
                let mut streams = self.streams.write().await;
                let mut router = self.subject_router.write().await;

                let stream_data =
                    streams
                        .entry(stream_name.to_string())
                        .or_insert_with(|| StreamData {
                            messages: BTreeMap::new(),
                            next_sequence: 1,
                            subscriptions: HashSet::new(),
                        });

                for pattern in subject_patterns {
                    stream_data.subscriptions.insert(pattern.clone());
                    router.subscribe_stream(stream_name, pattern);
                    debug!("Stream '{}' subscribed to '{}'", stream_name, pattern);
                }

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }

            RoutingOperation::BulkUnsubscribe {
                stream_name,
                subject_patterns,
            } => {
                let mut streams = self.streams.write().await;
                let mut router = self.subject_router.write().await;

                if let Some(stream_data) = streams.get_mut(stream_name) {
                    for pattern in subject_patterns {
                        stream_data.subscriptions.remove(pattern);
                        router.unsubscribe_stream(stream_name, pattern);
                        debug!("Stream '{}' unsubscribed from '{}'", stream_name, pattern);
                    }
                }

                GlobalResponse {
                    success: true,
                    sequence,
                    error: None,
                }
            }
        }
    }

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
}
