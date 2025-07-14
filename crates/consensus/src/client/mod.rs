//! Consensus client providing a clean API for consensus operations
//!
//! This module provides the primary interface for interacting with the consensus system.
//! It abstracts away the internal complexity and provides a simple, type-safe API.

// Submodules
pub mod consumer;
pub mod errors;
pub mod pubsub;
pub mod stream;
pub mod types;

// Re-exports
pub use consumer::{Consumer, ConsumerBuilder, ConsumerConfig};
pub use errors::{ClientError, ClientResult};
use proven_network::Transport;
pub use pubsub::{TypedMessage, TypedPublisher, TypedSubscription};
pub use stream::{Message, Stream, StreamInfo};
pub use types::MessageType;

use crate::{
    ConsensusGroupId, NodeId,
    config::StreamConfig,
    core::{engine::Engine, group::GroupStreamOperation},
    error::{ConsensusResult, Error},
    network::{ClusterState, messages::ClusterDiscoveryResponse},
    operations::{
        GlobalOperation, StreamManagementOperation,
        handlers::{GlobalOperationResponse, GroupStreamOperationResponse},
    },
    pubsub::PubSubManager,
};

use bytes::Bytes;
use proven_governance::Governance;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, info};

/// Client for interacting with the consensus system
///
/// This provides the main API for:
/// - Stream operations (create, delete, publish, subscribe)
/// - Cluster operations (join, discover, status)
/// - Admin operations (node management, group management)
/// - Monitoring and health checks
#[derive(Clone)]
pub struct ConsensusClient<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Proven engine
    engine: Arc<Engine<T, G>>,
    /// Node ID
    node_id: NodeId,
}

impl<T, G> ConsensusClient<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new consensus client
    pub(crate) fn new(engine: Arc<Engine<T, G>>, node_id: NodeId) -> Self {
        Self { engine, node_id }
    }

    // Stream Operations

    /// Create a new stream
    pub async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
        group_id: Option<ConsensusGroupId>,
    ) -> ConsensusResult<GlobalOperationResponse> {
        info!("Creating stream '{}' with config: {:?}", name, config);

        // Use the provided group_id or allocate a new one
        let target_group = match group_id {
            Some(id) => id,
            None => {
                // Auto-allocate to a group (for now, use initial group)
                ConsensusGroupId::initial()
            }
        };

        let operation = GlobalOperation::StreamManagement(StreamManagementOperation::Create {
            name: name.clone(),
            config,
            group_id: target_group,
        });

        self.engine.submit_global_request(operation).await
    }

    /// Delete a stream
    pub async fn delete_stream(&self, name: String) -> ConsensusResult<GlobalOperationResponse> {
        info!("Deleting stream '{}'", name);

        let operation =
            GlobalOperation::StreamManagement(StreamManagementOperation::Delete { name });

        self.engine.submit_global_request(operation).await
    }

    /// Publish data to a stream
    pub async fn publish(
        &self,
        stream: &str,
        data: Bytes,
        metadata: Option<HashMap<String, String>>,
    ) -> ConsensusResult<u64> {
        debug!("Publishing to stream '{}'", stream);

        let operation = GroupStreamOperation::publish_to_stream(stream.to_string(), data, metadata);
        let response = self
            .engine
            .route_stream_operation(stream, operation)
            .await?;

        if response.is_success() {
            Ok(response.sequence().unwrap_or(0))
        } else {
            Err(Error::InvalidOperation(
                response
                    .error()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "Failed to publish".to_string()),
            ))
        }
    }

    /// Subscribe to a stream (not directly supported - use routing operations instead)
    pub async fn subscribe(
        &self,
        stream: &str,
        subject: String,
        _start_sequence: Option<u64>,
    ) -> ConsensusResult<GlobalOperationResponse> {
        debug!("Subscribing stream '{}' to subject '{}'", stream, subject);

        let operation = GlobalOperation::Routing(crate::operations::RoutingOperation::Subscribe {
            stream_name: stream.to_string(),
            subject_pattern: subject,
        });

        self.engine.submit_global_request(operation).await
    }

    /// Get message from a stream
    pub async fn get_message(
        &self,
        stream: &str,
        sequence: u64,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        let operation = GroupStreamOperation::direct_get_from_stream(stream.to_string(), sequence);
        self.engine.route_stream_operation(stream, operation).await
    }

    /// Submit a global request directly
    pub async fn submit_global_request(
        &self,
        request: crate::core::global::GlobalRequest,
    ) -> ConsensusResult<GlobalOperationResponse> {
        self.engine.submit_global_request(request.operation).await
    }

    /// Get last sequence number for a stream
    pub async fn get_last_sequence(&self, stream: &str) -> ConsensusResult<u64> {
        // TODO: This needs a proper implementation once StreamManager exposes
        // a public method to get the last sequence or stream metadata.
        // For now, return 0 as a placeholder.

        // Verify the stream exists
        let _stream_info = self
            .engine
            .get_stream_info(stream)
            .await?
            .ok_or_else(|| Error::not_found(format!("Stream '{stream}' not found")))?;

        // Return 0 as placeholder - this should be implemented properly
        // when the stream manager API is updated
        Ok(0)
    }

    // Cluster Operations

    /// Get current cluster state
    pub async fn cluster_state(&self) -> ClusterState {
        self.engine.cluster_state().await
    }

    /// Check if we have an active cluster
    pub async fn has_active_cluster(&self) -> bool {
        self.engine.has_active_cluster().await
    }

    /// Discover existing clusters
    pub async fn discover_clusters(&self) -> ConsensusResult<Vec<ClusterDiscoveryResponse>> {
        self.engine.discover_existing_clusters().await
    }

    /// Join an existing cluster
    pub async fn join_cluster(
        &self,
        target_node: NodeId,
        _timeout: Duration,
    ) -> ConsensusResult<()> {
        info!("Attempting to join cluster via node {}", target_node);
        // This would need to be implemented in UnifiedConsensusManager
        // For now, return an error
        Err(Error::InvalidOperation(
            "Cluster join not yet implemented".to_string(),
        ))
    }

    /// Initialize as single-node cluster
    pub async fn initialize_single_node(&self) -> ConsensusResult<()> {
        info!("Initializing as single-node cluster");
        // This would need to be implemented in UnifiedConsensusManager
        // For now, return an error
        Err(Error::InvalidOperation(
            "Single node initialization not yet implemented".to_string(),
        ))
    }

    // Admin Operations

    /// Check if this node is the global leader
    pub async fn is_leader(&self) -> bool {
        self.engine.is_leader().await
    }

    /// Get current leader
    pub async fn current_leader(&self) -> Option<String> {
        self.engine.current_leader().await
    }

    /// Get cluster size
    pub async fn cluster_size(&self) -> Option<usize> {
        self.engine.cluster_size().await
    }

    /// Get current term
    pub async fn current_term(&self) -> Option<u64> {
        self.engine.current_term().await
    }

    // Group Management

    /// Create a new consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<GlobalOperationResponse> {
        let operation = GlobalOperation::Group(crate::operations::GroupOperation::Create {
            group_id,
            initial_members: members,
        });

        self.engine.submit_global_request(operation).await
    }

    /// Delete a consensus group
    pub async fn delete_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        let operation =
            GlobalOperation::Group(crate::operations::GroupOperation::Delete { group_id });

        self.engine.submit_global_request(operation).await
    }

    /// Get groups this node is part of
    pub async fn get_my_groups(&self) -> HashMap<ConsensusGroupId, Vec<NodeId>> {
        // Get groups with migration states from manager
        let groups_with_states = self.engine.get_my_groups().await;

        // For each group, get the actual member list
        let mut result = HashMap::new();
        for (group_id, _state) in groups_with_states {
            if let Ok(Some(group_info)) = self.engine.get_group_info(group_id).await {
                result.insert(group_id, group_info.members);
            }
        }
        result
    }

    /// Check if node is leader of a specific group
    pub async fn is_group_leader(&self, group_id: ConsensusGroupId) -> ConsensusResult<bool> {
        self.engine.is_group_leader(group_id).await
    }

    // Stream Information

    /// Check if a stream exists
    pub async fn stream_exists(&self, stream: &str) -> bool {
        self.engine.stream_exists(stream).await
    }

    /// Get the consensus group for a stream
    pub async fn get_stream_group(&self, stream: &str) -> ConsensusResult<ConsensusGroupId> {
        self.engine.get_stream_group(stream).await
    }

    /// Get all streams in a group
    pub async fn get_streams_in_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Vec<String>> {
        self.engine.get_streams_in_group(group_id).await
    }

    /// Get stream assignment summary
    pub async fn get_stream_assignments(
        &self,
    ) -> ConsensusResult<HashMap<ConsensusGroupId, Vec<String>>> {
        self.engine.get_stream_assignments().await
    }

    /// Get stream routing information (for debugging)
    pub async fn get_stream_routing_info(
        &self,
        stream: &str,
    ) -> ConsensusResult<crate::core::engine::routing::StreamRoutingInfo> {
        self.engine.get_stream_routing_info(stream).await
    }

    // Health and Monitoring

    /// Get global consensus metrics
    pub async fn global_metrics(
        &self,
    ) -> openraft::RaftMetrics<crate::core::global::GlobalConsensusTypeConfig> {
        self.engine.global_metrics().await
    }

    /// Get metrics for all consensus groups
    pub async fn group_metrics(
        &self,
    ) -> HashMap<
        ConsensusGroupId,
        openraft::RaftMetrics<crate::core::group::GroupConsensusTypeConfig>,
    > {
        self.engine.get_all_group_metrics().await
    }

    /// Wait for leader election (with optional timeout)
    pub async fn wait_for_leader(&self, timeout: Option<Duration>) -> ConsensusResult<()> {
        self.engine.wait_for_leader(timeout).await
    }

    /// Get node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    // Type-safe Stream Operations

    /// Get a typed stream handle
    ///
    /// Returns a `Stream<T>` that can be used to publish and read typed messages.
    /// Returns an error if the stream doesn't exist.
    pub async fn get_stream<M>(&self, name: &str) -> ClientResult<Stream<M, T, G>>
    where
        M: MessageType + Send + Sync + 'static,
    {
        // Verify stream exists and get its info
        let stream_info = self
            .engine
            .get_stream_info(name)
            .await
            .map_err(ClientError::Consensus)?
            .ok_or_else(|| ClientError::StreamNotFound(name.to_string()))?;

        Ok(Stream::<M, T, G>::new(
            name.to_string(),
            stream_info.config,
            self.engine.clone(),
        ))
    }

    /// Create a new typed stream
    ///
    /// Creates a stream and returns a typed handle to it.
    pub async fn create_stream_typed<M>(
        &self,
        name: &str,
        config: StreamConfig,
        group_id: Option<ConsensusGroupId>,
    ) -> ClientResult<Stream<M, T, G>>
    where
        M: MessageType,
    {
        // Use existing create_stream method
        let response = self
            .create_stream(name.to_string(), config.clone(), group_id)
            .await
            .map_err(ClientError::Consensus)?;

        if !response.is_success() {
            return Err(ClientError::Internal(
                response
                    .error()
                    .unwrap_or("Failed to create stream")
                    .to_string(),
            ));
        }

        Ok(Stream::<M, T, G>::new(
            name.to_string(),
            config,
            self.engine.clone(),
        ))
    }

    /// Get or create a typed stream
    ///
    /// If the stream exists, returns a handle to it.
    /// If it doesn't exist, creates it with the given config.
    pub async fn get_or_create_stream<M>(
        &self,
        name: &str,
        config: StreamConfig,
        group_id: Option<ConsensusGroupId>,
    ) -> ClientResult<Stream<M, T, G>>
    where
        M: MessageType,
    {
        // Try to get existing stream first
        match self.get_stream::<M>(name).await {
            Ok(stream) => Ok(stream),
            Err(ClientError::StreamNotFound(_)) => {
                // Stream doesn't exist, create it
                self.create_stream_typed::<M>(name, config, group_id).await
            }
            Err(e) => Err(e),
        }
    }

    // Type-safe PubSub Operations

    /// Create a typed publisher
    ///
    /// Returns a `TypedPublisher<T>` that can publish typed messages.
    pub fn publisher<M>(&self) -> TypedPublisher<M, T, G>
    where
        M: MessageType,
        G: Governance,
    {
        TypedPublisher::new(self.get_pubsub_manager())
    }

    /// Subscribe to a subject pattern with typed messages
    ///
    /// Returns a `TypedSubscription<T>` that yields typed messages.
    pub async fn typed_subscribe<M>(
        &self,
        pattern: &str,
    ) -> ClientResult<TypedSubscription<M, T, G>>
    where
        M: MessageType + Send + Sync + 'static,
        G: Governance,
    {
        let subscription = self
            .get_pubsub_manager()
            .subscribe(pattern)
            .await
            .map_err(ClientError::PubSub)?;

        Ok(TypedSubscription::new(subscription))
    }

    /// Get access to the PubSub manager
    ///
    /// This provides direct access for advanced use cases.
    pub(crate) fn get_pubsub_manager(&self) -> Arc<PubSubManager<T, G>>
    where
        T: Transport,
        G: Governance,
    {
        self.engine.pubsub_manager()
    }

    // Cluster State Operations

    /// Check if the cluster has been formed
    ///
    /// Returns true if this node has either:
    /// - Initiated a new cluster as the first node
    /// - Successfully joined an existing cluster
    pub async fn is_cluster_formed(&self) -> bool {
        matches!(
            self.engine.cluster_state().await,
            ClusterState::Initiator { .. } | ClusterState::Joined { .. }
        )
    }

    /// Get the current cluster state
    ///
    /// Returns detailed information about the cluster membership state
    pub async fn get_cluster_state(&self) -> ClusterState {
        self.engine.cluster_state().await
    }

    /// Get the list of current cluster members
    ///
    /// Returns a list of NodeIds for all nodes currently in the cluster
    pub async fn get_cluster_members(&self) -> Vec<NodeId> {
        let metrics = self.engine.global_metrics().await;
        metrics
            .membership_config
            .nodes()
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get the current cluster leader
    ///
    /// Returns the NodeId of the current leader, if one exists
    pub async fn get_cluster_leader(&self) -> Option<NodeId> {
        let metrics = self.engine.global_metrics().await;
        metrics.current_leader
    }

    /// Get the current cluster size
    ///
    /// Returns the number of voting members in the cluster
    pub async fn get_cluster_size(&self) -> usize {
        let metrics = self.engine.global_metrics().await;
        metrics.membership_config.voter_ids().count()
    }

    /// Check if cluster discovery is in progress
    ///
    /// Returns true if the node is still discovering existing clusters
    pub async fn is_discovering(&self) -> bool {
        self.engine.is_cluster_discovering().await
    }
}
