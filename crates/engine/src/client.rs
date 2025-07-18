//! Client API for interacting with the consensus engine
//!
//! This module provides a clean public API for submitting operations
//! and querying the consensus system.

use std::sync::Arc;

use proven_storage::StorageAdaptor;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::error::ConsensusError;
use crate::{
    consensus::{
        global::{GlobalRequest, GlobalResponse},
        group::{GroupRequest, GroupResponse, StreamOperation},
    },
    error::ConsensusResult,
    foundation::types::ConsensusGroupId,
    services::client::{ClientService, GroupInfo, StreamInfo},
    services::stream::{MessageData, StreamConfig},
};

/// Client for interacting with the consensus engine
///
/// This provides the main API for:
/// - Stream operations (create, delete, publish)
/// - Group operations (create, delete, query)
/// - Cluster operations (query status)
pub struct Client<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Reference to the client service
    client_service: Arc<ClientService<T, G, S>>,
    /// Node ID for reference
    node_id: NodeId,
}

impl<T, G, S> Client<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new client
    pub(crate) fn new(client_service: Arc<ClientService<T, G, S>>, node_id: NodeId) -> Self {
        Self {
            client_service,
            node_id,
        }
    }

    // Stream Operations

    /// Create a new stream with automatic group assignment
    ///
    /// This method automatically selects an appropriate group for the stream
    /// based on the current node's group membership and load balancing.
    pub async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
    ) -> ConsensusResult<GlobalResponse> {
        // Get a suitable group for this node
        let group_id = self.client_service.get_suitable_group().await?;

        let request = GlobalRequest::CreateStream {
            name: name.into(),
            config,
            group_id,
        };
        self.client_service.submit_global_request(request).await
    }

    /// Delete a stream
    pub async fn delete_stream(&self, name: String) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::DeleteStream { name: name.into() };
        self.client_service.submit_global_request(request).await
    }

    /// Publish a message to a stream
    pub async fn publish(
        &self,
        stream: String,
        payload: Vec<u8>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> ConsensusResult<GroupResponse> {
        // First, get stream info to find the group
        let stream_info = self
            .client_service
            .get_stream_info(&stream)
            .await?
            .ok_or_else(|| {
                crate::error::ConsensusError::not_found(format!("Stream '{stream}' not found"))
            })?;

        // Create message data
        let message = MessageData {
            payload: payload.into(),
            headers: metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
            key: None,
        };

        // Submit to the group that owns the stream
        let request = GroupRequest::Stream(StreamOperation::Append {
            stream: stream.into(),
            message,
        });

        self.client_service
            .submit_group_request(stream_info.group_id, request)
            .await
    }

    /// Get stream information
    pub async fn get_stream_info(&self, name: &str) -> ConsensusResult<Option<StreamInfo>> {
        self.client_service.get_stream_info(name).await
    }

    // Group Operations

    /// Create a new consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::CreateGroup {
            info: crate::consensus::global::types::GroupInfo {
                id: group_id,
                members,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                metadata: Default::default(),
            },
        };
        self.client_service.submit_global_request(request).await
    }

    /// Delete a consensus group
    pub async fn delete_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::DissolveGroup { id: group_id };
        self.client_service.submit_global_request(request).await
    }

    /// Get group information
    pub async fn get_group_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupInfo>> {
        self.client_service.get_group_info(group_id).await
    }

    // Node Operations

    /// Add a node to the cluster
    pub async fn add_node(&self, node_id: NodeId) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::AddNode {
            node_id,
            metadata: Default::default(),
        };
        self.client_service.submit_global_request(request).await
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::RemoveNode { node_id };
        self.client_service.submit_global_request(request).await
    }

    /// Get the node ID of this client
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Read messages from a stream
    pub async fn read_stream(
        &self,
        stream_name: String,
        start_sequence: u64,
        count: u64,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
        self.client_service
            .read_stream(&stream_name, start_sequence, count)
            .await
    }

    /// Delete a message from a stream
    pub async fn delete_message(
        &self,
        stream_name: String,
        sequence: u64,
    ) -> ConsensusResult<GroupResponse> {
        // Get stream info to find which group owns it
        let stream_info = self.get_stream_info(&stream_name).await?.ok_or_else(|| {
            ConsensusError::with_context(
                crate::error::ErrorKind::NotFound,
                format!("Stream '{stream_name}' not found"),
            )
        })?;

        // Submit delete operation to the group that owns the stream
        let request = GroupRequest::Stream(StreamOperation::Delete {
            stream: stream_name.into(),
            sequence,
        });

        self.client_service
            .submit_group_request(stream_info.group_id, request)
            .await
    }
}

impl<T, G, S> Clone for Client<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            client_service: self.client_service.clone(),
            node_id: self.node_id.clone(),
        }
    }
}
