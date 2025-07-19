//! Callback dispatcher for group consensus events

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{error::ConsensusResult, foundation::types::ConsensusGroupId};
use proven_topology::NodeId;

use super::{
    callbacks::GroupConsensusCallbacks,
    state::GroupState,
    types::{AdminOperation, GroupRequest, GroupResponse, MessageData, StreamOperation},
};

/// Dispatches callbacks based on operation results
pub struct GroupCallbackDispatcher {
    callbacks: Arc<dyn GroupConsensusCallbacks>,
}

impl GroupCallbackDispatcher {
    /// Create a new callback dispatcher with required callbacks
    pub fn new(callbacks: Arc<dyn GroupConsensusCallbacks>) -> Self {
        Self { callbacks }
    }

    /// Handle state synchronization after replay completes
    pub async fn dispatch_state_sync(&self, group_id: ConsensusGroupId, state: &GroupState) {
        if let Err(e) = self.callbacks.on_state_synchronized(group_id, state).await {
            tracing::error!("State sync callback failed for group {:?}: {}", group_id, e);
        } else {
            tracing::info!(
                "State synchronized for group {:?} - replay complete",
                group_id
            );
        }
    }

    /// Handle membership changes
    pub async fn dispatch_membership_changed(
        &self,
        group_id: ConsensusGroupId,
        added_members: &[NodeId],
        removed_members: &[NodeId],
    ) {
        if let Err(e) = self
            .callbacks
            .on_membership_changed(group_id, added_members, removed_members)
            .await
        {
            tracing::error!(
                "Membership change callback failed for group {:?}: {}",
                group_id,
                e
            );
        }
    }

    /// Dispatch callbacks based on operation and response
    pub async fn dispatch_operation(
        &self,
        group_id: ConsensusGroupId,
        request: &GroupRequest,
        response: &GroupResponse,
        is_replay: bool,
    ) {
        // Only dispatch callbacks for current operations (not replay)
        if is_replay {
            return;
        }

        // Dispatch based on request type and response
        match (request, response) {
            (
                GroupRequest::Stream(StreamOperation::Append { stream, message }),
                GroupResponse::Appended { .. },
            ) => {
                if let Err(e) = self
                    .callbacks
                    .on_message_appended(group_id, stream.as_str(), message)
                    .await
                {
                    tracing::error!("Message append callback failed: {}", e);
                }
            }

            (
                GroupRequest::Admin(AdminOperation::InitializeStream { stream }),
                GroupResponse::Success,
            ) => {
                if let Err(e) = self
                    .callbacks
                    .on_stream_created(group_id, stream.as_str())
                    .await
                {
                    tracing::error!("Stream creation callback failed: {}", e);
                }
            }

            (
                GroupRequest::Admin(AdminOperation::RemoveStream { stream }),
                GroupResponse::Success,
            ) => {
                if let Err(e) = self
                    .callbacks
                    .on_stream_removed(group_id, stream.as_str())
                    .await
                {
                    tracing::error!("Stream removal callback failed: {}", e);
                }
            }

            _ => {
                // Other operations don't have callbacks
            }
        }
    }
}
