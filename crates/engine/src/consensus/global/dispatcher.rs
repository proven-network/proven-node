//! Callback dispatcher for global consensus events

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    error::ConsensusResult, foundation::types::ConsensusGroupId, services::stream::StreamName,
};
use proven_topology::NodeId;

use super::{
    callbacks::GlobalConsensusCallbacks,
    state::GlobalState,
    types::{GlobalRequest, GlobalResponse, GroupInfo},
};

/// Dispatches callbacks based on operation results
pub struct GlobalCallbackDispatcher {
    callbacks: Arc<dyn GlobalConsensusCallbacks>,
}

impl GlobalCallbackDispatcher {
    /// Create a new callback dispatcher with required callbacks
    pub fn new(callbacks: Arc<dyn GlobalConsensusCallbacks>) -> Self {
        Self { callbacks }
    }

    /// Handle state synchronization after replay completes
    pub async fn dispatch_state_sync(&self, state: &GlobalState) {
        if let Err(e) = self.callbacks.on_state_synchronized(state).await {
            tracing::error!("State sync callback failed: {}", e);
        } else {
            tracing::info!("State synchronized - replay complete");
        }
    }

    /// Handle membership changes
    pub async fn dispatch_membership_changed(
        &self,
        added_members: &[NodeId],
        removed_members: &[NodeId],
    ) {
        if let Err(e) = self
            .callbacks
            .on_membership_changed(added_members, removed_members)
            .await
        {
            tracing::error!("Membership change callback failed: {}", e);
        }
    }

    /// Dispatch callbacks based on operation and response
    pub async fn dispatch_operation(
        &self,
        request: &GlobalRequest,
        response: &GlobalResponse,
        is_replay: bool,
    ) {
        // Only dispatch callbacks for current operations (not replay)
        if is_replay {
            return;
        }

        // Dispatch based on request type and response
        match (request, response) {
            (
                GlobalRequest::CreateStream { name, group_id, .. },
                GlobalResponse::StreamCreated { .. },
            ) => {
                if let Err(e) = self.callbacks.on_stream_created(name, *group_id).await {
                    tracing::error!("Stream creation callback failed: {}", e);
                }
            }

            (GlobalRequest::DeleteStream { name }, GlobalResponse::StreamDeleted { .. }) => {
                if let Err(e) = self.callbacks.on_stream_deleted(name).await {
                    tracing::error!("Stream deletion callback failed: {}", e);
                }
            }

            (GlobalRequest::CreateGroup { info }, GlobalResponse::GroupCreated { .. }) => {
                if let Err(e) = self.callbacks.on_group_created(info.id, info).await {
                    tracing::error!("Group creation callback failed: {}", e);
                }
            }

            (GlobalRequest::DissolveGroup { id }, GlobalResponse::GroupDissolved { .. }) => {
                if let Err(e) = self.callbacks.on_group_dissolved(*id).await {
                    tracing::error!("Group dissolution callback failed: {}", e);
                }
            }

            _ => {
                // Other operations don't have callbacks
            }
        }
    }
}
