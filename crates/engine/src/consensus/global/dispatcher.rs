//! Callback dispatcher for global consensus events

use std::sync::Arc;

use proven_topology::NodeId;
use tracing::error;

use super::{
    callbacks::GlobalConsensusCallbacks,
    types::{GlobalRequest, GlobalResponse},
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
    pub async fn dispatch_state_sync(&self) {
        if let Err(e) = self.callbacks.on_state_synchronized().await {
            error!("State sync callback failed: {}", e);
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
                GlobalRequest::CreateStream {
                    stream_name,
                    config,
                    placement,
                },
                GlobalResponse::StreamCreated { .. },
            ) => {
                if let Err(e) = self
                    .callbacks
                    .on_stream_created(stream_name, config, placement)
                    .await
                {
                    tracing::error!("Stream creation callback failed: {}", e);
                }
            }

            (GlobalRequest::DeleteStream { stream_name }, GlobalResponse::StreamDeleted { .. }) => {
                if let Err(e) = self.callbacks.on_stream_deleted(stream_name).await {
                    tracing::error!("Stream deletion callback failed: {}", e);
                }
            }

            (GlobalRequest::CreateGroup { info }, GlobalResponse::GroupCreated { .. }) => {
                if let Err(e) = self.callbacks.on_group_created(info.id, info).await {
                    error!("Group creation callback failed: {}", e);
                }
            }

            (GlobalRequest::DissolveGroup { id }, GlobalResponse::GroupDissolved { .. }) => {
                if let Err(e) = self.callbacks.on_group_dissolved(*id).await {
                    error!("Group dissolution callback failed: {}", e);
                }
            }

            (
                GlobalRequest::ReassignStream { stream_name, .. },
                GlobalResponse::StreamReassigned {
                    old_placement,
                    new_placement,
                    ..
                },
            ) => {
                if let Err(e) = self
                    .callbacks
                    .on_stream_reassigned(stream_name, old_placement, new_placement)
                    .await
                {
                    tracing::error!("Stream reassignment callback failed: {}", e);
                }
            }

            (
                GlobalRequest::AppendToGlobalStream { stream_name, .. },
                GlobalResponse::Appended { entries, .. },
            ) => {
                if let Some(entries) = entries
                    && let Err(e) = self
                        .callbacks
                        .on_global_stream_appended(stream_name, entries.clone())
                        .await
                {
                    tracing::error!("Global stream append callback failed: {}", e);
                }
            }

            _ => {
                // Other operations don't have callbacks yet
            }
        }
    }
}
