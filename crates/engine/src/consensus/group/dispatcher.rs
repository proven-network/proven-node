//! Callback dispatcher for group consensus events

use std::{num::NonZero, sync::Arc};
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
            tracing::debug!(
                "Skipping callback dispatch for replayed operation: {:?}",
                request
            );
            return;
        }

        // Dispatch based on request type and response
        match (request, response) {
            (
                GroupRequest::Stream(StreamOperation::Append { stream, messages }),
                GroupResponse::Appended {
                    sequence: last_sequence,
                    ..
                },
            ) => {
                // Compute sequences for all messages in the batch
                // last_sequence is the sequence of the last message
                // So the first message has sequence: last_sequence - (messages.len() - 1)
                let first_sequence = NonZero::new(
                    last_sequence
                        .get()
                        .saturating_sub(messages.len() as u64 - 1),
                )
                .unwrap();

                // Get current timestamp
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                // Build the messages with sequences and timestamps
                let messages_with_meta: Vec<(MessageData, NonZero<u64>, u64)> = messages
                    .iter()
                    .enumerate()
                    .map(|(i, msg)| {
                        let seq = NonZero::new(first_sequence.get() + i as u64).unwrap();
                        (msg.clone(), seq, timestamp)
                    })
                    .collect();

                tracing::info!(
                    "Dispatching on_messages_appended callback for {} messages to stream {} (sequences: {:?})",
                    messages_with_meta.len(),
                    stream,
                    messages_with_meta
                        .iter()
                        .map(|(_, seq, _)| seq)
                        .collect::<Vec<_>>()
                );

                if let Err(e) = self
                    .callbacks
                    .on_messages_appended(group_id, stream.as_str(), &messages_with_meta)
                    .await
                {
                    tracing::error!("Messages append callback failed: {}", e);
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
