//! Callbacks for group consensus state changes

use async_trait::async_trait;
use std::num::NonZero;

use crate::{error::ConsensusResult, foundation::types::ConsensusGroupId};
use proven_topology::NodeId;

use super::{state::GroupState, types::MessageData};

/// Callbacks for group consensus operations and state synchronization
#[async_trait]
pub trait GroupConsensusCallbacks: Send + Sync {
    /// Called when state machine has synchronized to current state after replay
    /// This is called once when the state machine has caught up to the last
    /// persisted log entry and is ready to process new operations.
    async fn on_state_synchronized(
        &self,
        group_id: ConsensusGroupId,
        state: &GroupState,
    ) -> ConsensusResult<()>;

    // Real-time operation callbacks (only called for new operations, not replay)

    /// Called when a new stream is created in the group (not during replay)
    async fn on_stream_created(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<()>;

    /// Called when a stream is removed from the group (not during replay)
    async fn on_stream_removed(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<()>;

    /// Called when messages are appended to a stream (not during replay)
    async fn on_messages_appended(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
        messages: &[(MessageData, NonZero<u64>, u64)], // (message, sequence, timestamp)
    ) -> ConsensusResult<()>;

    /// Called when group consensus membership changes (not during replay)
    async fn on_membership_changed(
        &self,
        group_id: ConsensusGroupId,
        added_members: &[NodeId],
        removed_members: &[NodeId],
    ) -> ConsensusResult<()>;
}
