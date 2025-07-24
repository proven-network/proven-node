//! Callbacks for global consensus state changes

use async_trait::async_trait;

use crate::{
    error::ConsensusResult,
    foundation::{GroupInfo, types::ConsensusGroupId},
    services::stream::StreamName,
};
use proven_topology::NodeId;

/// Callbacks for global consensus operations and state synchronization
#[async_trait]
pub trait GlobalConsensusCallbacks: Send + Sync {
    /// Called when state machine has synchronized to current state after replay
    /// This is called once when the state machine has caught up to the last
    /// persisted log entry and is ready to process new operations.
    async fn on_state_synchronized(&self) -> ConsensusResult<()>;

    // Real-time operation callbacks (only called for new operations, not replay)

    /// Called when a new group is created (not during replay)
    async fn on_group_created(
        &self,
        group_id: ConsensusGroupId,
        group_info: &GroupInfo,
    ) -> ConsensusResult<()>;

    /// Called when a group is dissolved (not during replay)
    async fn on_group_dissolved(&self, group_id: ConsensusGroupId) -> ConsensusResult<()>;

    /// Called when a new stream is created (not during replay)
    async fn on_stream_created(
        &self,
        stream_name: &StreamName,
        config: &crate::services::stream::StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()>;

    /// Called when a stream is deleted (not during replay)
    async fn on_stream_deleted(&self, stream_name: &StreamName) -> ConsensusResult<()>;

    /// Called when global consensus membership changes (not during replay)
    async fn on_membership_changed(
        &self,
        added_members: &[NodeId],
        removed_members: &[NodeId],
    ) -> ConsensusResult<()>;
}
