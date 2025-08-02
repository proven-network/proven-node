//! Global state access wrappers for enforcing read/write permissions at compile time

use std::sync::Arc;

use proven_storage::LogIndex;
use proven_topology::NodeId;

use super::global_state::GlobalState;
use crate::error::ConsensusResult;
use crate::foundation::models::stream::StreamPlacement;
use crate::foundation::models::{GroupInfo, NodeInfo, StreamConfig, StreamInfo};
use crate::foundation::types::{ConsensusGroupId, StreamName};
use crate::foundation::{Message, models::StreamState};

/// Trait for read-only operations on GlobalState
#[async_trait::async_trait]
pub trait GlobalStateRead {
    /// Get stream information
    async fn get_stream(&self, stream_name: &StreamName) -> Option<StreamInfo>;

    /// Get all streams
    async fn get_all_streams(&self) -> Vec<StreamInfo>;

    /// Get group information
    async fn get_group(&self, id: &ConsensusGroupId) -> Option<GroupInfo>;

    /// Get all groups
    async fn get_all_groups(&self) -> Vec<GroupInfo>;

    /// Get member information
    async fn get_member(&self, node_id: &NodeId) -> Option<NodeInfo>;

    /// Get groups for a node
    async fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupId>;

    /// Get streams for a group
    async fn get_streams_for_group(&self, group_id: ConsensusGroupId) -> Vec<StreamInfo>;

    /// Count streams in a group
    async fn count_streams_in_group(&self, group_id: ConsensusGroupId) -> usize;

    /// Get global stream state
    async fn get_global_stream_state(&self, stream_name: &StreamName) -> Option<StreamState>;
}

/// Trait for write operations on GlobalState (includes read operations)
#[async_trait::async_trait]
pub trait GlobalStateWrite: GlobalStateRead {
    /// Add a stream
    async fn add_stream(&self, info: StreamInfo) -> ConsensusResult<()>;

    /// Remove a stream
    async fn remove_stream(&self, stream_name: &StreamName) -> Option<StreamInfo>;

    /// Update stream configuration
    async fn update_stream_config(&self, stream_name: &StreamName, config: StreamConfig) -> bool;

    /// Reassign a stream to a different placement
    async fn reassign_stream(
        &self,
        stream_name: &StreamName,
        new_placement: StreamPlacement,
    ) -> bool;

    /// Add a group
    async fn add_group(&self, info: GroupInfo) -> ConsensusResult<()>;

    /// Remove a group
    async fn remove_group(&self, id: ConsensusGroupId) -> Option<GroupInfo>;

    /// Add a member
    async fn add_member(&self, info: NodeInfo);

    /// Remove a member
    async fn remove_member(&self, node_id: &NodeId) -> Option<NodeInfo>;

    /// Clear all state
    async fn clear(&self);

    /// Append messages to a global stream
    async fn append_to_global_stream(
        &self,
        stream_name: &StreamName,
        messages: Vec<Message>,
        timestamp: u64,
    ) -> (Arc<Vec<bytes::Bytes>>, Option<LogIndex>);

    /// Trim a global stream
    async fn trim_global_stream(
        &self,
        stream_name: &StreamName,
        up_to_seq: LogIndex,
    ) -> Option<LogIndex>;

    /// Delete a message from a global stream
    async fn delete_from_global_stream(
        &self,
        stream_name: &StreamName,
        sequence: LogIndex,
    ) -> Option<LogIndex>;
}

/// Read-only access to GlobalState
#[derive(Clone)]
pub struct GlobalStateReader {
    inner: Arc<GlobalState>,
}

/// Read-write access to GlobalState
#[derive(Clone)]
pub struct GlobalStateWriter {
    inner: Arc<GlobalState>,
}

/// Create a reader/writer pair for GlobalState
pub fn create_global_state_access() -> (GlobalStateReader, GlobalStateWriter) {
    let state = Arc::new(GlobalState::new());

    let reader = GlobalStateReader {
        inner: state.clone(),
    };
    let writer = GlobalStateWriter { inner: state };

    (reader, writer)
}

// Implement GlobalStateRead for GlobalStateReader
#[async_trait::async_trait]
impl GlobalStateRead for GlobalStateReader {
    async fn get_stream(&self, stream_name: &StreamName) -> Option<StreamInfo> {
        self.inner.get_stream(stream_name).await
    }

    async fn get_all_streams(&self) -> Vec<StreamInfo> {
        self.inner.get_all_streams().await
    }

    async fn get_group(&self, id: &ConsensusGroupId) -> Option<GroupInfo> {
        self.inner.get_group(id).await
    }

    async fn get_all_groups(&self) -> Vec<GroupInfo> {
        self.inner.get_all_groups().await
    }

    async fn get_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.inner.get_member(node_id).await
    }

    async fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupId> {
        self.inner.get_node_groups(node_id).await
    }

    async fn get_streams_for_group(&self, group_id: ConsensusGroupId) -> Vec<StreamInfo> {
        self.inner.get_streams_for_group(group_id).await
    }

    async fn count_streams_in_group(&self, group_id: ConsensusGroupId) -> usize {
        self.inner.count_streams_in_group(group_id).await
    }

    async fn get_global_stream_state(&self, stream_name: &StreamName) -> Option<StreamState> {
        self.inner.get_global_stream_state(stream_name).await
    }
}

// Implement GlobalStateRead for GlobalStateWriter
#[async_trait::async_trait]
impl GlobalStateRead for GlobalStateWriter {
    async fn get_stream(&self, stream_name: &StreamName) -> Option<StreamInfo> {
        self.inner.get_stream(stream_name).await
    }

    async fn get_all_streams(&self) -> Vec<StreamInfo> {
        self.inner.get_all_streams().await
    }

    async fn get_group(&self, id: &ConsensusGroupId) -> Option<GroupInfo> {
        self.inner.get_group(id).await
    }

    async fn get_all_groups(&self) -> Vec<GroupInfo> {
        self.inner.get_all_groups().await
    }

    async fn get_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.inner.get_member(node_id).await
    }

    async fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupId> {
        self.inner.get_node_groups(node_id).await
    }

    async fn get_streams_for_group(&self, group_id: ConsensusGroupId) -> Vec<StreamInfo> {
        self.inner.get_streams_for_group(group_id).await
    }

    async fn count_streams_in_group(&self, group_id: ConsensusGroupId) -> usize {
        self.inner.count_streams_in_group(group_id).await
    }

    async fn get_global_stream_state(&self, stream_name: &StreamName) -> Option<StreamState> {
        self.inner.get_global_stream_state(stream_name).await
    }
}

// Implement GlobalStateWrite for GlobalStateWriter
#[async_trait::async_trait]
impl GlobalStateWrite for GlobalStateWriter {
    async fn add_stream(&self, info: StreamInfo) -> ConsensusResult<()> {
        self.inner.add_stream(info).await
    }

    async fn remove_stream(&self, stream_name: &StreamName) -> Option<StreamInfo> {
        self.inner.remove_stream(stream_name).await
    }

    async fn update_stream_config(&self, stream_name: &StreamName, config: StreamConfig) -> bool {
        self.inner.update_stream_config(stream_name, config).await
    }

    async fn reassign_stream(
        &self,
        stream_name: &StreamName,
        new_placement: StreamPlacement,
    ) -> bool {
        self.inner.reassign_stream(stream_name, new_placement).await
    }

    async fn add_group(&self, info: GroupInfo) -> ConsensusResult<()> {
        self.inner.add_group(info).await
    }

    async fn remove_group(&self, id: ConsensusGroupId) -> Option<GroupInfo> {
        self.inner.remove_group(id).await
    }

    async fn add_member(&self, info: NodeInfo) {
        self.inner.add_member(info).await
    }

    async fn remove_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.inner.remove_member(node_id).await
    }

    async fn clear(&self) {
        self.inner.clear().await
    }

    async fn append_to_global_stream(
        &self,
        stream_name: &StreamName,
        messages: Vec<Message>,
        timestamp: u64,
    ) -> (Arc<Vec<bytes::Bytes>>, Option<LogIndex>) {
        self.inner
            .append_to_global_stream(stream_name, messages, timestamp)
            .await
    }

    async fn trim_global_stream(
        &self,
        stream_name: &StreamName,
        up_to_seq: LogIndex,
    ) -> Option<LogIndex> {
        self.inner.trim_global_stream(stream_name, up_to_seq).await
    }

    async fn delete_from_global_stream(
        &self,
        stream_name: &StreamName,
        sequence: LogIndex,
    ) -> Option<LogIndex> {
        self.inner
            .delete_from_global_stream(stream_name, sequence)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reader_cannot_write() {
        let (reader, _writer) = create_global_state_access();

        // Reader can only access read methods through the trait
        use GlobalStateRead;
        let _ = reader.get_all_groups().await;

        // This should not compile - no write methods on reader:
        // reader.add_group(...).await;
    }

    #[tokio::test]
    async fn test_writer_can_read_and_write() {
        let (_reader, writer) = create_global_state_access();

        // Writer can access both read and write methods
        use {GlobalStateRead, GlobalStateWrite};
        let _ = writer.get_all_groups().await;

        // Writer can also write
        let group_info = GroupInfo {
            id: ConsensusGroupId::new(1),
            members: vec![],
            created_at: 0,
            metadata: Default::default(),
        };
        let _ = writer.add_group(group_info).await;
    }

    #[tokio::test]
    async fn test_shared_state() {
        let (reader, writer) = create_global_state_access();

        // Writer modifies state
        let group_info = GroupInfo {
            id: ConsensusGroupId::new(1),
            members: vec![],
            created_at: 0,
            metadata: Default::default(),
        };
        writer.add_group(group_info.clone()).await.unwrap();

        // Reader sees the changes
        use GlobalStateRead;
        let groups = reader.get_all_groups().await;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].id, ConsensusGroupId::new(1));
    }
}
