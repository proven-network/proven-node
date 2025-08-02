//! Group state access wrappers for enforcing read/write permissions at compile time

use std::sync::Arc;

use proven_storage::LogIndex;

use super::group_state::GroupState;
use crate::foundation::Message;
use crate::foundation::models::StreamState;
use crate::foundation::types::StreamName;

/// Trait for read-only operations on GroupState
#[async_trait::async_trait]
pub trait GroupStateRead {
    /// Get stream state
    async fn get_stream(&self, stream_name: &StreamName) -> Option<StreamState>;

    /// List all streams
    async fn list_streams(&self) -> Vec<StreamName>;
}

/// Trait for write operations on GroupState (includes read operations)
#[async_trait::async_trait]
pub trait GroupStateWrite: GroupStateRead {
    /// Initialize a new stream
    async fn initialize_stream(&self, stream_name: StreamName) -> bool;

    /// Remove a stream
    async fn remove_stream(&self, stream_name: &StreamName) -> bool;

    /// Append messages to a stream
    async fn append_to_group_stream(
        &self,
        stream_name: &StreamName,
        messages: Vec<Message>,
        timestamp: u64,
    ) -> Arc<Vec<bytes::Bytes>>;

    /// Trim a stream
    async fn trim_stream(&self, stream_name: &StreamName, up_to_seq: LogIndex) -> Option<LogIndex>;

    /// Delete a message
    async fn delete_message(
        &self,
        stream_name: &StreamName,
        sequence: LogIndex,
    ) -> Option<LogIndex>;
}

/// Read-only access to GroupState
#[derive(Clone)]
pub struct GroupStateReader {
    inner: Arc<GroupState>,
}

/// Read-write access to GroupState
#[derive(Clone)]
pub struct GroupStateWriter {
    inner: Arc<GroupState>,
}

/// Create a reader/writer pair for GroupState
pub fn create_group_state_access() -> (GroupStateReader, GroupStateWriter) {
    let state = Arc::new(GroupState::new());

    let reader = GroupStateReader {
        inner: state.clone(),
    };
    let writer = GroupStateWriter { inner: state };

    (reader, writer)
}

// Implement GroupStateRead for GroupStateReader
#[async_trait::async_trait]
impl GroupStateRead for GroupStateReader {
    async fn get_stream(&self, name: &StreamName) -> Option<StreamState> {
        self.inner.get_stream(name).await
    }

    async fn list_streams(&self) -> Vec<StreamName> {
        self.inner.list_streams().await
    }
}

// Implement GroupStateRead for GroupStateWriter
#[async_trait::async_trait]
impl GroupStateRead for GroupStateWriter {
    async fn get_stream(&self, name: &StreamName) -> Option<StreamState> {
        self.inner.get_stream(name).await
    }

    async fn list_streams(&self) -> Vec<StreamName> {
        self.inner.list_streams().await
    }
}

// Implement GroupStateWrite for GroupStateWriter
#[async_trait::async_trait]
impl GroupStateWrite for GroupStateWriter {
    async fn initialize_stream(&self, name: StreamName) -> bool {
        self.inner.initialize_stream(name).await
    }

    async fn remove_stream(&self, name: &StreamName) -> bool {
        self.inner.remove_stream(name).await
    }

    async fn append_to_group_stream(
        &self,
        stream: &StreamName,
        messages: Vec<Message>,
        timestamp: u64,
    ) -> Arc<Vec<bytes::Bytes>> {
        self.inner
            .append_to_group_stream(stream, messages, timestamp)
            .await
    }

    async fn trim_stream(&self, name: &StreamName, up_to_seq: LogIndex) -> Option<LogIndex> {
        self.inner.trim_stream(name, up_to_seq).await
    }

    async fn delete_message(&self, name: &StreamName, sequence: LogIndex) -> Option<LogIndex> {
        self.inner.delete_message(name, sequence).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_group_reader_cannot_write() {
        let (reader, _writer) = create_group_state_access();

        // Reader can only access read methods through the trait
        use GroupStateRead;
        let _ = reader.list_streams().await;

        // This should not compile - no write methods on reader:
        // reader.initialize_stream(...).await;
    }

    #[tokio::test]
    async fn test_group_writer_can_read_and_write() {
        let (_reader, writer) = create_group_state_access();

        // Writer can access both read and write methods
        use {GroupStateRead, GroupStateWrite};
        let _ = writer.list_streams().await;

        // Writer can also write
        let stream_name = StreamName::new("test-stream");
        let _ = writer.initialize_stream(stream_name).await;
    }

    #[tokio::test]
    async fn test_group_shared_state() {
        let (reader, writer) = create_group_state_access();

        // Writer modifies state
        let stream_name = StreamName::new("test-stream");
        writer.initialize_stream(stream_name.clone()).await;

        // Reader sees the changes
        use GroupStateRead;
        let streams = reader.list_streams().await;
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0], stream_name);
    }
}
