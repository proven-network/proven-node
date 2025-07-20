//! Stream read handler for direct stream reads

use proven_storage::StorageAdaptor;

use crate::{
    error::{ConsensusResult, Error, ErrorKind},
    services::stream::StoredMessage,
};

use super::types::StreamServiceRef;

/// Handles direct stream read requests (bypasses consensus)
pub struct StreamReadHandler<S>
where
    S: StorageAdaptor,
{
    /// Stream service for local reads
    stream_service: StreamServiceRef<S>,
}

impl<S> StreamReadHandler<S>
where
    S: StorageAdaptor + 'static,
{
    /// Create a new stream read handler
    pub fn new(stream_service: StreamServiceRef<S>) -> Self {
        Self { stream_service }
    }

    /// Handle a read request for a stream
    pub async fn handle_read(
        &self,
        stream_name: &str,
        start_sequence: u64,
        count: u64,
    ) -> ConsensusResult<Vec<StoredMessage>> {
        // Get stream service
        let stream_guard = self.stream_service.read().await;
        let stream_service = stream_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Stream service not available")
        })?;

        // Directly read from the stream service
        stream_service
            .read_messages(stream_name, start_sequence, count)
            .await
    }
}

impl<S> Clone for StreamReadHandler<S>
where
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            stream_service: self.stream_service.clone(),
        }
    }
}
