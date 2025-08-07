//! Handler for PersistMessages command

use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::StreamName;
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::stream::commands::PersistMessages;
use crate::services::stream::internal::storage::StreamStorageImpl;
use dashmap::DashMap;
use proven_storage::{LogIndex, LogStorage, StorageAdaptor, StorageManager};

/// Handler for PersistMessages command from group consensus
pub struct PersistMessagesHandler<S>
where
    S: StorageAdaptor,
{
    /// Stream storage instances
    streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
}

impl<S> PersistMessagesHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(
        streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
        storage_manager: Arc<StorageManager<S>>,
    ) -> Self {
        Self {
            streams,
            storage_manager,
        }
    }

    async fn get_or_create_storage(
        &self,
        stream_name: &StreamName,
    ) -> Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>> {
        if let Some(storage) = self.streams.get(stream_name) {
            return storage.clone();
        }

        // Default to persistent storage when creating on-demand
        let namespace = proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));
        let storage = Arc::new(StreamStorageImpl::persistent(
            stream_name.clone(),
            self.storage_manager.stream_storage(),
            namespace,
        ));

        self.streams.insert(stream_name.clone(), storage.clone());
        storage
    }
}

#[async_trait::async_trait]
impl<S> RequestHandler<PersistMessages> for PersistMessagesHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: PersistMessages,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        let stream_name = &request.stream_name;
        let message_count = request.entries.len();

        // Get or create the storage for this stream
        let _storage = self.get_or_create_storage(stream_name).await;

        // Extract the pre-serialized entries
        let entries = request.entries;

        // Batch append to storage
        if !entries.is_empty() {
            let namespace = proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));

            match self
                .storage_manager
                .stream_storage()
                .append(&namespace, entries)
                .await
            {
                Ok(last_seq) => {
                    debug!(
                        "Successfully persisted {} messages to stream {} storage (last_seq: {})",
                        message_count, stream_name, last_seq
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to persist {} messages to stream {} storage: {}",
                        message_count, stream_name, e
                    );
                    return Err(EventError::Internal(format!(
                        "Failed to persist messages: {e}"
                    )));
                }
            }
        }

        Ok(())
    }
}
