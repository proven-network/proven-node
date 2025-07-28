//! Handler for DeleteStream command

use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::StreamName;
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::stream::commands::DeleteStream;
use crate::services::stream::internal::storage::StreamStorageImpl;
use dashmap::DashMap;
use proven_storage::StorageAdaptor;

/// Handler for DeleteStream command
pub struct DeleteStreamHandler<S>
where
    S: StorageAdaptor,
{
    /// Stream storage instances
    streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
    /// Stream configurations
    stream_configs: Arc<DashMap<StreamName, crate::foundation::StreamConfig>>,
}

impl<S> DeleteStreamHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(
        streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
        stream_configs: Arc<DashMap<StreamName, crate::foundation::StreamConfig>>,
    ) -> Self {
        Self {
            streams,
            stream_configs,
        }
    }

    async fn delete_stream(&self, name: &StreamName) -> Result<(), crate::error::Error> {
        self.stream_configs.remove(name);
        self.streams.remove(name);

        info!("Deleted stream {}", name);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<S> RequestHandler<DeleteStream> for DeleteStreamHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: DeleteStream,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        debug!("Deleting stream {}", request.name);

        if let Err(e) = self.delete_stream(&request.name).await {
            error!("Failed to delete stream {}: {}", request.name, e);
            return Err(EventError::Internal(e.to_string()));
        }

        Ok(())
    }
}
