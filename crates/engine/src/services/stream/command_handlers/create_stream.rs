//! Handler for CreateStream command

use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::foundation::{StreamConfig, StreamName, types::ConsensusGroupId};
use crate::services::stream::PersistenceType;
use crate::services::stream::commands::CreateStream;
use crate::services::stream::internal::storage::StreamStorageImpl;
use dashmap::DashMap;
use proven_storage::{LogIndex, StorageAdaptor, StorageManager};
use tokio::sync::watch;

/// Handler for CreateStream command
pub struct CreateStreamHandler<S>
where
    S: StorageAdaptor,
{
    /// Stream storage instances
    streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
    /// Stream configurations
    stream_configs: Arc<dashmap::DashMap<StreamName, StreamConfig>>,
    /// Stream notifiers
    stream_notifiers: Arc<dashmap::DashMap<StreamName, watch::Sender<LogIndex>>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
    /// Default persistence type
    default_persistence: PersistenceType,
}

impl<S> CreateStreamHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(
        streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
        stream_configs: Arc<DashMap<StreamName, StreamConfig>>,
        stream_notifiers: Arc<DashMap<StreamName, watch::Sender<LogIndex>>>,
        storage_manager: Arc<StorageManager<S>>,
        default_persistence: PersistenceType,
    ) -> Self {
        Self {
            streams,
            stream_configs,
            stream_notifiers,
            storage_manager,
            default_persistence,
        }
    }

    async fn create_stream(
        &self,
        name: StreamName,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> Result<(), crate::error::Error> {
        use crate::error::{Error, ErrorKind};

        // Check if stream already exists
        if self.stream_configs.contains_key(&name) {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                format!("Stream {name} already exists"),
            ));
        }

        // Store configuration
        self.stream_configs.insert(name.clone(), config.clone());

        // Create notifier for this stream
        let (tx, _rx) = watch::channel(LogIndex::new(1).unwrap());
        self.stream_notifiers.insert(name.clone(), tx);

        // Create storage instance based on config
        let persistence_type = config.persistence_type;
        if persistence_type == PersistenceType::Persistent {
            let namespace = proven_storage::StorageNamespace::new(format!("stream_{name}"));
            let storage = Arc::new(StreamStorageImpl::persistent(
                name.clone(),
                self.storage_manager.stream_storage(),
                namespace,
            ));
            self.streams.insert(name.clone(), storage);
        }

        info!(
            "Created stream {} in group {} with config {:?}",
            name, group_id, config
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl<S> RequestHandler<CreateStream> for CreateStreamHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: CreateStream,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        debug!(
            "Creating stream {} with config {:?} for group {:?}",
            request.name, request.config, request.group_id
        );

        if let Err(e) = self
            .create_stream(
                request.name.clone(),
                request.config.clone(),
                request.group_id,
            )
            .await
        {
            error!("Failed to create stream {}: {}", request.name, e);
            return Err(EventError::Internal(e.to_string()));
        }

        Ok(())
    }
}
