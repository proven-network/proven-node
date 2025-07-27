//! Command handlers for group consensus commands

use proven_attestation::Attestor;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{EventMetadata, RequestHandler};
use crate::services::stream::{
    StreamService,
    commands::{
        CreateStream, DeleteStream, GetStreamInfo, PersistMessages, ReadMessages, StreamInfo,
    },
};
use proven_storage::{LogStorage, StorageAdaptor};
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Handler for PersistMessages command from group consensus
#[derive(Clone)]
pub struct PersistMessagesHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<T, G, A, S>>,
}

impl<T, G, A, S> PersistMessagesHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<T, G, A, S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<T, G, A, S> RequestHandler<PersistMessages> for PersistMessagesHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: PersistMessages,
        _metadata: EventMetadata,
    ) -> Result<(), crate::foundation::events::Error> {
        let stream_name = &request.stream_name;
        let message_count = request.entries.len();

        // Get or create the storage for this stream
        let _storage = self.stream_service.get_or_create_storage(stream_name).await;

        // Extract the pre-serialized entries
        let entries = request.entries;

        // Batch append to storage
        if !entries.is_empty() {
            let namespace = proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));

            match self
                .stream_service
                .storage_manager()
                .stream_storage()
                .append(&namespace, entries)
                .await
            {
                Ok(last_seq) => {
                    info!(
                        "Successfully persisted {} messages to stream {} storage (last_seq: {})",
                        message_count, stream_name, last_seq
                    );

                    // Notify any stream watchers about the new messages
                    self.stream_service
                        .notify_stream_append(stream_name, last_seq)
                        .await;
                }
                Err(e) => {
                    error!(
                        "Failed to persist {} messages to stream {} storage: {}",
                        message_count, stream_name, e
                    );
                }
            }
        }

        Ok(())
    }
}

/// Handler for CreateStream command
#[derive(Clone)]
pub struct CreateStreamHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<T, G, A, S>>,
}

impl<T, G, A, S> CreateStreamHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<T, G, A, S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<T, G, A, S> RequestHandler<CreateStream> for CreateStreamHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: CreateStream,
        _metadata: EventMetadata,
    ) -> Result<(), crate::foundation::events::Error> {
        debug!(
            "Creating stream {} with config {:?} for group {:?}",
            request.name, request.config, request.group_id
        );

        if let Err(e) = self
            .stream_service
            .create_stream(
                request.name.clone(),
                request.config.clone(),
                request.group_id,
            )
            .await
        {
            error!("Failed to create stream {}: {}", request.name, e);
            return Err(crate::foundation::events::Error::Internal(e.to_string()));
        }

        Ok(())
    }
}

/// Handler for DeleteStream command
#[derive(Clone)]
pub struct DeleteStreamHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<T, G, A, S>>,
}

impl<T, G, A, S> DeleteStreamHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<T, G, A, S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<T, G, A, S> RequestHandler<DeleteStream> for DeleteStreamHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: DeleteStream,
        _metadata: EventMetadata,
    ) -> Result<(), crate::foundation::events::Error> {
        debug!("Deleting stream {}", request.name);

        if let Err(e) = self.stream_service.delete_stream(&request.name).await {
            error!("Failed to delete stream {}: {}", request.name, e);
            return Err(crate::foundation::events::Error::Internal(e.to_string()));
        }

        Ok(())
    }
}

/// Handler for ReadMessages command
#[derive(Clone)]
pub struct ReadMessagesHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<T, G, A, S>>,
}

impl<T, G, A, S> ReadMessagesHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<T, G, A, S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<T, G, A, S> RequestHandler<ReadMessages> for ReadMessagesHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: ReadMessages,
        _metadata: EventMetadata,
    ) -> Result<Vec<crate::services::stream::StoredMessage>, crate::foundation::events::Error> {
        match self
            .stream_service
            .read_messages(
                request.stream_name.as_str(),
                request.start_offset,
                proven_storage::LogIndex::new(request.count).expect("Invalid count"),
            )
            .await
        {
            Ok(messages) => Ok(messages),
            Err(e) => {
                error!(
                    "Failed to read messages from stream {}: {}",
                    request.stream_name, e
                );
                Err(crate::foundation::events::Error::Internal(e.to_string()))
            }
        }
    }
}

/// Handler for GetStreamInfo command
#[derive(Clone)]
pub struct GetStreamInfoHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<T, G, A, S>>,
}

impl<T, G, A, S> GetStreamInfoHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<T, G, A, S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<T, G, A, S> RequestHandler<GetStreamInfo> for GetStreamInfoHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetStreamInfo,
        _metadata: EventMetadata,
    ) -> Result<Option<StreamInfo>, crate::foundation::events::Error> {
        match self
            .stream_service
            .get_stream_metadata(&request.stream_name)
            .await
        {
            Some(metadata) => Ok(Some(StreamInfo {
                name: request.stream_name,
                config: metadata.config,
                group_id: metadata.group_id,
                start_offset: 0, // TODO: get actual start offset
                end_offset: metadata.message_count,
            })),
            None => Ok(None),
        }
    }
}
