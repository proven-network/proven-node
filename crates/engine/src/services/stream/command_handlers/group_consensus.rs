//! Command handlers for group consensus commands

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

/// Handler for PersistMessages command from group consensus
#[derive(Clone)]
pub struct PersistMessagesHandler<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
}

impl<S> PersistMessagesHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<S>>) -> Self {
        Self { stream_service }
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
pub struct CreateStreamHandler<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
}

impl<S> CreateStreamHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<S>>) -> Self {
        Self { stream_service }
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
pub struct DeleteStreamHandler<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
}

impl<S> DeleteStreamHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<S>>) -> Self {
        Self { stream_service }
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
pub struct ReadMessagesHandler<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
}

impl<S> ReadMessagesHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<S> RequestHandler<ReadMessages> for ReadMessagesHandler<S>
where
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
                proven_storage::LogIndex::new(request.start_offset).expect("Invalid start offset"),
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
pub struct GetStreamInfoHandler<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
}

impl<S> GetStreamInfoHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<S> RequestHandler<GetStreamInfo> for GetStreamInfoHandler<S>
where
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
