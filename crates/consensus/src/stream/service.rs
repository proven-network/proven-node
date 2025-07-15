//! Stream storage service
//!
//! This service subscribes to consensus events and maintains stream storage.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use proven_storage::LogStorage;

use crate::services::event::{EventBus, EventEnvelope, EventFilter};
use crate::stream::StreamName;
use crate::stream::storage::{StreamStorageImpl, StreamStorageWriter};

/// Stream storage service
pub struct StreamStorageService<L: LogStorage> {
    /// Stream storage instances by stream name
    streams: Arc<RwLock<HashMap<StreamName, Arc<StreamStorageImpl<L>>>>>,
    /// Storage backend for persistent streams
    storage: Arc<L>,
    /// Event bus reference
    event_bus: Arc<RwLock<Option<Arc<EventBus>>>>,
}

impl<L: LogStorage> StreamStorageService<L> {
    /// Create a new stream storage service
    pub fn new(storage: Arc<L>) -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            storage,
            event_bus: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the event bus
    pub async fn set_event_bus(&self, event_bus: Arc<EventBus>) {
        *self.event_bus.write().await = Some(event_bus);
    }

    /// Start the service
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let event_bus = self
            .event_bus
            .read()
            .await
            .as_ref()
            .ok_or("Event bus not set")?
            .clone();

        // Subscribe to stream events - filter by event type
        let filter = EventFilter::ByType(vec![crate::services::event::EventType::Stream]);

        let mut subscriber = event_bus
            .subscribe("stream-storage-service".to_string(), filter)
            .await?;
        let streams = self.streams.clone();
        let storage = self.storage.clone();

        // Spawn event handler
        tokio::spawn(async move {
            while let Some(envelope) = subscriber.recv().await {
                if let Err(e) = Self::handle_event(envelope, &streams, &storage).await {
                    tracing::error!("Error handling stream event: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Handle a stream event
    async fn handle_event(
        envelope: EventEnvelope,
        streams: &Arc<RwLock<HashMap<StreamName, Arc<StreamStorageImpl<L>>>>>,
        storage: &Arc<L>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match &envelope.event {
            crate::services::event::Event::StreamMessageAppended {
                stream,
                group_id,
                sequence,
                message,
                timestamp,
                term,
            } => {
                // Get or create stream storage
                let stream_storage = {
                    let mut streams_map = streams.write().await;

                    streams_map
                        .entry(stream.clone())
                        .or_insert_with(|| {
                            // TODO: Get persistence type from stream config
                            // For now, default to persistent
                            let namespace = proven_storage::StorageNamespace::new(format!(
                                "stream_{group_id}_{stream}"
                            ));
                            Arc::new(StreamStorageImpl::persistent(
                                stream.clone(),
                                storage.clone(),
                                namespace,
                            ))
                        })
                        .clone()
                };

                // Append to stream storage
                stream_storage
                    .append(*sequence, message.clone(), *timestamp, *term)
                    .await?;

                tracing::debug!(
                    "Stored message {} for stream {} in group {}",
                    sequence,
                    stream,
                    group_id
                );
            }
            crate::services::event::Event::StreamTrimmed {
                stream,
                group_id,
                new_start_seq,
            } => {
                // Handle stream trimming if needed
                tracing::debug!(
                    "Stream {} in group {} trimmed to start at {}",
                    stream,
                    group_id,
                    new_start_seq
                );
                // TODO: Implement stream trimming in storage
            }
            _ => {
                // Ignore other stream events
            }
        }

        Ok(())
    }

    /// Get stream storage for reading
    pub async fn get_stream(&self, stream_name: &StreamName) -> Option<Arc<StreamStorageImpl<L>>> {
        self.streams.read().await.get(stream_name).cloned()
    }
}
