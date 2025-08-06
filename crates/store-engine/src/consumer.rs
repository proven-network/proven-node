//! Background consumer for streaming store data from engine

use std::sync::Arc;

use proven_engine::Client;
use proven_storage::LogIndex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::{
    Error,
    view::{KeyOperation, StoreView},
};

/// Consumer for store data stream
pub struct StoreConsumer {
    view: StoreView,
    client: Arc<Client>,
    data_stream_name: String,
    key_stream_name: String,
}

impl StoreConsumer {
    /// Create a new consumer
    pub const fn new(
        view: StoreView,
        client: Arc<Client>,
        data_stream_name: String,
        key_stream_name: String,
    ) -> Self {
        Self {
            view,
            client,
            data_stream_name,
            key_stream_name,
        }
    }

    /// Start the consumer tasks
    #[allow(clippy::unused_async)]
    pub async fn start(self) -> Result<(JoinHandle<()>, JoinHandle<()>), Error> {
        // Start data stream consumer
        let data_handle = {
            let view = self.view.clone();
            let client = self.client.clone();
            let stream_name = self.data_stream_name.clone();

            tokio::spawn(async move {
                if let Err(e) = consume_data_stream(client, stream_name, view).await {
                    error!("Data stream consumer error: {}", e);
                }
            })
        };

        // Start key stream consumer
        let key_handle = {
            let view = self.view;
            let client = self.client;
            let stream_name = self.key_stream_name;

            tokio::spawn(async move {
                if let Err(e) = consume_key_stream(client, stream_name, view).await {
                    error!("Key stream consumer error: {}", e);
                }
            })
        };

        Ok((data_handle, key_handle))
    }
}

/// Consume the data stream and update the view
#[allow(clippy::cognitive_complexity)]
async fn consume_data_stream(
    client: Arc<Client>,
    stream_name: String,
    view: StoreView,
) -> Result<(), Error> {
    use tokio_stream::StreamExt;

    // Start from the beginning
    let start_seq = LogIndex::new(1).unwrap();

    // Create the stream
    let stream = client
        .stream_messages(stream_name.clone(), Some(start_seq))
        .await
        .map_err(|e| Error::Stream(e.to_string()))?;

    info!("Started data stream consumer for {}", stream_name);

    tokio::pin!(stream);
    while let Some((message, _timestamp, sequence)) = stream.next().await {
        // Extract key from headers
        let key = message
            .headers
            .iter()
            .find(|(k, _)| k == "key")
            .map(|(_, v)| v.clone());

        if let Some(key) = key {
            // Check if this is a delete (marked with deleted=true header)
            let is_deleted = message
                .headers
                .iter()
                .any(|(k, v)| k == "deleted" && v == "true");

            if is_deleted {
                debug!("Processing delete for key: {}", key);
                view.apply_data_message(key, None, sequence);
            } else {
                debug!("Processing put for key: {} (seq {})", key, sequence);
                view.apply_data_message(key, Some(message.payload), sequence);
            }
        }
    }

    info!("Data stream consumer finished for {}", stream_name);
    Ok(())
}

/// Consume the key operations stream
#[allow(clippy::cognitive_complexity)]
async fn consume_key_stream(
    client: Arc<Client>,
    stream_name: String,
    view: StoreView,
) -> Result<(), Error> {
    use tokio_stream::StreamExt;

    // Start from the beginning
    let start_seq = LogIndex::new(1).unwrap();

    // Create the stream
    let stream = client
        .stream_messages(stream_name.clone(), Some(start_seq))
        .await
        .map_err(|e| Error::Stream(e.to_string()))?;

    info!("Started key stream consumer for {}", stream_name);

    tokio::pin!(stream);
    while let Some((message, _timestamp, sequence)) = stream.next().await {
        // Deserialize key operation
        match serde_json::from_slice::<KeyOperation>(&message.payload) {
            Ok(operation) => {
                debug!(
                    "Processing key operation: {:?} (seq {})",
                    operation, sequence
                );
                view.apply_key_operation(&operation, sequence);
            }
            Err(e) => {
                error!("Failed to deserialize key operation: {}", e);
            }
        }
    }

    info!("Key stream consumer finished for {}", stream_name);
    Ok(())
}
