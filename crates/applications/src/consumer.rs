//! Consumer pattern for processing events from streams.
//!
//! This module implements stateful stream consumption with position tracking,
//! replacing the messaging consumer abstraction.

use std::sync::Arc;

use proven_engine::Client;
use proven_storage::LogIndex;
use tokio::task::JoinHandle;

use crate::{Error, Event, view::ApplicationView};

/// Consumer for building application view from events
pub struct ApplicationViewConsumer {
    /// The view being built
    view: ApplicationView,
    /// Last processed sequence number
    last_processed_seq: u64,
}

impl ApplicationViewConsumer {
    /// Create a new consumer.
    #[must_use]
    pub const fn new(view: ApplicationView) -> Self {
        Self {
            view,
            last_processed_seq: 0,
        }
    }

    /// Process an event and update the view.
    pub async fn handle_event(&mut self, event: Event, sequence: u64) {
        // Apply event to view
        self.view.apply_event(event).await;

        // Update sequence tracking
        self.last_processed_seq = sequence;
        self.view.update_last_processed_seq(sequence);
    }

    /// Get the last processed sequence number.
    #[must_use]
    pub const fn last_processed_seq(&self) -> u64 {
        self.last_processed_seq
    }
}

/// Start the event consumer background task.
pub async fn start_event_consumer(
    client: Arc<Client>,
    event_stream: String,
    view: ApplicationView,
) -> Result<JoinHandle<()>, Error> {
    // Check if stream exists and get starting position
    let start_seq = match client.get_stream_info(&event_stream).await {
        Ok(Some(_info)) => {
            // TODO: In a real implementation, we'd persist consumer position
            // For now, start from the beginning
            LogIndex::new(1).unwrap()
        }
        Ok(None) => {
            return Err(Error::Stream(format!(
                "Event stream '{event_stream}' not found"
            )));
        }
        Err(e) => {
            return Err(Error::Stream(e.to_string()));
        }
    };

    // Create consumer
    let mut consumer = ApplicationViewConsumer::new(view);

    // Start consumer task
    let handle = tokio::spawn(async move {
        if let Err(e) = run_consumer_loop(client, event_stream, &mut consumer, start_seq).await {
            tracing::error!("Event consumer error: {}", e);
        }
    });

    Ok(handle)
}

/// Run the consumer loop.
#[allow(clippy::cognitive_complexity)]
async fn run_consumer_loop(
    client: Arc<Client>,
    event_stream: String,
    consumer: &mut ApplicationViewConsumer,
    start_seq: LogIndex,
) -> Result<(), Error> {
    use tokio::pin;

    // Use streaming API with follow mode for continuous consumption
    let stream = client
        .stream_messages(event_stream.clone(), start_seq, None)
        .await
        .map_err(|e| Error::Stream(e.to_string()))?;

    tracing::info!(
        "Started event consumer from sequence {} with follow mode",
        start_seq
    );

    pin!(stream);
    while let Some(message) = tokio_stream::StreamExt::next(&mut stream).await {
        // Deserialize event from the stored message payload
        let event: Event = ciborium::de::from_reader(&message.data.payload[..])
            .map_err(|e| Error::Deserialization(e.to_string()))?;

        // Process event
        consumer.handle_event(event, message.sequence.get()).await;

        // Log progress periodically
        if message.sequence.get().is_multiple_of(100) {
            tracing::debug!(
                "Event consumer processed up to sequence {}",
                message.sequence.get()
            );
        }
    }

    tracing::info!("Event consumer finished");
    Ok(())
}
