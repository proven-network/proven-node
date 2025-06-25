use crate::{Application, Error, events::ApplicationEvent};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use proven_messaging::consumer_handler::ConsumerHandler;
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Shared in-memory view of applications built from events
#[derive(Clone, Debug)]
pub struct ApplicationView {
    // Use RwLock for efficient concurrent reads
    applications: Arc<RwLock<HashMap<Uuid, Application>>>,

    // Track the sequence number of the last processed event
    last_processed_seq: Arc<AtomicU64>,

    // Notifier for when sequence number advances (for consistency waiting)
    seq_notifier: Arc<Notify>,
}

impl ApplicationView {
    /// Creates a new empty application view.
    pub fn new() -> Self {
        Self {
            applications: Arc::new(RwLock::new(HashMap::new())),
            last_processed_seq: Arc::new(AtomicU64::new(0)),
            seq_notifier: Arc::new(Notify::new()),
        }
    }

    /// Get a single application by ID
    pub async fn get_application(&self, application_id: Uuid) -> Option<Application> {
        self.applications.read().await.get(&application_id).cloned()
    }

    /// List all applications owned by a specific user
    pub async fn list_applications_by_owner(&self, owner_id: Uuid) -> Vec<Application> {
        self.applications
            .read()
            .await
            .values()
            .filter(|app| app.owner_identity_id == owner_id)
            .cloned()
            .collect()
    }

    /// Check if an application exists
    pub async fn application_exists(&self, application_id: Uuid) -> bool {
        self.applications.read().await.contains_key(&application_id)
    }

    /// Get all applications (for admin/debugging purposes)
    pub async fn list_all_applications(&self) -> Vec<Application> {
        self.applications.read().await.values().cloned().collect()
    }

    /// Get the count of applications
    pub async fn application_count(&self) -> usize {
        self.applications.read().await.len()
    }

    /// Get the sequence number of the last processed event
    /// Useful for read-your-own-writes consistency
    pub fn last_processed_seq(&self) -> u64 {
        self.last_processed_seq.load(Ordering::SeqCst)
    }

    /// Wait until the view has processed at least the given sequence number
    /// Used for strong consistency guarantees in service handlers
    pub async fn wait_for_seq(&self, min_seq: u64) -> Result<(), Error> {
        if self.last_processed_seq() >= min_seq {
            return Ok(()); // Already caught up
        }

        // Use notification-based waiting to avoid polling
        loop {
            let notified = self.seq_notifier.notified();

            // Check again in case we missed a notification
            if self.last_processed_seq() >= min_seq {
                return Ok(());
            }

            // Wait for next notification
            notified.await;
        }
    }

    /// Apply an event to update the view state
    /// This is called by the consumer handler when processing events
    async fn apply_event(&self, event: &ApplicationEvent) {
        let mut apps = self.applications.write().await;

        match event {
            ApplicationEvent::Created {
                application_id,
                owner_identity_id,
                ..
            } => {
                let app = Application {
                    id: *application_id,
                    owner_identity_id: *owner_identity_id,
                };
                apps.insert(*application_id, app);
            }
            ApplicationEvent::OwnershipTransferred {
                application_id,
                new_owner_id,
                ..
            } => {
                if let Some(app) = apps.get_mut(application_id) {
                    app.owner_identity_id = *new_owner_id;
                }
            }
            ApplicationEvent::Archived { application_id, .. } => {
                apps.remove(application_id);
            }
        }
    }
}

impl Default for ApplicationView {
    fn default() -> Self {
        Self::new()
    }
}

/// Consumer handler that processes events from the event stream to update the view
#[derive(Clone, Debug)]
pub struct ApplicationViewConsumerHandler {
    view: ApplicationView,
}

impl ApplicationViewConsumerHandler {
    /// Creates a new consumer handler with the given view.
    ///
    /// # Arguments
    ///
    /// * `view` - The application view to update when events are processed
    pub fn new(view: ApplicationView) -> Self {
        Self { view }
    }

    /// Gets a reference to the application view.
    pub fn view(&self) -> &ApplicationView {
        &self.view
    }
}

#[async_trait]
impl ConsumerHandler<ApplicationEvent, DeserializeError, SerializeError>
    for ApplicationViewConsumerHandler
{
    type Error = Error;

    async fn handle(
        &self,
        event: ApplicationEvent,
        stream_sequence: u64,
    ) -> Result<(), Self::Error> {
        // Apply the event to update the view
        self.view.apply_event(&event).await;

        // Update the last processed sequence number
        self.view
            .last_processed_seq
            .store(stream_sequence, Ordering::SeqCst);

        // Notify any waiters that the sequence has advanced
        self.view.seq_notifier.notify_waiters();

        Ok(())
    }

    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        // Called when the consumer has processed all existing events
        Ok(())
    }
}
