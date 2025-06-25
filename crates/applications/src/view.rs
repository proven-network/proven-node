use crate::{Application, Error, events::ApplicationEvent};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::consumer_handler::ConsumerHandler;
use tokio::sync::RwLock;
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Shared in-memory view of applications built from events
#[derive(Clone, Debug)]
pub struct ApplicationView {
    // Use RwLock for efficient concurrent reads
    applications: Arc<RwLock<HashMap<Uuid, Application>>>,
}

impl ApplicationView {
    /// Creates a new empty application view.
    pub fn new() -> Self {
        Self {
            applications: Arc::new(RwLock::new(HashMap::new())),
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
        _stream_sequence: u64,
    ) -> Result<(), Self::Error> {
        // Apply the event to update the view
        self.view.apply_event(&event).await;
        Ok(())
    }

    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        // Called when the consumer has processed all existing events
        Ok(())
    }
}
