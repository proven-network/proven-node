use crate::{Application, Error, event::Event};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_util::{Domain, Origin};
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Shared in-memory view of applications built from events
#[derive(Clone, Debug)]
pub struct ApplicationView {
    // Use RwLock for efficient concurrent reads
    applications: Arc<RwLock<HashMap<Uuid, Application>>>,

    // Map from HTTP domain to application ID
    http_domain_to_application: Arc<RwLock<HashMap<Domain, Uuid>>>,

    // Track the sequence number of the last processed event
    last_processed_seq: Arc<AtomicU64>,

    // Notifier for when sequence number advances (for consistency waiting)
    seq_notifier: Arc<Notify>,
}

impl ApplicationView {
    /// Creates a new empty application view.
    #[must_use]
    pub fn new() -> Self {
        Self {
            applications: Arc::new(RwLock::new(HashMap::new())),
            http_domain_to_application: Arc::new(RwLock::new(HashMap::new())),
            last_processed_seq: Arc::new(AtomicU64::new(0)),
            seq_notifier: Arc::new(Notify::new()),
        }
    }

    /// Check if an application exists
    pub async fn application_exists(&self, application_id: &Uuid) -> bool {
        self.applications.read().await.contains_key(application_id)
    }

    /// Get a single application by ID
    pub async fn get_application(&self, application_id: &Uuid) -> Option<Application> {
        self.applications.read().await.get(application_id).cloned()
    }

    /// Get an application by HTTP domain
    pub async fn get_application_id_for_http_domain(&self, http_domain: &Domain) -> Option<Uuid> {
        self.http_domain_to_application
            .read()
            .await
            .get(http_domain)
            .copied()
    }

    /// Check if an HTTP domain is linked to an application
    pub async fn http_domain_linked(&self, http_domain: &Domain) -> bool {
        self.http_domain_to_application
            .read()
            .await
            .contains_key(http_domain)
    }

    /// Get all applications (for admin/debugging purposes)
    pub async fn list_all_applications(&self) -> Vec<Application> {
        self.applications.read().await.values().cloned().collect()
    }

    /// List all applications owned by a specific user
    pub async fn list_applications_by_owner(&self, owner_id: &Uuid) -> Vec<Application> {
        self.applications
            .read()
            .await
            .values()
            .filter(|app| app.owner_id == *owner_id)
            .cloned()
            .collect()
    }

    /// Get the count of applications
    pub async fn application_count(&self) -> usize {
        self.applications.read().await.len()
    }

    /// Get the sequence number of the last processed event
    /// Useful for read-your-own-writes consistency
    #[must_use]
    pub fn last_processed_seq(&self) -> u64 {
        self.last_processed_seq.load(Ordering::SeqCst)
    }

    /// Wait until the view has processed at least the given sequence number
    /// Used for strong consistency guarantees in service handlers
    pub async fn wait_for_seq(&self, min_seq: u64) {
        if self.last_processed_seq() >= min_seq {
            return; // Already caught up
        }

        // Use notification-based waiting to avoid polling
        loop {
            let notified = self.seq_notifier.notified();

            // Check again in case we missed a notification
            if self.last_processed_seq() >= min_seq {
                return;
            }

            // Wait for next notification
            notified.await;
        }
    }

    /// Apply an event to update the view state
    /// This is called by the consumer handler when processing events
    async fn apply_event(&self, event: &Event) {
        match event {
            Event::AllowedOriginAdded {
                application_id,
                origin,
                ..
            } => {
                self.apply_allowed_origin_added(*application_id, origin)
                    .await;
            }
            Event::AllowedOriginRemoved {
                application_id,
                origin,
                ..
            } => {
                self.apply_allowed_origin_removed(*application_id, origin)
                    .await;
            }
            Event::Archived { application_id, .. } => {
                self.apply_archived(*application_id).await;
            }
            Event::Created {
                application_id,
                created_at,
                owner_identity_id,
                ..
            } => {
                self.apply_created(*application_id, *created_at, *owner_identity_id)
                    .await;
            }
            Event::HttpDomainLinked {
                application_id,
                http_domain,
                ..
            } => {
                self.apply_http_domain_linked(*application_id, http_domain)
                    .await;
            }
            Event::HttpDomainUnlinked {
                application_id,
                http_domain,
                ..
            } => {
                self.apply_http_domain_unlinked(*application_id, http_domain)
                    .await;
            }
            Event::OwnershipTransferred {
                application_id,
                new_owner_id,
                ..
            } => {
                self.apply_ownership_transferred(*application_id, *new_owner_id)
                    .await;
            }
        }
    }

    /// Apply `AllowedOriginAdded` event
    async fn apply_allowed_origin_added(&self, application_id: Uuid, origin: &Origin) {
        let mut apps = self.applications.write().await;

        if let Some(app) = apps.get_mut(&application_id) {
            app.allowed_origins.push(origin.clone());
        }
    }

    /// Apply `AllowedOriginRemoved` event
    async fn apply_allowed_origin_removed(&self, application_id: Uuid, origin: &Origin) {
        let mut apps = self.applications.write().await;

        if let Some(app) = apps.get_mut(&application_id) {
            app.allowed_origins.retain(|o| o != origin);
        }
    }

    /// Apply `Archived` event
    async fn apply_archived(&self, application_id: Uuid) {
        let mut apps = self.applications.write().await;

        // Clean up linked domains for this application
        if let Some(app) = apps.get(&application_id) {
            let mut domain_map = self.http_domain_to_application.write().await;
            for domain in &app.linked_http_domains {
                domain_map.remove(domain);
            }
        }
        apps.remove(&application_id);
    }

    /// Apply `Created` event
    async fn apply_created(
        &self,
        application_id: Uuid,
        created_at: chrono::DateTime<chrono::Utc>,
        owner_identity_id: Uuid,
    ) {
        let mut apps = self.applications.write().await;

        let app = Application {
            created_at,
            id: application_id,
            linked_http_domains: vec![],
            allowed_origins: vec![],
            owner_id: owner_identity_id,
        };
        apps.insert(application_id, app);

        drop(apps);
    }

    /// Apply `HttpDomainLinked` event
    async fn apply_http_domain_linked(&self, application_id: Uuid, http_domain: &Domain) {
        let mut apps = self.applications.write().await;

        if let Some(app) = apps.get_mut(&application_id) {
            app.linked_http_domains.push(http_domain.clone());
        }

        drop(apps);

        self.http_domain_to_application
            .write()
            .await
            .insert(http_domain.clone(), application_id);
    }

    /// Apply `HttpDomainUnlinked` event
    async fn apply_http_domain_unlinked(&self, application_id: Uuid, http_domain: &Domain) {
        let mut apps = self.applications.write().await;

        if let Some(app) = apps.get_mut(&application_id) {
            app.linked_http_domains
                .retain(|domain| domain != http_domain);
        }

        drop(apps);

        self.http_domain_to_application
            .write()
            .await
            .remove(http_domain);
    }

    /// Apply `OwnershipTransferred` event
    async fn apply_ownership_transferred(&self, application_id: Uuid, new_owner_id: Uuid) {
        let mut apps = self.applications.write().await;

        if let Some(app) = apps.get_mut(&application_id) {
            app.owner_id = new_owner_id;
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
    #[must_use]
    pub const fn new(view: ApplicationView) -> Self {
        Self { view }
    }

    /// Gets a reference to the application view.
    #[must_use]
    pub const fn view(&self) -> &ApplicationView {
        &self.view
    }
}

#[async_trait]
impl ConsumerHandler<Event, DeserializeError, SerializeError> for ApplicationViewConsumerHandler {
    type Error = Error;

    async fn handle(&self, event: Event, stream_sequence: u64) -> Result<(), Self::Error> {
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
