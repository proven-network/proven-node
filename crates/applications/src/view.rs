use crate::{Application, Event};

use arc_swap::ArcSwap;
use proven_util::{Domain, Origin};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;
use uuid::Uuid;

// Moved DeserializeError and SerializeError to error.rs

/// Shared in-memory view of applications built from events
#[derive(Clone)]
pub struct ApplicationView {
    // Use ArcSwap for lock-free concurrent reads
    applications: Arc<ArcSwap<HashMap<Uuid, Application>>>,

    // Map from HTTP domain to application ID
    http_domain_to_application: Arc<ArcSwap<HashMap<Domain, Uuid>>>,

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
            applications: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            http_domain_to_application: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            last_processed_seq: Arc::new(AtomicU64::new(0)),
            seq_notifier: Arc::new(Notify::new()),
        }
    }

    /// Check if an application exists
    #[must_use]
    pub fn application_exists(&self, application_id: &Uuid) -> bool {
        self.applications.load().contains_key(application_id)
    }

    /// Get a single application by ID
    #[must_use]
    pub fn get_application(&self, application_id: &Uuid) -> Option<Application> {
        self.applications.load().get(application_id).cloned()
    }

    /// Get an application by HTTP domain
    #[must_use]
    pub fn get_application_id_for_http_domain(&self, http_domain: &Domain) -> Option<Uuid> {
        self.http_domain_to_application
            .load()
            .get(http_domain)
            .copied()
    }

    /// Check if an HTTP domain is linked to an application
    #[must_use]
    pub fn http_domain_linked(&self, http_domain: &Domain) -> bool {
        self.http_domain_to_application
            .load()
            .contains_key(http_domain)
    }

    /// Get all applications (for admin/debugging purposes)
    #[must_use]
    pub fn list_all_applications(&self) -> Vec<Application> {
        self.applications.load().values().cloned().collect()
    }

    /// List all applications owned by a specific user
    #[must_use]
    pub fn list_applications_by_owner(&self, owner_id: &Uuid) -> Vec<Application> {
        self.applications
            .load()
            .values()
            .filter(|app| app.owner_id == *owner_id)
            .cloned()
            .collect()
    }

    /// Get the count of applications
    #[must_use]
    pub fn application_count(&self) -> usize {
        self.applications.load().len()
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

    /// Update the last processed sequence number.
    pub fn update_last_processed_seq(&self, seq: u64) {
        self.last_processed_seq.store(seq, Ordering::SeqCst);
        self.seq_notifier.notify_waiters();
    }

    /// Apply an event to update the view state
    /// This is called by the consumer handler when processing events
    pub fn apply_event(&self, event: &Event) {
        match event {
            Event::AllowedOriginAdded {
                application_id,
                origin,
                ..
            } => {
                self.apply_allowed_origin_added(*application_id, origin);
            }
            Event::AllowedOriginRemoved {
                application_id,
                origin,
                ..
            } => {
                self.apply_allowed_origin_removed(*application_id, origin);
            }
            Event::Archived { application_id, .. } => {
                self.apply_archived(*application_id);
            }
            Event::Created {
                application_id,
                created_at,
                owner_identity_id,
                ..
            } => {
                self.apply_created(*application_id, *created_at, *owner_identity_id);
            }
            Event::HttpDomainLinked {
                application_id,
                http_domain,
                ..
            } => {
                self.apply_http_domain_linked(*application_id, http_domain);
            }
            Event::HttpDomainUnlinked {
                application_id,
                http_domain,
                ..
            } => {
                self.apply_http_domain_unlinked(*application_id, http_domain);
            }
            Event::OwnershipTransferred {
                application_id,
                new_owner_id,
                ..
            } => {
                self.apply_ownership_transferred(*application_id, *new_owner_id);
            }
        }
    }

    /// Apply `AllowedOriginAdded` event
    fn apply_allowed_origin_added(&self, application_id: Uuid, origin: &Origin) {
        let current = self.applications.load();
        if current.contains_key(&application_id) {
            let mut new_apps = (**current).clone();
            if let Some(app_mut) = new_apps.get_mut(&application_id) {
                app_mut.allowed_origins.push(origin.clone());
            }
            self.applications.store(Arc::new(new_apps));
        }
    }

    /// Apply `AllowedOriginRemoved` event
    fn apply_allowed_origin_removed(&self, application_id: Uuid, origin: &Origin) {
        let current = self.applications.load();
        if current.contains_key(&application_id) {
            let mut new_apps = (**current).clone();
            if let Some(app_mut) = new_apps.get_mut(&application_id) {
                app_mut.allowed_origins.retain(|o| o != origin);
            }
            self.applications.store(Arc::new(new_apps));
        }
    }

    /// Apply `Archived` event
    fn apply_archived(&self, application_id: Uuid) {
        // First update applications
        let current_apps = self.applications.load();
        if let Some(app) = current_apps.get(&application_id) {
            // Clean up linked domains
            let domains_to_remove = app.linked_http_domains.clone();

            // Remove from domain map
            let current_domains = self.http_domain_to_application.load();
            let mut new_domains = (**current_domains).clone();
            for domain in &domains_to_remove {
                new_domains.remove(domain);
            }
            self.http_domain_to_application.store(Arc::new(new_domains));

            // Remove from applications
            let mut new_apps = (**current_apps).clone();
            new_apps.remove(&application_id);
            self.applications.store(Arc::new(new_apps));
        }
    }

    /// Apply `Created` event
    fn apply_created(
        &self,
        application_id: Uuid,
        created_at: chrono::DateTime<chrono::Utc>,
        owner_identity_id: Uuid,
    ) {
        let app = Application {
            created_at,
            id: application_id,
            linked_http_domains: vec![],
            allowed_origins: vec![],
            owner_id: owner_identity_id,
        };

        let current = self.applications.load();
        let mut new_apps = (**current).clone();
        new_apps.insert(application_id, app);
        self.applications.store(Arc::new(new_apps));
    }

    /// Apply `HttpDomainLinked` event
    fn apply_http_domain_linked(&self, application_id: Uuid, http_domain: &Domain) {
        // Update applications
        let current_apps = self.applications.load();
        let mut new_apps = (**current_apps).clone();
        if let Some(app) = new_apps.get_mut(&application_id) {
            app.linked_http_domains.push(http_domain.clone());
        }
        self.applications.store(Arc::new(new_apps));

        // Update domain mapping
        let current_domains = self.http_domain_to_application.load();
        let mut new_domains = (**current_domains).clone();
        new_domains.insert(http_domain.clone(), application_id);
        self.http_domain_to_application.store(Arc::new(new_domains));
    }

    /// Apply `HttpDomainUnlinked` event
    fn apply_http_domain_unlinked(&self, application_id: Uuid, http_domain: &Domain) {
        // Update applications
        let current_apps = self.applications.load();
        let mut new_apps = (**current_apps).clone();
        if let Some(app) = new_apps.get_mut(&application_id) {
            app.linked_http_domains
                .retain(|domain| domain != http_domain);
        }
        self.applications.store(Arc::new(new_apps));

        // Update domain mapping
        let current_domains = self.http_domain_to_application.load();
        let mut new_domains = (**current_domains).clone();
        new_domains.remove(http_domain);
        self.http_domain_to_application.store(Arc::new(new_domains));
    }

    /// Apply `OwnershipTransferred` event
    fn apply_ownership_transferred(&self, application_id: Uuid, new_owner_id: Uuid) {
        let current = self.applications.load();
        let mut new_apps = (**current).clone();
        if let Some(app) = new_apps.get_mut(&application_id) {
            app.owner_id = new_owner_id;
        }
        self.applications.store(Arc::new(new_apps));
    }
}

impl Default for ApplicationView {
    fn default() -> Self {
        Self::new()
    }
}
