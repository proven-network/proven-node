use crate::{Error, Event, Identity};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::consumer_handler::ConsumerHandler;
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Shared in-memory view of identities built from events
#[derive(Clone, Debug)]
pub struct IdentityView {
    // Use RwLock for efficient concurrent reads
    identities: Arc<RwLock<HashMap<Uuid, Identity>>>,

    // Track the sequence number of the last processed event
    last_processed_seq: Arc<AtomicU64>,

    // Map from PRF public key to identity ID
    prf_to_identity: Arc<RwLock<HashMap<Bytes, Uuid>>>,

    // Notifier for when sequence number advances (for consistency waiting)
    seq_notifier: Arc<Notify>,
}

impl IdentityView {
    /// Creates a new empty identity view.
    #[must_use]
    pub fn new() -> Self {
        Self {
            identities: Arc::new(RwLock::new(HashMap::new())),
            last_processed_seq: Arc::new(AtomicU64::new(0)),
            prf_to_identity: Arc::new(RwLock::new(HashMap::new())),
            seq_notifier: Arc::new(Notify::new()),
        }
    }

    /// Get a single identity by ID
    pub async fn get_identity(&self, identity_id: &Uuid) -> Option<Identity> {
        self.identities.read().await.get(identity_id).cloned()
    }

    /// Get an identity by PRF public key
    pub async fn get_identity_by_prf_public_key(&self, prf_public_key: &Bytes) -> Option<Identity> {
        let identity_id = {
            let prf_map = self.prf_to_identity.read().await;
            *prf_map.get(prf_public_key)?
        };
        let identities = self.identities.read().await;
        identities.get(&identity_id).cloned()
    }

    /// List all identities
    pub async fn list_all_identities(&self) -> Vec<Identity> {
        self.identities.read().await.values().cloned().collect()
    }

    /// Check if an identity exists
    pub async fn identity_exists(&self, identity_id: &Uuid) -> bool {
        self.identities.read().await.contains_key(identity_id)
    }

    /// Check if a PRF public key is linked to any identity
    pub async fn prf_public_key_exists(&self, prf_public_key: &Bytes) -> bool {
        self.prf_to_identity
            .read()
            .await
            .contains_key(prf_public_key)
    }

    /// Get the count of identities
    pub async fn identity_count(&self) -> usize {
        self.identities.read().await.len()
    }

    /// Get the count of linked PRF public keys
    pub async fn prf_public_key_count(&self) -> usize {
        self.prf_to_identity.read().await.len()
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
            Event::Created { identity_id, .. } => {
                let identity = Identity { id: *identity_id };
                let mut identities = self.identities.write().await;
                identities.insert(*identity_id, identity);
            }
            Event::PrfPublicKeyLinked {
                identity_id,
                prf_public_key,
                ..
            } => {
                let mut prf_map = self.prf_to_identity.write().await;
                prf_map.insert(prf_public_key.clone(), *identity_id);
            }
        }
    }
}

impl Default for IdentityView {
    fn default() -> Self {
        Self::new()
    }
}

/// Consumer handler that processes events from the event stream to update the view
#[derive(Clone, Debug)]
pub struct IdentityViewConsumerHandler {
    view: IdentityView,
}

impl IdentityViewConsumerHandler {
    /// Creates a new consumer handler with the given view.
    ///
    /// # Arguments
    ///
    /// * `view` - The identity view to update when events are processed
    #[must_use]
    pub const fn new(view: IdentityView) -> Self {
        Self { view }
    }

    /// Gets a reference to the identity view.
    #[must_use]
    pub const fn view(&self) -> &IdentityView {
        &self.view
    }
}

#[async_trait]
impl ConsumerHandler<Event, DeserializeError, SerializeError> for IdentityViewConsumerHandler {
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
