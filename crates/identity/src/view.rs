//! In-memory view of identity state built from events.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use bytes::Bytes;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::{Event, Identity};

/// In-memory view of all identities, built from consuming events.
#[derive(Clone)]
pub struct IdentityView {
    /// All identities indexed by ID.
    identities: Arc<ArcSwap<HashMap<Uuid, Identity>>>,

    /// Mapping from PRF public key to identity ID.
    prf_public_keys_to_identities: Arc<ArcSwap<HashMap<Bytes, Uuid>>>,

    /// The last processed event sequence number.
    last_processed_seq: Arc<AtomicU64>,

    /// Notify for sequence updates.
    seq_notify: Arc<Notify>,
}

impl IdentityView {
    /// Create a new empty view.
    #[must_use]
    pub fn new() -> Self {
        Self {
            identities: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            prf_public_keys_to_identities: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            last_processed_seq: Arc::new(AtomicU64::new(0)),
            seq_notify: Arc::new(Notify::new()),
        }
    }

    /// Apply an event to update the view.
    pub fn apply_event(&self, event: &Event) {
        match event {
            Event::Created { identity_id, .. } => {
                let identity = Identity::new(*identity_id);
                let current = self.identities.load();
                let mut new_identities = (**current).clone();
                new_identities.insert(*identity_id, identity);
                self.identities.store(Arc::new(new_identities));
                tracing::debug!("Applied Created event for identity {}", identity_id);
            }
            Event::PrfPublicKeyLinked {
                identity_id,
                prf_public_key,
                ..
            } => {
                let current = self.prf_public_keys_to_identities.load();
                let mut new_mapping = (**current).clone();
                new_mapping.insert(prf_public_key.clone(), *identity_id);
                self.prf_public_keys_to_identities
                    .store(Arc::new(new_mapping));
                tracing::debug!(
                    "Applied PrfPublicKeyLinked event for identity {} with key {:?}",
                    identity_id,
                    prf_public_key
                );
            }
        }
    }

    /// Update the last processed sequence number.
    pub fn update_last_processed_seq(&self, seq: u64) {
        self.last_processed_seq.store(seq, Ordering::SeqCst);
        self.seq_notify.notify_waiters();
    }

    /// Wait for a specific sequence number to be processed.
    pub async fn wait_for_seq(&self, target_seq: u64) {
        loop {
            let current_seq = self.last_processed_seq.load(Ordering::SeqCst);
            if current_seq >= target_seq {
                return;
            }
            self.seq_notify.notified().await;
        }
    }

    /// Get the last processed sequence number.
    #[must_use]
    pub fn last_processed_seq(&self) -> u64 {
        self.last_processed_seq.load(Ordering::SeqCst)
    }

    /// Get an identity by its ID.
    #[must_use]
    pub fn get_identity(&self, identity_id: &Uuid) -> Option<Identity> {
        self.identities.load().get(identity_id).copied()
    }

    /// Get an identity by PRF public key.
    #[must_use]
    pub fn get_identity_by_prf_public_key(&self, prf_public_key: &Bytes) -> Option<Identity> {
        let mapping = self.prf_public_keys_to_identities.load();
        mapping
            .get(prf_public_key)
            .and_then(|identity_id| self.identities.load().get(identity_id).copied())
    }

    /// Check if an identity exists.
    #[must_use]
    pub fn identity_exists(&self, identity_id: &Uuid) -> bool {
        self.identities.load().contains_key(identity_id)
    }

    /// Check if a PRF public key exists.
    #[must_use]
    pub fn prf_public_key_exists(&self, prf_public_key: &Bytes) -> bool {
        self.prf_public_keys_to_identities
            .load()
            .contains_key(prf_public_key)
    }

    /// List all identities.
    #[must_use]
    pub fn list_all_identities(&self) -> Vec<Identity> {
        self.identities.load().values().copied().collect()
    }

    /// Get the count of identities.
    #[must_use]
    pub fn identity_count(&self) -> usize {
        self.identities.load().len()
    }

    /// Get the count of PRF public keys.
    #[must_use]
    pub fn prf_public_key_count(&self) -> usize {
        self.prf_public_keys_to_identities.load().len()
    }
}

impl Default for IdentityView {
    fn default() -> Self {
        Self::new()
    }
}
