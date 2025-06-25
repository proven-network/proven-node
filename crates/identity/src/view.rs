use crate::{Error, Identity, events::IdentityEvent};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::consumer_handler::ConsumerHandler;
use tokio::sync::RwLock;
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Shared in-memory view of identities built from events
#[derive(Clone, Debug)]
pub struct IdentityView {
    // Use RwLock for efficient concurrent reads
    identities: Arc<RwLock<HashMap<Uuid, Identity>>>,
    // Map from PRF public key to identity ID
    prf_to_identity: Arc<RwLock<HashMap<Bytes, Uuid>>>,
}

impl IdentityView {
    /// Creates a new empty identity view.
    pub fn new() -> Self {
        Self {
            identities: Arc::new(RwLock::new(HashMap::new())),
            prf_to_identity: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get a single identity by ID
    pub async fn get_identity(&self, identity_id: Uuid) -> Option<Identity> {
        self.identities.read().await.get(&identity_id).cloned()
    }

    /// Get an identity by PRF public key
    pub async fn get_identity_by_prf_public_key(&self, prf_public_key: &Bytes) -> Option<Identity> {
        let prf_map = self.prf_to_identity.read().await;
        let identity_id = prf_map.get(prf_public_key)?;
        let identities = self.identities.read().await;
        identities.get(identity_id).cloned()
    }

    /// List all identities
    pub async fn list_all_identities(&self) -> Vec<Identity> {
        self.identities.read().await.values().cloned().collect()
    }

    /// Check if an identity exists
    pub async fn identity_exists(&self, identity_id: Uuid) -> bool {
        self.identities.read().await.contains_key(&identity_id)
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

    /// Apply an event to update the view state
    /// This is called by the consumer handler when processing events
    async fn apply_event(&self, event: &IdentityEvent) {
        match event {
            IdentityEvent::Created { identity_id, .. } => {
                let identity = Identity { id: *identity_id };
                let mut identities = self.identities.write().await;
                identities.insert(*identity_id, identity);
            }
            IdentityEvent::PrfPublicKeyLinked {
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
    pub fn new(view: IdentityView) -> Self {
        Self { view }
    }

    /// Gets a reference to the identity view.
    pub fn view(&self) -> &IdentityView {
        &self.view
    }
}

#[async_trait]
impl ConsumerHandler<IdentityEvent, DeserializeError, SerializeError>
    for IdentityViewConsumerHandler
{
    type Error = Error;

    async fn handle(&self, event: IdentityEvent, _stream_sequence: u64) -> Result<(), Self::Error> {
        // Apply the event to update the view
        self.view.apply_event(&event).await;
        Ok(())
    }

    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        // Called when the consumer has processed all existing events
        Ok(())
    }
}
