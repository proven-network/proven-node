//! Manages user passkeys.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod passkey;

pub use error::Error;
pub use passkey::Passkey;

use async_trait::async_trait;
use proven_store::Store;
use uuid::Uuid;

/// Options for creating a new `PasskeyManager`
pub struct PasskeyManagerOptions<PS>
where
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    /// The KV store to use for storing passkeys.
    pub passkeys_store: PS,
}

/// Trait for managing user passkeys.
#[async_trait]
pub trait PasskeyManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Passkey store type.
    type PasskeyStore: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>;

    /// Creates a new instance of the passkey manager.
    fn new(options: PasskeyManagerOptions<Self::PasskeyStore>) -> Self;

    /// Gets a passkey by ID.
    async fn get_passkey(&self, passkey_id: &Uuid) -> Result<Option<Passkey>, Error>;

    /// Saves a passkey.
    async fn save_passkey(&self, passkey_id: &Uuid, passkey: Passkey) -> Result<(), Error>;
}

/// Manages user passkeys.
#[derive(Clone)]
pub struct PasskeyManager<PS>
where
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    passkeys_store: PS,
}

#[async_trait]
impl<PS> PasskeyManagement for PasskeyManager<PS>
where
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    type PasskeyStore = PS;

    fn new(PasskeyManagerOptions { passkeys_store }: PasskeyManagerOptions<PS>) -> Self {
        Self { passkeys_store }
    }

    async fn get_passkey(&self, passkey_id: &Uuid) -> Result<Option<Passkey>, Error> {
        self.passkeys_store
            .get(passkey_id.to_string())
            .await
            .map_err(|e| Error::PasskeyStore(e.to_string()))
    }

    async fn save_passkey(&self, passkey_id: &Uuid, passkey: Passkey) -> Result<(), Error> {
        self.passkeys_store
            .put(passkey_id.to_string(), passkey)
            .await
            .map_err(|e| Error::PasskeyStore(e.to_string()))
    }
}
