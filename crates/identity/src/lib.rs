//! Manages user identities and passkeys.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod identity;
mod passkey;

pub use error::Error;
pub use identity::Identity;
pub use passkey::Passkey;

use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use proven_store::Store;
use uuid::Uuid;

static CREATE_IDENTITIES_SQL: &str = include_str!("../sql/01_create_identities.sql");
static CREATE_LINKED_PASSKEYS_SQL: &str = include_str!("../sql/02_create_linked_passkeys.sql");

/// Options for creating a new `IdentityManager`
pub struct IdentityManagerOptions<IS, PS>
where
    IS: SqlStore,
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    /// The SQL store to use for storing identities.
    pub identity_store: IS,

    /// The KV store to use for storing passkeys.
    pub passkeys_store: PS,
}

/// Trait for managing user identities and passkeys.
#[async_trait]
pub trait IdentityManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Identity store type.
    type IdentityStore: SqlStore;

    /// Passkey store type.
    type PasskeyStore: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>;

    /// Creates a new instance of the identity manager.
    fn new(options: IdentityManagerOptions<Self::IdentityStore, Self::PasskeyStore>) -> Self;

    /// Gets an identity by ID.
    async fn get_identity(&self, identity_id: &str) -> Result<Option<Identity>, Error>;

    /// Gets an identity by passkey PRF public key, or creates a new one if it doesn't exist.
    async fn get_or_create_identity_by_passkey_prf_public_key(
        &self,
        passkey_prf_public_key_bytes: &Bytes,
    ) -> Result<Identity, Error>;

    /// Gets a passkey by ID.
    async fn get_passkey(&self, passkey_id: &Uuid) -> Result<Option<Passkey>, Error>;

    /// Saves a passkey.
    async fn save_passkey(&self, passkey_id: &Uuid, passkey: Passkey) -> Result<(), Error>;
}

/// Manages user identities and passkeys.
#[derive(Clone)]
pub struct IdentityManager<IS, PS>
where
    IS: SqlStore,
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    identity_store: IS,
    passkeys_store: PS,
}

#[async_trait]
impl<IS, PS> IdentityManagement for IdentityManager<IS, PS>
where
    IS: SqlStore,
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    type IdentityStore = IS;
    type PasskeyStore = PS;

    fn new(
        IdentityManagerOptions {
            identity_store,
            passkeys_store,
        }: IdentityManagerOptions<IS, PS>,
    ) -> Self {
        Self {
            identity_store,
            passkeys_store,
        }
    }

    async fn get_identity(&self, _identity_id: &str) -> Result<Option<Identity>, Error> {
        unimplemented!()
    }

    async fn get_or_create_identity_by_passkey_prf_public_key(
        &self,
        passkey_prf_public_key_bytes: &Bytes,
    ) -> Result<Identity, Error> {
        let connection = self
            .identity_store
            .connect(vec![CREATE_IDENTITIES_SQL, CREATE_LINKED_PASSKEYS_SQL])
            .await
            .map_err(|e| Error::IdentityStore(e.to_string()))?;

        let mut rows = connection
            .query(
                r"
                    SELECT id
                    FROM identities
                    JOIN linked_passkeys ON identities.id = linked_passkeys.identity_id
                    WHERE linked_passkeys.prf_public_key = ?1
                "
                .trim(),
                vec![SqlParam::Blob(passkey_prf_public_key_bytes.clone())],
            )
            .await
            .map_err(|e| Error::IdentityStore(e.to_string()))?;

        // Create new identity if no rows found
        let Some(first_row) = rows.next().await else {
            let identity_id = Uuid::new_v4();

            connection
                .execute(
                    "INSERT INTO identities (id, created_at, updated_at) VALUES (?1, ?2, ?3)",
                    vec![
                        SqlParam::Blob(identity_id.as_bytes().to_vec().into()),
                        SqlParam::Integer(
                            SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64,
                        ),
                        SqlParam::Integer(
                            SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64,
                        ),
                    ],
                )
                .await
                .map_err(|e| Error::IdentityStore(e.to_string()))?;

            connection
                .execute(
                    "INSERT INTO linked_passkeys (prf_public_key, identity_id, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
                    vec![
                        SqlParam::Blob(passkey_prf_public_key_bytes.clone()),
                        SqlParam::Blob(identity_id.as_bytes().to_vec().into()),
                        SqlParam::Integer(
                            SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64,
                        ),
                        SqlParam::Integer(
                            SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64,
                        ),
                    ],
                )
                .await
                .map_err(|e| Error::IdentityStore(e.to_string()))?;

            return Ok(Identity {
                identity_id,
                passkeys: vec![],
            });
        };

        // Get identity ID from first row
        let identity_id = match &first_row[0] {
            SqlParam::Blob(blob) => Uuid::from_slice(blob).unwrap(),
            SqlParam::BlobWithName(_, blob) => Uuid::from_slice(blob).unwrap(),
            other => unreachable!("Unexpected SQL param: {:?}", other),
        };

        Ok(Identity {
            identity_id,
            passkeys: vec![],
        })
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
