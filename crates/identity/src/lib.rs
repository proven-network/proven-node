//! Manages all user sessions (created via ROLA) and their associated data.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod identity;
mod passkey;
mod session;
mod whoami;

pub use error::Error;
pub use identity::Identity;
pub use passkey::Passkey;
pub use session::Session;
pub use whoami::WhoAmI;

use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use futures::StreamExt;
use proven_attestation::{AttestationParams, Attestor};
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use proven_store::{Store, Store1};
use rand::thread_rng;
use tracing::{debug, info};
use uuid::Uuid;

static CREATE_IDENTITIES_SQL: &str = include_str!("../sql/01_create_identities.sql");
static CREATE_LINKED_PASSKEYS_SQL: &str = include_str!("../sql/02_create_linked_passkeys.sql");

/// Options for creating a new `IdentityManager`
pub struct IdentityManagerOptions<A, IS, PS, SS>
where
    A: Attestor,
    IS: SqlStore,
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    /// The attestor to use for remote attestation.
    pub attestor: A,

    /// The SQL store to use for storing identities.
    pub identity_store: IS,

    /// The KV store to use for storing passkeys.
    pub passkeys_store: PS,

    /// The KV store to use for storing sessions.
    pub sessions_store: SS,
}

/// Options for creating a new anonymous session.
pub struct CreateAnonymousSessionOptions<'a> {
    /// The application ID.
    pub application_id: &'a str,

    /// Challenge used in remote attestation.
    pub nonce: &'a Bytes,

    /// The origin of the request.
    pub origin: &'a str,

    /// The verifying key of the client.
    pub verifying_key: &'a VerifyingKey,
}

/// Trait for managing user sessions.
#[async_trait]
pub trait IdentityManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Attestor type.
    type Attestor: Attestor;

    /// Identity store type.
    type IdentityStore: SqlStore;

    /// Passkey store type.
    type PasskeyStore: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>;

    /// Session store type.
    type SessionStore: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>;

    /// Creates a new instance of the session manager.
    fn new(
        options: IdentityManagerOptions<
            Self::Attestor,
            Self::IdentityStore,
            Self::PasskeyStore,
            Self::SessionStore,
        >,
    ) -> Self;

    /// Creates a new anonymous session.
    async fn create_anonymous_session(
        &self,
        options: CreateAnonymousSessionOptions<'_>,
    ) -> Result<Bytes, Error>;

    /// Gets an identity by ID.
    async fn get_identity(&self, identity_id: &str) -> Result<Option<Identity>, Error>;

    /// Gets an identity by passkey PRF public key, or creates a new one if it doesn't exist.
    async fn get_or_create_identity_by_passkey_prf_public_key(
        &self,
        passkey_prf_public_key_bytes: &Bytes,
    ) -> Result<Identity, Error>;

    /// Gets a passkey by ID.
    async fn get_passkey(&self, passkey_id: &Uuid) -> Result<Option<Passkey>, Error>;

    /// Gets a session by ID.
    async fn get_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
    ) -> Result<Option<Session>, Error>;

    /// Identifies a session
    async fn identify_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
        identity_id: &Uuid,
    ) -> Result<Session, Error>;

    /// Saves a passkey.
    async fn save_passkey(&self, passkey_id: &Uuid, passkey: Passkey) -> Result<(), Error>;
}

/// Manages all user sessions (created via ROLA) and their associated data.
#[derive(Clone)]
pub struct IdentityManager<A, IS, PS, SS>
where
    A: Attestor,
    IS: SqlStore,
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    attestor: A,
    identity_store: IS,
    passkeys_store: PS,
    sessions_store: SS,
}

#[async_trait]
impl<A, IS, PS, SS> IdentityManagement for IdentityManager<A, IS, PS, SS>
where
    A: Attestor,
    IS: SqlStore,
    PS: Store<Passkey, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    type Attestor = A;
    type IdentityStore = IS;
    type PasskeyStore = PS;
    type SessionStore = SS;

    fn new(
        IdentityManagerOptions {
            attestor,
            identity_store,
            passkeys_store,
            sessions_store,
        }: IdentityManagerOptions<A, IS, PS, SS>,
    ) -> Self {
        Self {
            attestor,
            identity_store,
            passkeys_store: passkeys_store,
            sessions_store,
        }
    }

    async fn create_anonymous_session(
        &self,
        CreateAnonymousSessionOptions {
            application_id,
            nonce,
            origin,
            verifying_key,
        }: CreateAnonymousSessionOptions<'_>,
    ) -> Result<Bytes, Error> {
        let session_id = Uuid::new_v4();

        let server_signing_key = SigningKey::generate(&mut thread_rng());
        let server_public_key = server_signing_key.verifying_key();

        let session = Session::Anonymous {
            origin: origin.to_string(),
            session_id: session_id.clone(),
            signing_key: server_signing_key.clone(),
            verifying_key: *verifying_key,
        };

        info!(
            "Creating anonymous session (id: {}) for application: {}",
            session_id, application_id
        );

        self.sessions_store
            .scope(application_id)
            .put(session_id.to_string(), session.clone())
            .await
            .map_err(|e| Error::SessionStore(e.to_string()))?;

        match self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce.clone()),
                user_data: Some(Bytes::from(session_id.as_bytes().to_vec())),
                public_key: Some(Bytes::from(server_public_key.to_bytes().to_vec())),
            })
            .await
        {
            Ok(attestation) => Ok(attestation),
            Err(e) => Err(Error::Attestation(e.to_string())),
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

    async fn get_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
    ) -> Result<Option<Session>, Error> {
        debug!(
            "Getting session (id: {}) for application: {}",
            session_id, application_id
        );

        match self
            .sessions_store
            .scope(application_id.to_string())
            .get(session_id.to_string())
            .await
        {
            Ok(Some(session)) => Ok(Some(session)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::SessionStore(e.to_string())),
        }
    }

    async fn identify_session(
        &self,
        application_id: &Uuid,
        session_id: &Uuid,
        identity_id: &Uuid,
    ) -> Result<Session, Error> {
        let new_session = match self.get_session(application_id, session_id).await? {
            Some(session) => match session {
                Session::Anonymous {
                    origin,
                    session_id,
                    signing_key,
                    verifying_key,
                } => {
                    let new_session = Session::Identified {
                        identity_id: identity_id.clone(),
                        origin,
                        session_id,
                        signing_key,
                        verifying_key,
                    };

                    self.sessions_store
                        .scope(application_id.to_string())
                        .put(session_id.to_string(), new_session.clone())
                        .await
                        .map_err(|e| Error::SessionStore(e.to_string()))?;

                    new_session
                }
                Session::Identified { .. } => {
                    return Err(Error::SessionStore(
                        "Session already identified".to_string(),
                    ));
                }
            },
            None => {
                return Err(Error::SessionStore("Session not found".to_string()));
            }
        };

        Ok(new_session)
    }

    async fn save_passkey(&self, passkey_id: &Uuid, passkey: Passkey) -> Result<(), Error> {
        self.passkeys_store
            .put(passkey_id.to_string(), passkey)
            .await
            .map_err(|e| Error::PasskeyStore(e.to_string()))
    }
}
