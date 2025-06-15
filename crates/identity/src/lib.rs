//! Manages all user sessions (created via ROLA) and their associated data.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod identity;
mod ledger_identity;
mod session;
mod whoami;

pub use error::Error;
pub use identity::Identity;
pub use ledger_identity::LedgerIdentity;
pub use ledger_identity::radix::RadixIdentityDetails;
pub use session::Session;
pub use whoami::WhoAmI;

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_attestation::{AttestationParams, Attestor};
use proven_store::{Store, Store1, Store2};
use rand::{Rng, thread_rng};
use tracing::{debug, info};
use uuid::Uuid;

/// Options for creating a new `IdentityManager`
pub struct IdentityManagerOptions<A, CS, SS>
where
    A: Attestor,
    CS: Store2,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    /// The attestor to use for remote attestation.
    pub attestor: A,

    /// The KV store to use for storing challenges.
    pub challenge_store: CS,

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

    /// Challenge store type.
    type ChallengeStore: Store2;

    /// Session store type.
    type SessionStore: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>;

    /// Creates a new instance of the session manager.
    fn new(
        options: IdentityManagerOptions<Self::Attestor, Self::ChallengeStore, Self::SessionStore>,
    ) -> Self;

    /// Creates a new anonymous session.
    async fn create_anonymous_session(
        &self,
        options: CreateAnonymousSessionOptions<'_>,
    ) -> Result<Bytes, Error>;

    /// Creates a new ROLA challenge to use for session identifiction.
    async fn create_rola_challenge(
        &self,
        application_id: &str,
        origin: &str,
    ) -> Result<String, Error>;

    /// Gets an identity by ID.
    async fn get_identity(&self, identity_id: &str) -> Result<Option<Identity>, Error>;

    /// Gets an identity by passkey PRF public key.
    async fn get_identity_by_passkey_prf_public_key(
        &self,
        passkey_prf_public_key: &str,
    ) -> Result<Option<Identity>, Error>;

    /// Gets a session by ID.
    async fn get_session(
        &self,
        application_id: &str,
        session_id: &str,
    ) -> Result<Option<Session>, Error>;

    /// Identifies a session
    async fn identify_session(&self, session_id: &str, identity_id: &str) -> Result<Bytes, Error>;
}

/// Manages all user sessions (created via ROLA) and their associated data.
#[derive(Clone)]
pub struct IdentityManager<A, CS, SS>
where
    A: Attestor,
    CS: Store2,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    attestor: A,
    challenge_store: CS,
    sessions_store: SS,
}

#[async_trait]
impl<A, CS, SS> IdentityManagement for IdentityManager<A, CS, SS>
where
    A: Attestor,
    CS: Store2,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    type Attestor = A;
    type ChallengeStore = CS;
    type SessionStore = SS;

    fn new(
        IdentityManagerOptions {
            attestor,
            challenge_store,
            sessions_store,
        }: IdentityManagerOptions<A, CS, SS>,
    ) -> Self {
        Self {
            attestor,
            challenge_store,
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

    async fn create_rola_challenge(
        &self,
        application_id: &str,
        origin: &str,
    ) -> Result<String, Error> {
        let mut challenge = String::new();

        for _ in 0..32 {
            challenge.push_str(&format!("{:02x}", thread_rng().r#gen::<u8>()));
        }

        self.challenge_store
            .scope(application_id)
            .scope(origin)
            .put(challenge.clone(), Bytes::from_static(&[1u8]))
            .await
            .map_err(|e| Error::ChallengeStore(e.to_string()))?;

        Ok(challenge)
    }

    async fn get_identity(&self, _identity_id: &str) -> Result<Option<Identity>, Error> {
        unimplemented!()
    }

    async fn get_identity_by_passkey_prf_public_key(
        &self,
        _passkey_prf_public_key: &str,
    ) -> Result<Option<Identity>, Error> {
        todo!()
    }

    async fn get_session(
        &self,
        application_id: &str,
        session_id: &str,
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
        _session_id: &str,
        _identity_id: &str,
    ) -> Result<Bytes, Error> {
        unimplemented!()
    }
}
