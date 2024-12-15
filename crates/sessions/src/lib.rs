//! Manages all user sessions (created via ROLA) and their associated data.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod session;

use error::Error;
pub use session::Session;

use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_attestation::{AttestationParams, Attestor, AttestorError};
use proven_radix_rola::{Rola, RolaOptions, SignedChallenge, Type as SignedChallengeType};
use proven_store::{Store, Store1, StoreError};
use radix_common::network::NetworkDefinition;
use rand::{thread_rng, Rng};

/// Options for creating a new `SessionManager`
pub struct SessionManagerOptions<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    /// The attestor to use for remote attestation.
    pub attestor: A,

    /// The KV store to use for storing challenges.
    pub challenge_store: CS,

    /// The KV store to use for storing sessions.
    pub sessions_store: SS,

    /// The origin of the Radix gateway.
    pub radix_gateway_origin: String,

    /// Radix network definition.
    pub radix_network_definition: NetworkDefinition,
}

/// Options for creating a new session.
pub struct CreateSessionOptions {
    /// The application ID.
    pub application_id: String,

    /// The application name.
    pub application_name: Option<String>,

    /// The dApp definition address.
    pub dapp_definition_address: String,

    /// Challenge used in remote attestation.
    pub nonce: Bytes,

    /// The origin of the request.
    pub origin: String,

    /// Signed ROLA challenges.
    pub signed_challenges: Vec<SignedChallenge>,

    /// The verifying key of the client.
    pub verifying_key: VerifyingKey,
}

/// Trait for managing user sessions.
#[async_trait]
pub trait SessionManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Attestor type.
    type Attestor: Attestor;

    /// Attestor error type.
    type AttestorError: AttestorError;

    /// Challenge store type.
    type ChallengeStore: Store1;

    /// Challenge store error type.
    type ChallengeStoreError: StoreError;

    /// Session store type.
    type SessionStore: Store1<
        Session,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >;

    /// Session store error type.
    type SessionStoreError: StoreError;

    /// Creates a new instance of the session manager.
    fn new(
        options: SessionManagerOptions<Self::Attestor, Self::ChallengeStore, Self::SessionStore>,
    ) -> Self;

    /// Creates a new challenge to use for session creation.
    async fn create_challenge(
        &self,
        origin: String,
    ) -> Result<
        String,
        Error<Self::AttestorError, Self::ChallengeStoreError, Self::SessionStoreError>,
    >;

    /// Creates a new session.
    async fn create_session(
        &self,
        params: CreateSessionOptions,
    ) -> Result<Bytes, Error<Self::AttestorError, Self::ChallengeStoreError, Self::SessionStoreError>>;

    /// Gets a session by its ID.
    async fn get_session(
        &self,
        application_id: String,
        session_id: String,
    ) -> Result<
        Option<Session>,
        Error<Self::AttestorError, Self::ChallengeStoreError, Self::SessionStoreError>,
    >;
}

/// Manages all user sessions (created via ROLA) and their associated data.
#[derive(Clone)]
pub struct SessionManager<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    attestor: A,
    challenge_store: CS,
    sessions_store: SS,
    radix_gateway_origin: String,
    radix_network_definition: NetworkDefinition,
}

#[async_trait]
impl<A, CS, SS> SessionManagement for SessionManager<A, CS, SS>
where
    A: Attestor,
    CS: Store1,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    type Attestor = A;
    type AttestorError = A::Error;
    type ChallengeStore = CS;
    type ChallengeStoreError = CS::Error;
    type SessionStore = SS;
    type SessionStoreError = SS::Error;

    fn new(
        SessionManagerOptions {
            attestor,
            challenge_store,
            sessions_store,
            radix_gateway_origin,
            radix_network_definition,
        }: SessionManagerOptions<A, CS, SS>,
    ) -> Self {
        Self {
            attestor,
            challenge_store,
            sessions_store,
            radix_gateway_origin,
            radix_network_definition,
        }
    }

    async fn create_challenge(
        &self,
        origin: String,
    ) -> Result<String, Error<A::Error, CS::Error, SS::Error>> {
        let mut challenge = String::new();

        for _ in 0..32 {
            challenge.push_str(&format!("{:02x}", thread_rng().gen::<u8>()));
        }

        self.challenge_store
            .scope(origin)
            .put(challenge.clone(), Bytes::from_static(&[1u8]))
            .await
            .map_err(Error::ChallengeStore)?;

        Ok(challenge)
    }

    async fn create_session(
        &self,
        CreateSessionOptions {
            application_id,
            application_name,
            dapp_definition_address,
            nonce,
            origin,
            signed_challenges,
            verifying_key,
        }: CreateSessionOptions,
    ) -> Result<Bytes, Error<A::Error, CS::Error, SS::Error>> {
        let rola = Rola::new(RolaOptions {
            application_name: application_name.clone().unwrap_or_default(),
            dapp_definition_address: dapp_definition_address.clone(),
            expected_origin: origin.clone(),
            gateway_url: self.radix_gateway_origin.clone(),
            network_definition: self.radix_network_definition.clone(),
        });

        let mut challenges = HashSet::new();
        for signed_challenge in signed_challenges.clone() {
            challenges.insert(signed_challenge.challenge.clone());
        }

        for challenge in challenges {
            let scoped_challenge_store = self.challenge_store.scope(origin.clone());

            match scoped_challenge_store.get(challenge.clone()).await {
                Ok(_) => {
                    scoped_challenge_store
                        .delete(challenge.clone())
                        .await
                        .map_err(Error::ChallengeStore)?;
                }
                Err(e) => {
                    return Err(Error::ChallengeStore(e));
                }
            }
        }

        let identity_addresses = signed_challenges
            .iter()
            .filter(|sc| sc.r#type == SignedChallengeType::Persona)
            .map(|sc| sc.address.clone())
            .collect::<Vec<String>>();

        if identity_addresses.len() != 1 {
            return Err(Error::SignedChallengeInvalid);
        }

        for signed_challenge in signed_challenges.clone() {
            if (rola.verify_signed_challenge(signed_challenge).await).is_err() {
                return Err(Error::SignedChallengeInvalid);
            }
        }

        let server_signing_key = SigningKey::generate(&mut thread_rng());
        let server_public_key = server_signing_key.verifying_key();

        let account_addresses = signed_challenges
            .iter()
            .filter(|sc| sc.r#type == SignedChallengeType::Account)
            .map(|sc| sc.address.clone())
            .collect::<Vec<String>>();

        let mut session_id_bytes = [0u8; 32];
        thread_rng().fill(&mut session_id_bytes);

        let session = Session {
            account_addresses,
            dapp_definition_address: dapp_definition_address.clone(),
            expected_origin: origin.clone(),
            identity_address: identity_addresses[0].clone(),
            session_id: hex::encode(session_id_bytes),
            signing_key: server_signing_key.as_bytes().to_vec(),
            verifying_key: verifying_key.as_bytes().to_vec(),
        };

        self.sessions_store
            .scope(application_id)
            .put(session.session_id.clone(), session.clone())
            .await
            .map_err(Error::SessionStore)?;

        match self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce),
                user_data: Some(Bytes::from(session_id_bytes.to_vec())),
                public_key: Some(Bytes::from(server_public_key.to_bytes().to_vec())),
            })
            .await
        {
            Ok(attestation) => Ok(attestation),
            Err(e) => Err(Error::Attestation(e)),
        }
    }

    async fn get_session(
        &self,
        application_id: String,
        session_id: String,
    ) -> Result<Option<Session>, Error<A::Error, CS::Error, SS::Error>> {
        match self
            .sessions_store
            .scope(application_id)
            .get(session_id.clone())
            .await
        {
            Ok(Some(session)) => Ok(Some(session)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::SessionStore(e)),
        }
    }
}
