//! Manages all user sessions (created via ROLA) and their associated data.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod identity;
mod session;

pub use error::Error;
pub use identity::Identity;
pub use identity::radix::RadixIdentityDetails;
pub use session::Session;

use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_attestation::{AttestationParams, Attestor};
use proven_radix_rola::{Rola, RolaOptions, SignedChallenge, Type as SignedChallengeType};
use proven_store::{Store, Store1, Store2};
use radix_common::network::NetworkDefinition;
use rand::{Rng, thread_rng};

/// Options for creating a new `SessionManager`
pub struct SessionManagerOptions<'a, A, CS, SS>
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

    /// The origin of the Radix gateway.
    pub radix_gateway_origin: &'a str,

    /// Radix network definition.
    pub radix_network_definition: &'a NetworkDefinition,
}

/// Options for creating a new session.
pub struct CreateSessionOptions<'a> {
    /// The application ID.
    pub application_id: &'a str,

    /// The application name.
    pub application_name: Option<&'a str>,

    /// The dApp definition address.
    pub dapp_definition_address: &'a str,

    /// Challenge used in remote attestation.
    pub nonce: &'a Bytes,

    /// The origin of the request.
    pub origin: &'a str,

    /// Signed ROLA challenges.
    pub signed_challenges: &'a [SignedChallenge],

    /// The verifying key of the client.
    pub verifying_key: &'a VerifyingKey,
}

/// Trait for managing user sessions.
#[async_trait]
pub trait SessionManagement
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
        options: SessionManagerOptions<Self::Attestor, Self::ChallengeStore, Self::SessionStore>,
    ) -> Self;

    /// Creates a new challenge to use for session creation.
    async fn create_challenge(&self, application_id: &str, origin: &str) -> Result<String, Error>;

    /// Creates a new session.
    async fn create_session(&self, params: CreateSessionOptions<'_>) -> Result<Bytes, Error>;

    /// Gets a session by its ID.
    async fn get_session(
        &self,
        application_id: &str,
        session_id: &str,
    ) -> Result<Option<Session>, Error>;
}

/// Manages all user sessions (created via ROLA) and their associated data.
#[derive(Clone)]
pub struct SessionManager<A, CS, SS>
where
    A: Attestor,
    CS: Store2,
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
    CS: Store2,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    type Attestor = A;
    type ChallengeStore = CS;
    type SessionStore = SS;

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
            radix_gateway_origin: radix_gateway_origin.to_string(),
            radix_network_definition: radix_network_definition.clone(),
        }
    }

    async fn create_challenge(&self, application_id: &str, origin: &str) -> Result<String, Error> {
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
        }: CreateSessionOptions<'_>,
    ) -> Result<Bytes, Error> {
        let rola = Rola::new(RolaOptions {
            application_name: application_name.unwrap_or_default(),
            dapp_definition_address,
            expected_origin: origin,
            gateway_url: &self.radix_gateway_origin,
            network_definition: &self.radix_network_definition,
        });

        let mut challenges = HashSet::new();
        for signed_challenge in signed_challenges {
            challenges.insert(&signed_challenge.challenge);
        }

        for challenge in challenges {
            let scoped_challenge_store = self.challenge_store.scope(application_id).scope(origin);

            match scoped_challenge_store.get(challenge.clone()).await {
                Ok(_) => {
                    scoped_challenge_store
                        .delete(challenge.clone())
                        .await
                        .map_err(|e| Error::ChallengeStore(e.to_string()))?;
                }
                Err(e) => {
                    return Err(Error::ChallengeStore(e.to_string()));
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

        for signed_challenge in signed_challenges {
            if (rola.verify_signed_challenge(signed_challenge.clone()).await).is_err() {
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

        let radix_identity = RadixIdentityDetails {
            account_addresses,
            dapp_definition_address: dapp_definition_address.to_string(),
            expected_origin: origin.to_string(),
            identity_address: identity_addresses[0].clone(),
        };

        let session = Session {
            identities: vec![Identity::Radix(radix_identity)],
            session_id: hex::encode(session_id_bytes),
            signing_key: server_signing_key.as_bytes().to_vec(),
            verifying_key: verifying_key.as_bytes().to_vec(),
        };

        self.sessions_store
            .scope(application_id)
            .put(session.session_id.clone(), session.clone())
            .await
            .map_err(|e| Error::SessionStore(e.to_string()))?;

        match self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce),
                user_data: Some(&Bytes::from(session_id_bytes.to_vec())),
                public_key: Some(&Bytes::from(server_public_key.to_bytes().to_vec())),
            })
            .await
        {
            Ok(attestation) => Ok(attestation),
            Err(e) => Err(Error::Attestation(e.to_string())),
        }
    }

    async fn get_session(
        &self,
        application_id: &str,
        session_id: &str,
    ) -> Result<Option<Session>, Error> {
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
}
