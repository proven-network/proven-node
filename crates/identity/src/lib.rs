//! Manages all user sessions (created via ROLA) and their associated data.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod identity;
mod ledger_identity;
mod session;

pub use error::Error;
pub use identity::Identity;
pub use ledger_identity::LedgerIdentity;
pub use ledger_identity::radix::RadixIdentityDetails;
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
use tracing::info;
use uuid::Uuid;

/// Options for creating a new `IdentityManager`
pub struct IdentityManagerOptions<'a, A, CS, SS>
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

/// Options for creating a new session.
pub struct IdentifySessionViaRadixOptions<'a> {
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

    /// Gets an identity by Radix identity address.
    async fn get_identity_by_radix_identity_address(
        &self,
        radix_identity_address: &str,
    ) -> Result<Option<Identity>, Error>;

    /// Gets a session by ID.
    async fn get_session(
        &self,
        application_id: &str,
        session_id: &str,
    ) -> Result<Option<Session>, Error>;

    /// Identifies a session
    async fn identify_session(&self, session_id: &str, identity_id: &str) -> Result<Bytes, Error>;

    /// Identifies a session via ROLA.
    async fn identify_session_via_rola(
        &self,
        options: IdentifySessionViaRadixOptions<'_>,
    ) -> Result<Bytes, Error>;
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
    radix_gateway_origin: String,
    radix_network_definition: NetworkDefinition,
}

impl<A, CS, SS> IdentityManager<A, CS, SS>
where
    A: Attestor,
    CS: Store2,
    SS: Store1<Session, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    #[allow(clippy::unused_self)]
    fn create_identity_from_ledger_identity(
        &self,
        _ledger_identity: LedgerIdentity,
    ) -> Result<Identity, Error> {
        unimplemented!()
    }
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
            radix_gateway_origin,
            radix_network_definition,
        }: IdentityManagerOptions<A, CS, SS>,
    ) -> Self {
        Self {
            attestor,
            challenge_store,
            sessions_store,
            radix_gateway_origin: radix_gateway_origin.to_string(),
            radix_network_definition: radix_network_definition.clone(),
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

    async fn get_identity_by_radix_identity_address(
        &self,
        _radix_identity_address: &str,
    ) -> Result<Option<Identity>, Error> {
        unimplemented!()
    }

    async fn get_session(
        &self,
        application_id: &str,
        session_id: &str,
    ) -> Result<Option<Session>, Error> {
        info!(
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

    async fn identify_session_via_rola(
        &self,
        IdentifySessionViaRadixOptions {
            application_id,
            application_name,
            dapp_definition_address,
            nonce,
            origin,
            signed_challenges,
            verifying_key,
        }: IdentifySessionViaRadixOptions<'_>,
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

        let session_id = Uuid::new_v4();

        let radix_identity = RadixIdentityDetails {
            account_addresses,
            dapp_definition_address: dapp_definition_address.to_string(),
            expected_origin: origin.to_string(),
            identity_address: identity_addresses[0].clone(),
        };

        let identity = self
            .get_identity_by_radix_identity_address(&radix_identity.identity_address)
            .await?
            .unwrap_or_else(|| {
                self.create_identity_from_ledger_identity(LedgerIdentity::Radix(
                    radix_identity.clone(),
                ))
                .expect("Failed to create identity")
            });

        let session = Session::Identified {
            identity,
            ledger_identity: LedgerIdentity::Radix(radix_identity),
            origin: origin.to_string(),
            session_id,
            signing_key: server_signing_key.clone(),
            verifying_key: *verifying_key,
        };

        self.sessions_store
            .scope(application_id)
            .put(session.session_id().to_string(), session.clone())
            .await
            .map_err(|e| Error::SessionStore(e.to_string()))?;

        match self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce.clone()),
                user_data: Some(Bytes::from(session.session_id().as_bytes().to_vec())),
                public_key: Some(Bytes::from(server_public_key.to_bytes().to_vec())),
            })
            .await
        {
            Ok(attestation) => Ok(attestation),
            Err(e) => Err(Error::Attestation(e.to_string())),
        }
    }
}
