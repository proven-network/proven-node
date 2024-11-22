mod error;
mod session;

use error::{Error, Result};
pub use session::*;

use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_attestation::{AttestationParams, Attestor};
use proven_radix_rola::{Rola, SignedChallenge, Type as SignedChallengeType};
use proven_store::Store;
use radix_common::network::NetworkDefinition;
use rand::{thread_rng, Rng};

#[async_trait]
pub trait SessionManagement: Clone + Send + Sync {
    type Attestor: Attestor;
    type ChallengeStore: Store;
    type SessionStore: Store;

    fn new(
        attestor: Self::Attestor,
        challenge_store: Self::ChallengeStore,
        gateway_origin: String,
        sessions_store: Self::SessionStore,
        network_definition: NetworkDefinition,
    ) -> Self;

    async fn create_challenge(
        &self,
    ) -> Result<String, <Self::ChallengeStore as Store>::Error, <Self::SessionStore as Store>::Error>;

    async fn create_session_with_attestation(
        &self,
        verifying_key: VerifyingKey,
        nonce: Bytes,
        signed_challenges: Vec<SignedChallenge>,
        origin: String,
        dapp_definition_address: String,
        application_name: Option<String>,
    ) -> Result<Bytes, <Self::ChallengeStore as Store>::Error, <Self::SessionStore as Store>::Error>;

    async fn get_session(
        &self,
        session_id: String,
    ) -> Result<
        Option<Session>,
        <Self::ChallengeStore as Store>::Error,
        <Self::SessionStore as Store>::Error,
    >;
}

#[derive(Clone)]
pub struct SessionManager<A: Attestor, CS: Store, SS: Store> {
    attestor: A,
    challenge_store: CS,
    gateway_origin: String,
    sessions_store: SS,
    network_definition: NetworkDefinition,
}

#[async_trait]
impl<A, CS, SS> SessionManagement for SessionManager<A, CS, SS>
where
    A: Attestor,
    CS: Store,
    SS: Store,
{
    type Attestor = A;
    type ChallengeStore = CS;
    type SessionStore = SS;

    fn new(
        attestor: Self::Attestor,
        challenge_store: Self::ChallengeStore,
        gateway_origin: String,
        sessions_store: Self::SessionStore,
        network_definition: NetworkDefinition,
    ) -> Self {
        SessionManager {
            attestor,
            challenge_store,
            gateway_origin,
            sessions_store,
            network_definition,
        }
    }

    async fn create_challenge(&self) -> Result<String, CS::Error, SS::Error> {
        let mut challenge = String::new();

        for _ in 0..32 {
            challenge.push_str(&format!("{:02x}", thread_rng().gen::<u8>()));
        }

        self.challenge_store
            .put(challenge.clone(), Bytes::from_static(&[1u8]))
            .await
            .map_err(Error::ChallengeStore)?;

        Ok(challenge)
    }

    async fn create_session_with_attestation(
        &self,
        verifying_key: VerifyingKey,
        nonce: Bytes,
        signed_challenges: Vec<SignedChallenge>,
        origin: String,
        dapp_definition_address: String,
        application_name: Option<String>,
    ) -> Result<Bytes, CS::Error, SS::Error> {
        let rola = Rola::new(
            self.network_definition.clone(),
            self.gateway_origin.clone(),
            dapp_definition_address.clone(),
            origin.clone(),
            application_name.clone().unwrap_or_default(),
        );

        let mut challenges = HashSet::new();
        for signed_challenge in signed_challenges.clone() {
            challenges.insert(signed_challenge.challenge.clone());
        }

        for challenge in challenges {
            match self.challenge_store.get(challenge.clone()).await {
                Ok(_) => {
                    self.challenge_store
                        .del(challenge.clone())
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

        let session: MarkedSession<CS, SS> = MarkedSession::new(
            hex::encode(session_id_bytes),
            server_signing_key.as_bytes().to_vec(),
            verifying_key.as_bytes().to_vec(),
            dapp_definition_address.clone(),
            origin,
            identity_addresses[0].clone(),
            account_addresses,
        );

        self.sessions_store
            .put(session.session_id.clone(), session.clone().try_into()?)
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
            Err(_) => Err(Error::Attestation),
        }
    }

    async fn get_session(
        &self,
        session_id: String,
    ) -> Result<Option<Session>, CS::Error, SS::Error> {
        match self.sessions_store.get(session_id.clone()).await {
            Ok(Some(bytes)) => {
                let marked: MarkedSession<CS, SS> = bytes.try_into()?;
                Ok(Some(marked.into()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::SessionStore(e)),
        }
    }
}
