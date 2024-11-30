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
use proven_store::{Store, Store1};
use radix_common::network::NetworkDefinition;
use rand::{thread_rng, Rng};

pub struct CreateSessionParams {
    pub application_id: String,
    pub application_name: Option<String>,
    pub dapp_definition_address: String,
    pub nonce: Bytes,
    pub origin: String,
    pub signed_challenges: Vec<SignedChallenge>,
    pub verifying_key: VerifyingKey,
}

#[async_trait]
pub trait SessionManagement: Clone + Send + Sync + 'static {
    type Attestor: Attestor;
    type ChallengeStore: Store1;
    type SessionStore: Store1;

    fn new(
        attestor: Self::Attestor,
        challenge_store: Self::ChallengeStore,
        sessions_store: Self::SessionStore,
        radix_gateway_origin: String,
        radix_network_definition: NetworkDefinition,
    ) -> Self;

    async fn create_challenge(
        &self,
        application_id: String,
    ) -> Result<
        String,
        <Self::ChallengeStore as Store1>::Error,
        <Self::SessionStore as Store1>::Error,
    >;

    async fn create_session(
        &self,
        params: CreateSessionParams,
    ) -> Result<Bytes, <Self::ChallengeStore as Store1>::Error, <Self::SessionStore as Store1>::Error>;

    async fn get_session(
        &self,
        application_id: String,
        session_id: String,
    ) -> Result<
        Option<Session>,
        <Self::ChallengeStore as Store1>::Error,
        <Self::SessionStore as Store1>::Error,
    >;
}

#[derive(Clone)]
pub struct SessionManager<A: Attestor, CS: Store1, SS: Store1> {
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
    SS: Store1,
{
    type Attestor = A;
    type ChallengeStore = CS;
    type SessionStore = SS;

    fn new(
        attestor: Self::Attestor,
        challenge_store: Self::ChallengeStore,
        sessions_store: Self::SessionStore,
        radix_gateway_origin: String,
        radix_network_definition: NetworkDefinition,
    ) -> Self {
        SessionManager {
            attestor,
            challenge_store,
            sessions_store,
            radix_gateway_origin,
            radix_network_definition,
        }
    }

    async fn create_challenge(
        &self,
        application_id: String,
    ) -> Result<String, CS::Error, SS::Error> {
        let mut challenge = String::new();

        for _ in 0..32 {
            challenge.push_str(&format!("{:02x}", thread_rng().gen::<u8>()));
        }

        self.challenge_store
            .scope(application_id)
            .put(challenge.clone(), Bytes::from_static(&[1u8]))
            .await
            .map_err(Error::ChallengeStore)?;

        Ok(challenge)
    }

    async fn create_session(
        &self,
        CreateSessionParams {
            application_id,
            application_name,
            dapp_definition_address,
            nonce,
            origin,
            signed_challenges,
            verifying_key,
        }: CreateSessionParams,
    ) -> Result<Bytes, CS::Error, SS::Error> {
        let rola = Rola::new(
            self.radix_network_definition.clone(),
            self.radix_gateway_origin.clone(),
            dapp_definition_address.clone(),
            origin.clone(),
            application_name.clone().unwrap_or_default(),
        );

        let mut challenges = HashSet::new();
        for signed_challenge in signed_challenges.clone() {
            challenges.insert(signed_challenge.challenge.clone());
        }

        for challenge in challenges {
            let scoped_challenge_store = self.challenge_store.scope(application_id.clone());

            match scoped_challenge_store.get(challenge.clone()).await {
                Ok(_) => {
                    scoped_challenge_store
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
            .scope(application_id)
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
        application_id: String,
        session_id: String,
    ) -> Result<Option<Session>, CS::Error, SS::Error> {
        match self
            .sessions_store
            .scope(application_id)
            .get(session_id.clone())
            .await
        {
            Ok(Some(bytes)) => {
                let marked: MarkedSession<CS, SS> = bytes.try_into()?;
                Ok(Some(marked.into()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Error::SessionStore(e)),
        }
    }
}
