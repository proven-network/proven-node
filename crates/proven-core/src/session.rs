use std::collections::HashSet;
use std::vec;

use async_trait::async_trait;
use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_attestation::{AttestationParams, Attestor};
use proven_store::Store;
use radix_common::network::NetworkDefinition;
use rand::{thread_rng, Rng};
use rola::{Rola, SignedChallenge, Type as SignedChallengeType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub signing_key: Vec<u8>,
    pub verifying_key: Vec<u8>,
    pub dapp_definition_address: String,
    pub expected_origin: String,
    pub identity_address: String,
    pub account_addresses: Vec<String>,
}

#[async_trait]
trait SessionManagement {
    type A: Attestor + 'static;
    type CS: Store + 'static;
    type SS: Store + 'static;

    fn new(
        attestor: Self::A,
        challenge_store: Self::CS,
        sessions_store: Self::SS,
        network_definition: NetworkDefinition,
    ) -> Self;

    fn create_challenge(&self) -> Result<String>;

    fn create_session_with_attestation(
        &self,
        verifying_key: VerifyingKey,
        nonce: Vec<u8>,
        signed_challenges: Vec<SignedChallenge>,
        origin: String,
        dapp_definition_address: String,
        application_name: Option<String>,
    ) -> Result<Vec<u8>>;

    fn get_session(&self, session_id: String) -> Result<Option<Session>>;
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    AttestationError,
    ChallengeStoreError,
    SessionStoreError,
    SignedChallengeInvalid,
}

pub struct SessionManager {
    attestor: Box<dyn Attestor<AE = Box<dyn std::error::Error>>>,
    challenge_store: Box<dyn Store<SE = Box<dyn std::error::Error>>>,
    sessions_store: Box<dyn Store<SE = Box<dyn std::error::Error>>>,
    network_definition: NetworkDefinition,
}

#[async_trait]
impl SessionManagement for SessionManager {
    type A = Box<dyn Attestor<AE = Error>>;
    type CS = Box<dyn Store<SE = Error>>;
    type SS = Box<dyn Store<SE = Error>>;

    fn new(
        attestor: Self::A,
        challenge_store: Self::CS,
        sessions_store: Self::SS,
        network_definition: NetworkDefinition,
    ) -> Self {
        SessionManager {
            attestor,
            challenge_store,
            sessions_store,
            network_definition,
        }
    }

    async fn create_challenge(&self) -> Result<String> {
        let mut challenge = String::new();

        for _ in 0..32 {
            challenge.push_str(&format!("{:02x}", thread_rng().gen::<u8>()));
        }

        self.challenge_store
            .put(challenge.clone(), vec![1])
            .await
            .map_err(|_| Error::ChallengeStoreError)?;

        Ok(challenge)
    }

    async fn create_session_with_attestation(
        &self,
        verifying_key: VerifyingKey,
        nonce: Vec<u8>,
        signed_challenges: Vec<SignedChallenge>,
        origin: String,
        dapp_definition_address: String,
        application_name: Option<String>,
    ) -> Result<Vec<u8>> {
        let rola = Rola::new(
            self.network_definition.clone(),
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
                    self.challenge_store.del(challenge.clone()).await.unwrap();
                }
                Err(_) => {
                    return Err(Error::SignedChallengeInvalid);
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
            session_id: hex::encode(session_id_bytes),
            signing_key: server_signing_key.as_bytes().to_vec(),
            verifying_key: verifying_key.as_bytes().to_vec(),
            dapp_definition_address: dapp_definition_address.clone(),
            expected_origin: origin,
            identity_address: identity_addresses[0].clone(),
            account_addresses,
        };

        let session_cbor = serde_cbor::to_vec(&session).unwrap();
        self.sessions_store
            .put(session.session_id.clone(), session_cbor)
            .await
            .map_err(|_| Error::SessionStoreError)?;

        match self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce),
                user_data: Some(session_id_bytes.to_vec()),
                public_key: Some(server_public_key.to_bytes().to_vec()),
            })
            .await
        {
            Ok(attestation) => Ok(attestation),
            Err(_) => Err(Error::AttestationError),
        }
    }

    async fn get_session(&self, session_id: String) -> Result<Option<Session>> {
        match self.sessions_store.get(session_id.clone()).await {
            Ok(session_opt) => match session_opt {
                Some(session_cbor) => {
                    let session: Session = serde_cbor::from_slice(&session_cbor).unwrap();
                    Ok(Some(session))
                }
                None => Ok(None),
            },
            Err(_) => Err(Error::SessionStoreError),
        }
    }
}
