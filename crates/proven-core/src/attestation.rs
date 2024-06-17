use async_nats::jetstream::kv::Config as KevValueConfig;
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use axum_extra::TypedHeader;
use axum_typed_multipart::TryFromMultipart;
use axum_typed_multipart::TypedMultipart;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use headers::Origin;
use proven_attestation::{AttestationParams, Attestor};
use radix_common::network::NetworkDefinition;
use rand::{thread_rng, Rng};
use rola::{Rola, SignedChallenge, Type as SignedChallengeType};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use tracing::info;

#[derive(TryFromMultipart)]
struct AttestationRequest {
    public_key: Bytes,
    nonce: Bytes,
    signed_challenge: String,
    dapp_definition_address: String,
    application_name: Option<String>,
}

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

pub async fn create_attestation_handlers<A: Attestor + Clone + 'static>(
    attestor: A,
    nats_client: async_nats::Client,
) -> Router {
    let challenge_js_context = async_nats::jetstream::new(nats_client.clone());
    let challenge_store = challenge_js_context
        .create_key_value(KevValueConfig {
            bucket: "challenges".to_string(),
            max_age: Duration::from_secs(60 * 5),
            ..Default::default()
        })
        .await
        .unwrap();
    let challenge_store_2 = challenge_store.clone();

    let sessions_js_context = async_nats::jetstream::new(nats_client.clone());
    let sessions_store = sessions_js_context
        .create_key_value(KevValueConfig {
            bucket: "sessions".to_string(),
            max_age: Duration::from_secs(60 * 60 * 7), // 1 week validity
            ..Default::default()
        })
        .await
        .unwrap();

    let app = Router::new()
        .route(
            "/create-challenge",
            get(move || async move {
                let mut challenge = String::new();
                for _ in 0..32 {
                    challenge.push_str(&format!("{:02x}", thread_rng().gen::<u8>()));
                }

                challenge_store_2
                    .put(challenge.clone(), Bytes::from_static(&[1]))
                    .await
                    .unwrap();

                challenge
            }),
        )
        .route(
            "/verify",
            post(
                |origin_header: Option<TypedHeader<Origin>>,
                 data: TypedMultipart<AttestationRequest>| async move {
                    let origin = match origin_header {
                        Some(value) => value.to_string(),
                        None => {
                            return Response::builder()
                                .status(400)
                                .body("Origin header not found".into())
                                .unwrap();
                        }
                    };

                    let verifying_key_bytes: [u8; 32] = match data.public_key.to_vec().try_into() {
                        Ok(vkb) => vkb,
                        Err(_) => {
                            return Response::builder()
                                .status(400)
                                .body("Public key incorrect length".into())
                                .unwrap();
                        }
                    };

                    let verifying_key = match VerifyingKey::from_bytes(&verifying_key_bytes) {
                        Ok(vk) => vk,
                        Err(_) => {
                            return Response::builder()
                                .status(400)
                                .body("Failed to parse public key".into())
                                .unwrap();
                        }
                    };

                    let signed_challenges: Vec<SignedChallenge> =
                        serde_json::from_str(data.signed_challenge.as_str()).unwrap();

                    let rola = Rola::new(
                        NetworkDefinition::stokenet(),
                        data.dapp_definition_address.clone(),
                        origin.clone(),
                        data.application_name.clone().unwrap_or_default(),
                    );

                    let mut challenges = HashSet::new();
                    for signed_challenge in signed_challenges.clone() {
                        challenges.insert(signed_challenge.challenge.clone());
                    }

                    for challenge in challenges {
                        match challenge_store.get(challenge.clone()).await {
                            Ok(_) => {
                                challenge_store.delete(challenge.clone()).await.unwrap();
                            }
                            Err(_) => {
                                return Response::builder()
                                    .status(400)
                                    .body("Challenge not found".into())
                                    .unwrap();
                            }
                        }
                    }

                    let identity_addresses = signed_challenges
                        .iter()
                        .filter(|sc| sc.r#type == SignedChallengeType::Persona)
                        .map(|sc| sc.address.clone())
                        .collect::<Vec<String>>();

                    if identity_addresses.len() != 1 {
                        return Response::builder()
                            .status(400)
                            .body("Must authorize one identity".into())
                            .unwrap();
                    }

                    for signed_challenge in signed_challenges.clone() {
                        info!("signed_challenge: {:?}", signed_challenge);
                        match rola.verify_signed_challenge(signed_challenge).await {
                            Ok(_) => {}
                            Err(_) => {
                                return Response::builder()
                                    .status(400)
                                    .body("Failed to verify signed challenge".into())
                                    .unwrap();
                            }
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
                        dapp_definition_address: data.dapp_definition_address.clone(),
                        expected_origin: origin,
                        identity_address: identity_addresses[0].clone(),
                        account_addresses,
                    };

                    info!("session: {:?}", session);

                    let session_cbor = serde_cbor::to_vec(&session).unwrap();
                    sessions_store
                        .put(session.session_id.clone(), Bytes::from(session_cbor))
                        .await
                        .unwrap();

                    match attestor
                        .attest(AttestationParams {
                            nonce: Some(data.nonce.to_vec()),
                            user_data: Some(session_id_bytes.to_vec()),
                            public_key: Some(server_public_key.to_bytes().to_vec()),
                        })
                        .await
                    {
                        Ok(attestation) => {
                            let bytes = bytes::Bytes::from(attestation);
                            let body = http_body_util::Full::new(bytes);

                            Response::builder()
                                .header("Content-Type", "application/cose")
                                .header("Content-Disposition", "inline; filename=\"attestation\"")
                                .body(body)
                                .unwrap()
                        }
                        Err(_) => Response::builder()
                            .status(500)
                            .body("Failed to generate attestation".into())
                            .unwrap(),
                    }
                },
            ),
        );

    app
}
