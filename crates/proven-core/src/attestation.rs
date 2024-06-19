use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use axum_extra::TypedHeader;
use axum_typed_multipart::TryFromMultipart;
use axum_typed_multipart::TypedMultipart;
use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use headers::Origin;
use proven_sessions::SessionManagement;
use rola::SignedChallenge;
use tracing::info;

#[derive(TryFromMultipart)]
struct SessionRequest {
    public_key: Bytes,
    nonce: Bytes,
    signed_challenge: String,
    dapp_definition_address: String,
    application_name: Option<String>,
}

pub async fn create_attestation_handlers<T: SessionManagement + 'static>(
    session_manager: T,
) -> Router {
    let session_manager_clone = session_manager.clone();
    let app = Router::new()
        .route(
            "/create-challenge",
            get(|| async move {
                let challenge = session_manager_clone.create_challenge().await.unwrap();

                Response::builder().body(challenge).unwrap()
            }),
        )
        .route(
            "/verify",
            post(
                |origin_header: Option<TypedHeader<Origin>>,
                 data: TypedMultipart<SessionRequest>| async move {
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

                    match session_manager
                        .create_session_with_attestation(
                            verifying_key,
                            data.nonce.to_vec(),
                            signed_challenges,
                            origin,
                            data.dapp_definition_address.clone(),
                            data.application_name.clone(),
                        )
                        .await
                    {
                        Ok(attestation_document) => {
                            let bytes = bytes::Bytes::from(attestation_document);
                            let body = http_body_util::Full::new(bytes);
                            Response::builder().body(body).unwrap()
                        }
                        Err(e) => {
                            info!("Error creating session: {:?}", e);
                            Response::builder()
                                .status(400)
                                .body("Error creating session".into())
                                .unwrap()
                        }
                    }
                },
            ),
        );

    app
}
