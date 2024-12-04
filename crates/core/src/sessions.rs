use axum::body::Body;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use axum_extra::TypedHeader;
use axum_typed_multipart::TryFromMultipart;
use axum_typed_multipart::TypedMultipart;
use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use headers::Origin;
use proven_radix_rola::SignedChallenge;
use proven_sessions::{CreateSessionOptions, SessionManagement};
use tracing::info;

#[derive(TryFromMultipart)]
struct SessionRequest {
    application_name: Option<String>,
    dapp_definition_address: String,
    nonce: Bytes,
    public_key: Bytes,
    signed_challenge: String,
}

pub fn create_session_router<SM>(session_manager: SM) -> Router
where
    SM: SessionManagement,
{
    let session_manager_clone = session_manager.clone();
    let app = Router::new()
        .route(
            "/create-challenge",
            get(|origin_header: Option<TypedHeader<Origin>>| async move {
                let origin = match origin_header {
                    Some(value) => value.to_string(),
                    None => {
                        return Response::builder()
                            .status(400)
                            .body("Origin header not found".into())
                            .unwrap();
                    }
                };

                match session_manager_clone.create_challenge(origin).await {
                    Ok(challenge) => Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(challenge))
                        .unwrap_or_else(|_| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Body::from("Failed to construct response"),
                            )
                                .into_response()
                        }),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Body::from(format!("Failed to create challenge: {e}")),
                    )
                        .into_response(),
                }
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

                    let Ok(verifying_key) = VerifyingKey::from_bytes(&verifying_key_bytes) else {
                        return Response::builder()
                            .status(400)
                            .body("Failed to parse public key".into())
                            .unwrap();
                    };

                    let signed_challenges: Vec<SignedChallenge> =
                        serde_json::from_str(data.signed_challenge.as_str()).unwrap();

                    match session_manager
                        .create_session(CreateSessionOptions {
                            application_id: "TODO_APPLICATION_ID".to_string(),
                            application_name: data.application_name.clone(),
                            dapp_definition_address: data.dapp_definition_address.clone(),
                            nonce: data.nonce.clone(),
                            origin,
                            signed_challenges,
                            verifying_key,
                        })
                        .await
                    {
                        Ok(attestation_document) => {
                            let body = http_body_util::Full::new(attestation_document);
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
