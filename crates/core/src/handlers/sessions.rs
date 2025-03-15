use crate::PrimaryContext;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use axum_typed_multipart::{TryFromMultipart, TypedMultipart};
use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use headers::Origin;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_radix_rola::SignedChallenge;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::{CreateSessionOptions, SessionManagement};
use tracing::info;

#[derive(TryFromMultipart)]
pub struct SessionRequest {
    application_name: Option<String>,
    dapp_definition_address: String,
    nonce: Bytes,
    public_key: Bytes,
    signed_challenge: String,
}

pub(crate) async fn create_challenge_handler<AM, RM, SM, A>(
    Path(application_id): Path<String>,
    State(PrimaryContext {
        session_manager, ..
    }): State<PrimaryContext<AM, RM, SM, A>>,
    origin_header: Option<TypedHeader<Origin>>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
    A: Attestor,
{
    let origin = match origin_header {
        Some(value) => value.to_string(),
        None => {
            return Response::builder()
                .status(400)
                .body("Origin header not found".into())
                .unwrap();
        }
    };

    match session_manager
        .create_challenge(&application_id, &origin)
        .await
    {
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
}

pub(crate) async fn verify_session_handler<AM, RM, SM, A>(
    Path(application_id): Path<String>,
    State(PrimaryContext {
        session_manager, ..
    }): State<PrimaryContext<AM, RM, SM, A>>,
    origin_header: Option<TypedHeader<Origin>>,
    data: TypedMultipart<SessionRequest>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
    A: Attestor,
{
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
            application_id: &application_id,
            application_name: data.application_name.as_deref(),
            dapp_definition_address: &data.dapp_definition_address,
            nonce: &data.nonce,
            origin: &origin,
            signed_challenges: &signed_challenges,
            verifying_key: &verifying_key,
        })
        .await
    {
        Ok(attestation_document) => Response::builder()
            .body(Body::from(attestation_document))
            .unwrap(),
        Err(e) => {
            info!("Error creating session: {:?}", e);
            Response::builder()
                .status(400)
                .body("Error creating session".into())
                .unwrap()
        }
    }
}
