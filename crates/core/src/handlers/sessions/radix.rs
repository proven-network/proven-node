use super::super::parse_bearer_token;
use crate::FullContext;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use axum_typed_multipart::{TryFromMultipart, TypedMultipart};
use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use headers::Origin;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::{IdentifySessionViaRadixOptions, IdentityManagement};
use proven_radix_rola::SignedChallenge;
use proven_runtime::RuntimePoolManagement;
use tracing::info;

#[derive(TryFromMultipart)]
pub struct SessionRequest {
    application_name: Option<String>,
    dapp_definition_address: String,
    nonce: Bytes,
    public_key: Bytes,
    signed_challenge: String,
}

pub(crate) async fn create_rola_challenge_handler<AM, RM, IM, A, G>(
    Path(application_id): Path<String>,
    State(FullContext {
        identity_manager, ..
    }): State<FullContext<AM, RM, IM, A, G>>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    // Get origin or return error
    let origin = match headers.get("Origin") {
        Some(header) => header.to_str().unwrap_or("unknown"),
        None => {
            return Response::builder()
                .status(400)
                .body(Body::from("Origin header not found"))
                .unwrap();
        }
    };

    let maybe_session_id = match headers.get("Authorization") {
        Some(header) => match header.to_str() {
            Ok(header_str) => match parse_bearer_token(header_str) {
                Ok(token) => Some(token),
                Err(e) => return Response::builder().status(401).body(Body::from(e)).unwrap(),
            },
            Err(_) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid authorization header"))
                    .unwrap();
            }
        },
        None => None,
    };

    let _maybe_session = if let Some(session_id) = maybe_session_id {
        match identity_manager
            .get_session(&application_id, &session_id)
            .await
        {
            Ok(Some(session)) => Some(session),
            Ok(None) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid session"))
                    .unwrap();
            }
            Err(_) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid token"))
                    .unwrap();
            }
        }
    } else {
        return Response::builder()
            .status(401)
            .body(Body::from("Authorization header required"))
            .unwrap();
    };

    match identity_manager
        .create_rola_challenge("application_id", &origin)
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

pub(crate) async fn verify_rola_handler<AM, RM, IM, A, G>(
    Path(application_id): Path<String>,
    State(FullContext {
        identity_manager, ..
    }): State<FullContext<AM, RM, IM, A, G>>,
    origin_header: Option<TypedHeader<Origin>>,
    data: TypedMultipart<SessionRequest>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    A: Attestor,
    G: Governance,
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

    match identity_manager
        .identify_session_via_rola(IdentifySessionViaRadixOptions {
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
