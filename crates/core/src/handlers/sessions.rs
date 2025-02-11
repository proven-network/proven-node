mod radix;

use crate::PrimaryContext;
pub(crate) use radix::{create_rola_challenge_handler, verify_rola_handler};

use axum::body::Body;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use axum_typed_multipart::{TryFromMultipart, TypedMultipart};
use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use headers::Origin;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_identity::{CreateAnonymousSessionOptions, IdentityManagement};
use proven_runtime::RuntimePoolManagement;
use tracing::info;

#[derive(TryFromMultipart)]
pub struct CreateSessionRequest {
    application_id: String,
    nonce: Bytes,
    public_key: Bytes,
}

pub(crate) async fn create_session_handler<AM, RM, SM, A>(
    State(PrimaryContext {
        session_manager, ..
    }): State<PrimaryContext<AM, RM, SM, A>>,
    origin_header: Option<TypedHeader<Origin>>,
    data: TypedMultipart<CreateSessionRequest>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
    A: Attestor,
{
    let origin = match origin_header {
        Some(value) => value.to_string(),
        None => {
            return Response::builder()
                .status(400)
                .body("Origin header not found".into())
                .unwrap()
        }
    };

    let verifying_key_bytes: [u8; 32] = match data.public_key.to_vec().try_into() {
        Ok(vkb) => vkb,
        Err(_) => {
            return Response::builder()
                .status(400)
                .body("Public key incorrect length".into())
                .unwrap()
        }
    };

    let Ok(verifying_key) = VerifyingKey::from_bytes(&verifying_key_bytes) else {
        return Response::builder()
            .status(400)
            .body("Failed to parse public key".into())
            .unwrap();
    };

    match session_manager
        .create_anonymous_session(CreateAnonymousSessionOptions {
            application_id: &data.application_id,
            nonce: &data.nonce,
            origin: &origin,
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
