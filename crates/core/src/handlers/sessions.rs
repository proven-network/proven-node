use crate::FullContext;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use axum_typed_multipart::{TryFromMultipart, TypedMultipart};
use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use headers::Origin;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::{
    CreateAnonymousSessionOptions, CreateManagementSessionOptions, SessionManagement,
};
use proven_topology::TopologyAdaptor;
use tracing::info;
use uuid::Uuid;

#[derive(TryFromMultipart)]
pub struct CreateSessionRequest {
    nonce: Bytes,
    public_key: Bytes,
}

pub(crate) async fn create_session_handler<A, G, AM, RM, IM, PM, SM>(
    Path(application_id): Path<Uuid>,
    State(FullContext {
        sessions_manager, ..
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    origin_header: Option<TypedHeader<Origin>>,
    data: TypedMultipart<CreateSessionRequest>,
) -> impl IntoResponse
where
    A: Attestor,
    G: TopologyAdaptor,
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
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

    match sessions_manager
        .create_anonymous_session(CreateAnonymousSessionOptions {
            application_id: &application_id,
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

pub(crate) async fn create_management_session_handler<A, G, AM, RM, IM, PM, SM>(
    State(FullContext {
        sessions_manager, ..
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    origin_header: Option<TypedHeader<Origin>>,
    data: TypedMultipart<CreateSessionRequest>,
) -> impl IntoResponse
where
    A: Attestor,
    G: TopologyAdaptor,
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
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

    match sessions_manager
        .create_management_session(CreateManagementSessionOptions {
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
            info!("Error creating management session: {:?}", e);
            Response::builder()
                .status(400)
                .body("Error creating management session".into())
                .unwrap()
        }
    }
}
