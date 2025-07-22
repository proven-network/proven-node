#![allow(clippy::type_complexity)]

use crate::FullContext;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use headers::Origin;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_identity::IdentityManagement;
use proven_logger::{error, info};
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use proven_topology::TopologyAdaptor;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;
use webauthn_rs::prelude::*;

// Base64URL representation of 32 bytes of '1'
// TODO: Tie this to a network parameter or something
const PRF_EVAL_FIRST_B64URL: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";

// In-memory state storage
// TODO: Switch to using proper KV store
static REGISTRATION_STATES: LazyLock<Arc<Mutex<HashMap<String, (PasskeyRegistration, Uuid)>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
static AUTHENTICATION_STATES: LazyLock<Arc<Mutex<HashMap<String, DiscoverableAuthentication>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

#[derive(Deserialize)]
pub struct RegistrationStartData {
    pub user_name: String,
}

#[derive(Deserialize)]
pub struct StateQuery {
    pub state: String,
}

#[allow(clippy::too_many_lines)] // TODO: Refactor this to be more readable
pub(crate) async fn webauthn_registration_start_handler<A, G, AM, RM, IM, PM, SM>(
    State(FullContext {
        governance, origin, ..
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    origin_header: Option<TypedHeader<Origin>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    Json(RegistrationStartData { user_name }): Json<RegistrationStartData>,
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
    let user_unique_id = Uuid::new_v4();

    let rp_origin = Url::parse(&governance.get_primary_auth_gateway().await.unwrap()).unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin_url = Url::parse(&origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if origin_header.0.to_string() != origin {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Origin mismatch".to_string())
                .unwrap();
        }
    } else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Origin header missing".to_string())
            .unwrap();
    }

    let webauthn = WebauthnBuilder::new(rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin_url)
        .build()
        .unwrap();

    // Initiate passkey registration - this automatically creates discoverable credentials when possible
    let (ccr, state) = webauthn
        .start_passkey_registration(user_unique_id, &user_name, &user_name, None)
        .expect("Failed to start registration");

    // Store both the state and the user UUID using the state_id
    REGISTRATION_STATES
        .lock()
        .await
        .insert(state_id.clone(), (state, user_unique_id));

    // Convert CCR to Value for modification
    let Ok(mut ccr_val) = serde_json::to_value(&ccr) else {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to prepare registration options".to_string())
            .unwrap();
    };

    // Manually override to require resident keys (discoverable credentials)
    let resident_key_set = ccr_val
        .pointer_mut("/publicKey/authenticatorSelection")
        .and_then(|auth_sel| auth_sel.as_object_mut())
        .map(|auth_obj| {
            auth_obj.insert(
                "residentKey".to_string(),
                serde_json::Value::String("required".to_string()),
            );
            auth_obj.insert(
                "requireResidentKey".to_string(),
                serde_json::Value::Bool(true),
            );
            auth_obj.insert(
                "userVerification".to_string(),
                serde_json::Value::String("required".to_string()),
            );
            auth_obj.insert(
                "authenticatorAttachment".to_string(),
                serde_json::Value::String("platform".to_string()),
            );
            true
        })
        .unwrap_or_else(|| {
            // Create authenticatorSelection if it doesn't exist
            ccr_val
                .pointer_mut("/publicKey")
                .and_then(|pk_val| pk_val.as_object_mut())
                .is_some_and(|pk_obj| {
                    let auth_selection = serde_json::json!({
                        "residentKey": "required",
                        "requireResidentKey": true,
                        "userVerification": "required",
                        "authenticatorAttachment": "platform"
                    });
                    pk_obj.insert("authenticatorSelection".to_string(), auth_selection);
                    true
                })
        });

    if !resident_key_set {
        error!("Failed to set resident key requirement");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to configure credential requirements".to_string())
            .unwrap();
    }

    // Navigate into ccr_val["publicKey"]["extensions"] and insert PRF
    let prf_inserted = ccr_val
        .pointer_mut("/publicKey/extensions")
        .and_then(|ext_val| ext_val.as_object_mut())
        .map(|extensions_obj| {
            // Create and insert the PRF extension
            let prf_val = serde_json::json!({
                "eval": { "first": PRF_EVAL_FIRST_B64URL }
            });
            extensions_obj.insert("prf".to_string(), prf_val);
            true // Indicate success
        })
        .unwrap_or_else(|| {
            // If /publicKey/extensions doesn't exist, try to create it
            ccr_val
                .pointer_mut("/publicKey")
                .and_then(|pk_val| pk_val.as_object_mut())
                .is_some_and(|pk_obj| {
                    // Create extensions object with PRF
                    let extensions = serde_json::json!({
                        "prf": {
                            "eval": { "first": PRF_EVAL_FIRST_B64URL }
                        }
                    });
                    pk_obj.insert("extensions".to_string(), extensions);
                    true
                })
        });

    if !prf_inserted {
        error!("Failed to insert PRF extension into publicKey.extensions");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to structure registration options".to_string())
            .unwrap();
    }

    // Serialize the modified CCR directly (no additional wrapping needed)
    let Ok(final_response_json) = serde_json::to_string(&ccr_val) else {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to serialize registration options".to_string())
            .unwrap();
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(final_response_json)
        .unwrap()
}

pub(crate) async fn webauthn_registration_finish_handler<A, G, AM, RM, IM, PM, SM>(
    State(FullContext {
        governance,
        origin,
        passkey_manager,
        ..
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    origin_header: Option<TypedHeader<Origin>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    Json(register_public_key_credential): Json<RegisterPublicKeyCredential>,
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
    let rp_origin = Url::parse(&governance.get_primary_auth_gateway().await.unwrap()).unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin_url = Url::parse(&origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if origin_header.0.to_string() != origin {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Origin mismatch".to_string())
                .unwrap();
        }
    } else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Origin header missing".to_string())
            .unwrap();
    }

    let webauthn = WebauthnBuilder::new(rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin_url)
        .build()
        .unwrap();

    // Get the registration state and user UUID from our in-memory storage
    let Some((registration_state, user_uuid)) = REGISTRATION_STATES.lock().await.remove(&state_id)
    else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Registration session not found or expired".to_string())
            .unwrap();
    };

    // Complete the registration
    match webauthn.finish_passkey_registration(&register_public_key_credential, &registration_state)
    {
        Ok(passkey) => {
            // Convert webauthn_rs::Passkey to identity::Passkey
            let identity_passkey = proven_passkeys::Passkey::from(passkey);

            // Store the passkey using the user UUID as the passkey ID
            match passkey_manager
                .save_passkey(&user_uuid, identity_passkey)
                .await
            {
                Ok(()) => {
                    info!("Stored passkey for user {user_uuid}");
                    Response::builder()
                        .status(StatusCode::OK)
                        .body("Registration successful".to_string())
                        .unwrap()
                }
                Err(e) => {
                    error!("Failed to store passkey: {e}");
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to store passkey".to_string())
                        .unwrap()
                }
            }
        }
        Err(e) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(format!("registration failed: {e}"))
            .unwrap(),
    }
}

#[allow(clippy::too_many_lines)] // TODO: Refactor this to be more readable
pub(crate) async fn webauthn_authentication_start_handler<A, G, AM, RM, IM, PM, SM>(
    State(FullContext {
        governance, origin, ..
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    origin_header: Option<TypedHeader<Origin>>,
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
    let rp_origin = Url::parse(&governance.get_primary_auth_gateway().await.unwrap()).unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin_url = Url::parse(&origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if origin_header.0.to_string() != origin {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Origin mismatch".to_string())
                .unwrap();
        }
    } else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Origin header missing".to_string())
            .unwrap();
    }

    let webauthn = WebauthnBuilder::new(rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin_url)
        .build()
        .unwrap();

    // Start discoverable authentication (for conditional UI)
    // This allows any discoverable credential for this RP to be used
    let (rcr, state) = match webauthn.start_discoverable_authentication() {
        Ok((rcr, state)) => (rcr, state),
        Err(e) => {
            error!("Failed to start discoverable authentication: {e}");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to start authentication".to_string())
                .unwrap();
        }
    };

    // Store the authentication state using the state_id
    AUTHENTICATION_STATES
        .lock()
        .await
        .insert(state_id.clone(), state);

    // Convert RCR to Value for modification (add PRF extension)
    let mut rcr_val = match serde_json::to_value(&rcr) {
        Ok(val) => val,
        Err(e) => {
            error!("Failed to serialize RCR to JSON value: {e}");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to prepare authentication options".to_string())
                .unwrap();
        }
    };

    // Add PRF extension to authentication request
    let prf_inserted = rcr_val
        .pointer_mut("/publicKey/extensions")
        .and_then(|ext_val| ext_val.as_object_mut())
        .map(|extensions_obj| {
            // Create and insert the PRF extension
            let prf_val = serde_json::json!({
                "eval": { "first": PRF_EVAL_FIRST_B64URL }
            });
            extensions_obj.insert("prf".to_string(), prf_val);
            true // Indicate success
        })
        .unwrap_or_else(|| {
            // If /publicKey/extensions doesn't exist, try to create it
            rcr_val
                .pointer_mut("/publicKey")
                .and_then(|pk_val| pk_val.as_object_mut())
                .is_some_and(|pk_obj| {
                    // Create extensions object with PRF
                    let extensions = serde_json::json!({
                        "prf": {
                            "eval": { "first": PRF_EVAL_FIRST_B64URL }
                        }
                    });
                    pk_obj.insert("extensions".to_string(), extensions);
                    true
                })
        });

    if !prf_inserted {
        error!("Failed to insert PRF extension into publicKey.extensions");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to structure authentication options".to_string())
            .unwrap();
    }

    // Set hints to client-device only
    let hints_set = rcr_val
        .pointer_mut("/publicKey")
        .and_then(|pk_val| pk_val.as_object_mut())
        .is_some_and(|pk_obj| {
            pk_obj.insert(
                "hints".to_string(),
                serde_json::Value::Array(vec![serde_json::Value::String(
                    "client-device".to_string(),
                )]),
            );
            true
        });

    if !hints_set {
        error!("Failed to insert userVerification");
    }

    // Remove mediation field (webauthn_rs forces it when activating discoverable)
    // Set to immediate in client-side code
    if let Some(obj) = rcr_val.as_object_mut() {
        obj.remove("mediation");
    }

    // Serialize the modified RCR
    let final_response_json = match serde_json::to_string(&rcr_val) {
        Ok(json_str) => json_str,
        Err(e) => {
            error!("Failed to serialize modified RCR: {e}");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to serialize authentication options".to_string())
                .unwrap();
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(final_response_json)
        .unwrap()
}

pub(crate) async fn webauthn_authentication_finish_handler<A, G, AM, RM, IM, PM, SM>(
    State(FullContext {
        governance,
        origin,
        passkey_manager,
        ..
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    origin_header: Option<TypedHeader<Origin>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    Json(auth_public_key_credential): Json<PublicKeyCredential>,
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
    let rp_origin = Url::parse(&governance.get_primary_auth_gateway().await.unwrap()).unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin_url = Url::parse(&origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if origin_header.0.to_string() != origin {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Origin mismatch".to_string())
                .unwrap();
        }
    } else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Origin header missing".to_string())
            .unwrap();
    }

    let webauthn = WebauthnBuilder::new(rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin_url)
        .build()
        .unwrap();

    // Get the authentication state from our in-memory storage
    let Some(auth_state) = AUTHENTICATION_STATES.lock().await.remove(&state_id) else {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Authentication session not found or expired".to_string())
            .unwrap();
    };

    // First, identify which user is trying to authenticate
    let (user_unique_id, _user_handle) =
        match webauthn.identify_discoverable_authentication(&auth_public_key_credential) {
            Ok(user_data) => user_data,
            Err(e) => {
                error!("Failed to identify user from discoverable authentication: {e}");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(format!("Failed to identify user: {e}"))
                    .unwrap();
            }
        };

    // Load the stored passkey using the user UUID
    let identity_passkey = match passkey_manager.get_passkey(&user_unique_id).await {
        Ok(Some(passkey)) => passkey,
        Ok(None) => {
            error!("No stored passkey found for user {user_unique_id}");
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(format!("No stored passkey found for user {user_unique_id}"))
                .unwrap();
        }
        Err(e) => {
            error!("Failed to load passkey: {e}");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to load stored passkey".to_string())
                .unwrap();
        }
    };

    // Convert back to webauthn_rs::Passkey
    let passkey: webauthn_rs::prelude::Passkey = identity_passkey.into_inner();

    // Convert to DiscoverableKey
    let discoverable_key: webauthn_rs::prelude::DiscoverableKey = (&passkey).into();

    // Complete the discoverable authentication
    match webauthn.finish_discoverable_authentication(
        &auth_public_key_credential,
        auth_state,
        &[discoverable_key],
    ) {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body("Authentication successful".to_string())
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(format!("Authentication failed: {e}"))
            .unwrap(),
    }
}
