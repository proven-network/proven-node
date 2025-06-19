use crate::FullContext;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use headers::Origin;
use once_cell::sync::Lazy;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;
use webauthn_rs::prelude::*;

// Base64URL representation of 32 bytes of '1'
// TODO: Tie this to a network parameter or something
const PRF_EVAL_FIRST_B64URL: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";

// In-memory state storage
// TODO: Switch to using proper KV store
static REGISTRATION_STATES: Lazy<Arc<Mutex<HashMap<String, (PasskeyRegistration, Uuid)>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));
static AUTHENTICATION_STATES: Lazy<Arc<Mutex<HashMap<String, DiscoverableAuthentication>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

#[derive(Deserialize)]
pub struct RegistrationStartData {
    pub user_name: String,
}

#[derive(Deserialize)]
pub struct StateQuery {
    pub state: String,
}

pub(crate) async fn webauthn_registration_start_handler<AM, RM, IM, A, G>(
    State(FullContext { network, .. }): State<FullContext<AM, RM, IM, A, G>>,
    origin_header: Option<TypedHeader<Origin>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    Json(RegistrationStartData { user_name }): Json<RegistrationStartData>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    let user_unique_id = Uuid::new_v4();

    let rp_origin = Url::parse(
        &network
            .governance()
            .get_primary_auth_gateway()
            .await
            .unwrap(),
    )
    .unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin = &network.origin().await.unwrap();
    let origin_url = Url::parse(origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if &origin_header.0.to_string() != origin {
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

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
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
    let mut ccr_val = match serde_json::to_value(&ccr) {
        Ok(val) => val,
        Err(e) => {
            eprintln!("Failed to serialize CCR to JSON value: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to prepare registration options".to_string())
                .unwrap();
        }
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
                .map(|pk_obj| {
                    let auth_selection = serde_json::json!({
                        "residentKey": "required",
                        "requireResidentKey": true,
                        "userVerification": "required",
                        "authenticatorAttachment": "platform"
                    });
                    pk_obj.insert("authenticatorSelection".to_string(), auth_selection);
                    true
                })
                .unwrap_or(false)
        });

    if !resident_key_set {
        eprintln!("Failed to set resident key requirement");
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
                .map(|pk_obj| {
                    // Create extensions object with PRF
                    let extensions = serde_json::json!({
                        "prf": {
                            "eval": { "first": PRF_EVAL_FIRST_B64URL }
                        }
                    });
                    pk_obj.insert("extensions".to_string(), extensions);
                    true
                })
                .unwrap_or(false)
        });

    if !prf_inserted {
        eprintln!("Failed to insert PRF extension into publicKey.extensions");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to structure registration options".to_string())
            .unwrap();
    }

    // Serialize the modified CCR directly (no additional wrapping needed)
    let final_response_json = match serde_json::to_string(&ccr_val) {
        Ok(json_str) => json_str,
        Err(e) => {
            eprintln!("Failed to serialize modified CCR: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to serialize registration options".to_string())
                .unwrap();
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(final_response_json)
        .unwrap()
}

pub(crate) async fn webauthn_registration_finish_handler<AM, RM, IM, A, G>(
    State(FullContext {
        identity_manager,
        network,
        ..
    }): State<FullContext<AM, RM, IM, A, G>>,
    origin_header: Option<TypedHeader<Origin>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    Json(register_public_key_credential): Json<RegisterPublicKeyCredential>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    let rp_origin = Url::parse(
        &network
            .governance()
            .get_primary_auth_gateway()
            .await
            .unwrap(),
    )
    .unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin = &network.origin().await.unwrap();
    let origin_url = Url::parse(origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if &origin_header.0.to_string() != origin {
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

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin_url)
        .build()
        .unwrap();

    // Get the registration state and user UUID from our in-memory storage
    let (registration_state, user_uuid) = match REGISTRATION_STATES.lock().await.remove(&state_id) {
        Some(state) => state,
        None => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Registration session not found or expired".to_string())
                .unwrap();
        }
    };

    // Complete the registration
    match webauthn.finish_passkey_registration(&register_public_key_credential, &registration_state)
    {
        Ok(passkey) => {
            // Convert webauthn_rs::Passkey to identity::Passkey
            let identity_passkey = proven_identity::Passkey::from(passkey);

            // Store the passkey using the user UUID as the passkey ID
            match identity_manager
                .save_passkey(&user_uuid, identity_passkey)
                .await
            {
                Ok(()) => {
                    println!("Stored passkey for user {}", user_uuid);
                    Response::builder()
                        .status(StatusCode::OK)
                        .body("Registration successful".to_string())
                        .unwrap()
                }
                Err(e) => {
                    eprintln!("Failed to store passkey: {}", e);
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

pub(crate) async fn webauthn_authentication_start_handler<AM, RM, IM, A, G>(
    State(FullContext { network, .. }): State<FullContext<AM, RM, IM, A, G>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    origin_header: Option<TypedHeader<Origin>>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    let rp_origin = Url::parse(
        &network
            .governance()
            .get_primary_auth_gateway()
            .await
            .unwrap(),
    )
    .unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin = &network.origin().await.unwrap();
    let origin_url = Url::parse(origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if &origin_header.0.to_string() != origin {
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

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
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
            eprintln!("Failed to start discoverable authentication: {}", e);
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
            eprintln!("Failed to serialize RCR to JSON value: {}", e);
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
                .map(|pk_obj| {
                    // Create extensions object with PRF
                    let extensions = serde_json::json!({
                        "prf": {
                            "eval": { "first": PRF_EVAL_FIRST_B64URL }
                        }
                    });
                    pk_obj.insert("extensions".to_string(), extensions);
                    true
                })
                .unwrap_or(false)
        });

    if !prf_inserted {
        eprintln!("Failed to insert PRF extension into publicKey.extensions");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to structure authentication options".to_string())
            .unwrap();
    }

    // Set hints to client-device only
    let hints_set = rcr_val
        .pointer_mut("/publicKey")
        .and_then(|pk_val| pk_val.as_object_mut())
        .map(|pk_obj| {
            pk_obj.insert(
                "hints".to_string(),
                serde_json::Value::Array(vec![serde_json::Value::String(
                    "client-device".to_string(),
                )]),
            );
            true
        })
        .unwrap_or(false);

    if !hints_set {
        eprintln!("Failed to insert userVerification");
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
            eprintln!("Failed to serialize modified RCR: {}", e);
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

pub(crate) async fn webauthn_authentication_finish_handler<AM, RM, IM, A, G>(
    State(FullContext {
        identity_manager,
        network,
        ..
    }): State<FullContext<AM, RM, IM, A, G>>,
    origin_header: Option<TypedHeader<Origin>>,
    Query(StateQuery { state: state_id }): Query<StateQuery>,
    Json(auth_public_key_credential): Json<PublicKeyCredential>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    let rp_origin = Url::parse(
        &network
            .governance()
            .get_primary_auth_gateway()
            .await
            .unwrap(),
    )
    .unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin = &network.origin().await.unwrap();
    let origin_url = Url::parse(origin).unwrap();

    // Check origin_header and origin match
    if let Some(origin_header) = origin_header {
        if &origin_header.0.to_string() != origin {
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

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin_url)
        .build()
        .unwrap();

    // Get the authentication state from our in-memory storage
    let auth_state = match AUTHENTICATION_STATES.lock().await.remove(&state_id) {
        Some(state) => state,
        None => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Authentication session not found or expired".to_string())
                .unwrap();
        }
    };

    // First, identify which user is trying to authenticate
    let (user_unique_id, _user_handle) =
        match webauthn.identify_discoverable_authentication(&auth_public_key_credential) {
            Ok(user_data) => user_data,
            Err(e) => {
                eprintln!(
                    "Failed to identify user from discoverable authentication: {}",
                    e
                );
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(format!("Failed to identify user: {e}"))
                    .unwrap();
            }
        };

    // Load the stored passkey using the user UUID
    let identity_passkey = match identity_manager.get_passkey(&user_unique_id).await {
        Ok(Some(passkey)) => passkey,
        Ok(None) => {
            eprintln!("No stored passkey found for user {}", user_unique_id);
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(format!(
                    "No stored passkey found for user {}",
                    user_unique_id
                ))
                .unwrap();
        }
        Err(e) => {
            eprintln!("Failed to load passkey: {}", e);
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
        Ok(auth_result) => {
            // Authentication successful
            println!("Discoverable authentication successful: {:?}", auth_result);

            Response::builder()
                .status(StatusCode::OK)
                .body("Authentication successful".to_string())
                .unwrap()
        }
        Err(e) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(format!("Authentication failed: {e}"))
            .unwrap(),
    }
}
