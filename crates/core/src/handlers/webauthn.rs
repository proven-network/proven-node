use crate::FullContext;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use headers::Origin;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
use url::Url;
use uuid::Uuid;
use webauthn_rs::prelude::*;

use axum::extract::Path;

// Base64URL representation of 32 bytes of '1'
// TODO: Tie this to a network parameter or something
const PRF_EVAL_FIRST_B64URL: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";

pub(crate) async fn webauthn_registration_start_handler<AM, RM, SM, A, G>(
    State(FullContext { network, .. }): State<FullContext<AM, RM, SM, A, G>>,
    _origin_header: Option<TypedHeader<Origin>>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
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

    let origin = Url::parse(&network.origin().await.unwrap()).unwrap();

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin)
        .build()
        .unwrap();

    // Initiate passkey registration
    let (ccr, state) = webauthn
        .start_passkey_registration(user_unique_id, "user@proven.network", "Proven User", None)
        .expect("Failed to start registration");

    // Just serialize the state to /tmp with serde_json for testing
    let state_json = serde_json::to_string(&state).unwrap();
    std::fs::write("/tmp/registration_state.json", state_json.as_bytes()).unwrap();

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

pub(crate) async fn webauthn_registration_finish_handler<AM, RM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { network, .. }): State<FullContext<AM, RM, SM, A, G>>,
    Json(register_public_key_credential): Json<RegisterPublicKeyCredential>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
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

    let origin = Url::parse(&network.origin().await.unwrap()).unwrap();

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin)
        .build()
        .unwrap();

    // Deserialize the state from /tmp with serde_json for testing
    let registration_state = std::fs::read("/tmp/registration_state.json").unwrap();
    let registration_state: PasskeyRegistration =
        serde_json::from_slice(&registration_state).unwrap();

    // Complete the registration
    match webauthn.finish_passkey_registration(&register_public_key_credential, &registration_state)
    {
        Ok(passkey) => {
            // Store the passkey for future authentication
            let passkey_json = serde_json::to_string(&passkey).unwrap();
            std::fs::write("/tmp/stored_passkey.json", passkey_json.as_bytes()).unwrap();

            Response::builder()
                .status(StatusCode::OK)
                .body("Registration successful".to_string())
                .unwrap()
        }
        Err(e) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(format!("registration failed: {e}"))
            .unwrap(),
    }
}

pub(crate) async fn webauthn_authentication_start_handler<AM, RM, SM, A, G>(
    State(FullContext { network, .. }): State<FullContext<AM, RM, SM, A, G>>,
    _origin_header: Option<TypedHeader<Origin>>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    // Check if we have stored passkeys
    if !std::path::Path::new("/tmp/stored_passkey.json").exists() {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("No credentials found - registration required".to_string())
            .unwrap();
    }

    // Load the stored passkey
    let passkey_data = match std::fs::read("/tmp/stored_passkey.json") {
        Ok(data) => data,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("No credentials found - registration required".to_string())
                .unwrap();
        }
    };

    let passkey: Passkey = match serde_json::from_slice(&passkey_data) {
        Ok(pk) => pk,
        Err(e) => {
            eprintln!("Failed to deserialize stored passkey: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to load stored credentials".to_string())
                .unwrap();
        }
    };

    let rp_origin = Url::parse(
        &network
            .governance()
            .get_primary_auth_gateway()
            .await
            .unwrap(),
    )
    .unwrap();

    let rp_id = rp_origin.host_str().unwrap();

    let origin = Url::parse(&network.origin().await.unwrap()).unwrap();

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin)
        .build()
        .unwrap();

    // Start authentication
    let (rcr, state) = match webauthn.start_passkey_authentication(&[passkey]) {
        Ok((rcr, state)) => (rcr, state),
        Err(e) => {
            eprintln!("Failed to start authentication: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to start authentication".to_string())
                .unwrap();
        }
    };

    // Store the authentication state
    let state_json = serde_json::to_string(&state).unwrap();
    std::fs::write("/tmp/authentication_state.json", state_json.as_bytes()).unwrap();

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

pub(crate) async fn webauthn_authentication_finish_handler<AM, RM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { network, .. }): State<FullContext<AM, RM, SM, A, G>>,
    Json(auth_public_key_credential): Json<PublicKeyCredential>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
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

    let origin = Url::parse(&network.origin().await.unwrap()).unwrap();

    let webauthn = WebauthnBuilder::new(&rp_id, &rp_origin)
        .unwrap()
        .rp_name("Proven Network")
        .allow_cross_origin(true)
        .append_allowed_origin(&origin)
        .build()
        .unwrap();

    // Deserialize the authentication state from /tmp
    let auth_state_data = match std::fs::read("/tmp/authentication_state.json") {
        Ok(data) => data,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Authentication session not found".to_string())
                .unwrap();
        }
    };

    let auth_state: PasskeyAuthentication = match serde_json::from_slice(&auth_state_data) {
        Ok(state) => state,
        Err(e) => {
            eprintln!("Failed to deserialize authentication state: {}", e);
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to load authentication session".to_string())
                .unwrap();
        }
    };

    // Complete the authentication
    match webauthn.finish_passkey_authentication(&auth_public_key_credential, &auth_state) {
        Ok(auth_result) => {
            // Authentication successful
            println!("Authentication successful: {:?}", auth_result);

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
