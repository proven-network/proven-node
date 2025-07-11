//! Handler for the /whoami endpoint.

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Json, Response};
use proven_governance::Governance;
use serde_json::json;

use crate::FullContext;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;

/// Handler for the `/whoami` endpoint.
///
/// Returns the node information from `network.get_self()`.
///
/// # Errors
///
/// Returns an error if the node information cannot be retrieved.
pub async fn whoami_handler<AM, RM, IM, PM, SM, A, G>(
    State(FullContext { network, .. }): State<FullContext<AM, RM, IM, PM, SM, A, G>>,
) -> Result<Json<serde_json::Value>, Response>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
{
    let origin = network.origin().await.map_err(|e| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to get origin: {e}")))
            .unwrap()
    })?;

    let region = network.region().await.map_err(|e| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to get region: {e}")))
            .unwrap()
    })?;

    let availability_zone = network.availability_zone().await.map_err(|e| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to get availability zone: {e}")))
            .unwrap()
    })?;

    let specializations = network.specializations().await.map_err(|e| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("Failed to get specializations: {e}")))
            .unwrap()
    })?;

    let response = json!({
        "node": {
            "origin": origin,
            "public_key": hex::encode(network.public_key().to_bytes()),
            "region": region,
            "availability_zone": availability_zone,
            "specializations": specializations,
        }
    });

    Ok(Json(response))
}
