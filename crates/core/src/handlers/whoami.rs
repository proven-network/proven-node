//! Handler for the /whoami endpoint.

use axum::extract::State;
use axum::response::{Json, Response};
use proven_governance::Governance;
use serde_json::json;
use tracing::error;

use crate::PrimaryContext;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;

/// Handler for the `/whoami` endpoint.
/// Returns the node information from network.get_self().
pub async fn whoami_handler<AM, RM, SM, A, G>(
    State(PrimaryContext { network, .. }): State<PrimaryContext<AM, RM, SM, A, G>>,
) -> Result<Json<serde_json::Value>, Response>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
{
    match network.get_self().await {
        Ok(node) => {
            let response = json!({
                "node": {
                    "fqdn": node.fqdn(),
                    "public_key": node.public_key(),
                    "region": node.region(),
                    "availability_zone": node.availability_zone(),
                    "specializations": node.specializations(),
                }
            });
            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to get node information: {}", e);
            Err(Response::builder()
                .status(500)
                .body(format!("Failed to get node information: {}", e).into())
                .unwrap())
        }
    }
}
