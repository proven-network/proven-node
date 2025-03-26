use crate::LightContext;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use proven_attestation::Attestor;
use proven_governance::Governance;

pub(crate) async fn nats_cluster_endpoint_handler<A, G>(
    State(LightContext { network }): State<LightContext<A, G>>,
) -> impl IntoResponse
where
    A: Attestor,
    G: Governance,
{
    let nats_cluster_endpoint = network
        .nats_cluster_endpoint()
        .await
        .map_err(|e| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!(
                    "Failed to get NATS cluster endpoint: {e}"
                )))
                .unwrap()
        })
        .unwrap();

    nats_cluster_endpoint.to_string()
}
