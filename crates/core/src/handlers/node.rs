use crate::LightContext;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use bytes::Bytes;
use proven_attestation::Attestor;
use proven_governance::Governance;

pub(crate) async fn nats_cluster_endpoint_handler<A, G>(
    State(LightContext { network }): State<LightContext<A, G>>,
) -> impl IntoResponse
where
    A: Attestor,
    G: Governance,
{
    let attested_data = network
        .attested_nats_cluster_endpoint(Bytes::from_static(b"test_nonce"))
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

    Response::builder()
        .status(StatusCode::OK)
        .header(
            "X-Attestation",
            STANDARD_NO_PAD.encode(attested_data.attestation),
        )
        .body(Body::from(attested_data.data))
        .unwrap()
}
