use crate::LightContext;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use bytes::Bytes;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_headers::Nonce;

pub(crate) async fn nats_cluster_endpoint_handler<A, G>(
    State(LightContext { network }): State<LightContext<A, G>>,
    nonce_header: Option<TypedHeader<Nonce>>,
) -> impl IntoResponse
where
    A: Attestor,
    G: Governance,
{
    let nonce = nonce_header
        .map(|TypedHeader(Nonce(bytes))| bytes)
        .unwrap_or_else(|| Bytes::from_static(b"default_nonce_if_missing"));

    let attested_data = network
        .attested_nats_cluster_endpoint(nonce)
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
