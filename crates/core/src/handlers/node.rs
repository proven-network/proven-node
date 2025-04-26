use crate::LightContext;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum_extra::TypedHeader;
use bytes::Bytes;
use headers::{Header, HeaderValue};
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_headers::{Attestation, Nonce};

// Helper function for creating error responses
fn error_response(status: StatusCode, message: String) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(message))
        .unwrap()
}

pub(crate) async fn nats_cluster_endpoint_handler<A, G>(
    State(LightContext { network }): State<LightContext<A, G>>,
    nonce_header: Option<TypedHeader<Nonce>>,
) -> Response
where
    A: Attestor,
    G: Governance,
{
    let nonce = nonce_header
        .map(|TypedHeader(Nonce(bytes))| bytes)
        .unwrap_or_else(Bytes::new);

    match network.attested_nats_cluster_endpoint(nonce).await {
        Ok(attested_data) => {
            let response_attestation_header = Attestation(attested_data.attestation);
            Response::builder()
                .status(StatusCode::OK)
                .header(
                    Attestation::name(),
                    response_attestation_header
                        .encode_to_value()
                        .expect("Failed to encode attestation header"),
                )
                .body(Body::from(attested_data.data))
                .unwrap()
        }
        Err(e) => {
            tracing::error!("Failed to get NATS cluster endpoint: {}", e);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get NATS cluster endpoint: {}", e),
            )
        }
    }
}

// Helper extension trait to encode headers::Header to HeaderValue
// Needed because Response::header() expects HeaderValue
trait HeaderExt {
    fn encode_to_value(&self) -> Result<HeaderValue, headers::Error>;
}

impl<H> HeaderExt for H
where
    H: headers::Header,
{
    fn encode_to_value(&self) -> Result<HeaderValue, headers::Error> {
        let mut values = Vec::new();
        self.encode(&mut values);
        values.pop().ok_or_else(headers::Error::invalid)
    }
}
