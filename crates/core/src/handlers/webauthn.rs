use crate::FullContext;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
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

const IFRAME_HTML: &str = include_str!("../../static/iframe.html");
const IFRAME_JS: &str = include_str!("../../static/iframe.js");
const WEBAUTHN_JS: &str = include_str!("../../static/webauthn.js");
const WS_WORKER_JS: &str = include_str!("../../static/ws-worker.js");

pub(crate) async fn webauthn_iframe_handler<AM, RM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { .. }): State<FullContext<AM, RM, SM, A, G>>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    let referer = headers
        .get("Referer")
        .map_or("http://localhost:3200", |r| r.to_str().unwrap());

    (
        [
            (
                "Content-Security-Policy",
                format!("frame-ancestors {referer}"),
            ),
            (
                "X-Frame-Options",
                format!("ALLOW-FROM frame-ancestors {referer}"),
            ),
        ],
        Html(IFRAME_HTML),
    )
}

pub(crate) async fn webauthn_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], WEBAUTHN_JS)
}

pub(crate) async fn iframe_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], IFRAME_JS)
}

pub(crate) async fn ws_worker_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], WS_WORKER_JS)
}

pub(crate) async fn webauthn_registration_start_handler<AM, RM, SM, A, G>(
    State(FullContext { .. }): State<FullContext<AM, RM, SM, A, G>>,
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

    // Configure webauthn
    let rp_id = "localhost"; // TODO: Replace with value from primary domains
    let rp_origin = Url::parse("http://localhost:3200").unwrap();
    let webauthn = WebauthnBuilder::new(rp_id, &rp_origin)
        .unwrap()
        .build()
        .unwrap();

    // Initiate passkey registration
    let (ccr, state) = webauthn
        .start_passkey_registration(user_unique_id, "user@proven.network", "Proven User", None)
        .expect("Failed to start registration");

    // Just serialize the state to /tmp with serde_json for testing
    let state_json = serde_json::to_string(&state).unwrap();
    let registration_state = state_json.as_bytes();
    std::fs::write("/tmp/registration_state.json", registration_state).unwrap();

    Json(ccr)
}

pub(crate) async fn webauthn_registration_finish_handler<AM, RM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { .. }): State<FullContext<AM, RM, SM, A, G>>,
    Json(register_public_key_credential): Json<RegisterPublicKeyCredential>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    // TODO: Replace with value from primary domains
    let rp_id = "localhost"; // TODO: Replace with value from primary domains
    let rp_origin = Url::parse("http://localhost:3200").unwrap();
    let webauthn = WebauthnBuilder::new(rp_id, &rp_origin)
        .unwrap()
        .build()
        .unwrap();

    // Deserialize the state from /tmp with serde_json for testing
    let registration_state = std::fs::read("/tmp/registration_state.json").unwrap();
    let registration_state: PasskeyRegistration =
        serde_json::from_slice(&registration_state).unwrap();

    // Complete the registration
    match webauthn.finish_passkey_registration(&register_public_key_credential, &registration_state)
    {
        Ok(_passkey) => {
            // TODO: Store passkey with user session/account
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
