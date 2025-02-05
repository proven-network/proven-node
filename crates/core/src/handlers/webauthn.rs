use crate::PrimaryContext;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use axum_extra::TypedHeader;
use headers::Origin;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use url::Url;
use uuid::Uuid;
use webauthn_rs::prelude::*;

use axum::extract::Path;

static WEB_AUTHN_IFRAME_HTML: &str = include_str!("../../static/webauthn.html");
static WEB_AUTHN_JS: &str = include_str!("../../static/webauthn.js");

pub(crate) async fn webauthn_iframe_handler<AM, RM, SM, A>(
    Path(_application_id): Path<String>,
    State(PrimaryContext { .. }): State<PrimaryContext<AM, RM, SM, A>>,
    origin_header: Option<TypedHeader<Origin>>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
    A: Attestor,
{
    let origin = match origin_header {
        Some(value) => value.to_string(),
        None => "proven.local".to_string(),
    };

    Response::builder()
        .header(
            "Content-Security-Policy",
            format!("frame-ancestors {origin}"),
        )
        .header(
            "X-Frame-Options",
            format!("ALLOW-FROM frame-ancestors {origin}"),
        )
        .body(WEB_AUTHN_IFRAME_HTML.to_string())
        .unwrap()
}

pub(crate) async fn webauthn_js_handler() -> impl IntoResponse {
    Response::builder()
        .header("Content-Type", "application/javascript")
        .body(WEB_AUTHN_JS.to_string())
        .unwrap()
}

pub(crate) async fn webauthn_registration_start_handler<AM, RM, SM, A>(
    State(PrimaryContext { .. }): State<PrimaryContext<AM, RM, SM, A>>,
    _origin_header: Option<TypedHeader<Origin>>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
    A: Attestor,
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

pub(crate) async fn webauthn_registration_finish_handler<AM, RM, SM, A>(
    Path(_application_id): Path<String>,
    State(PrimaryContext { .. }): State<PrimaryContext<AM, RM, SM, A>>,
    Json(register_public_key_credential): Json<RegisterPublicKeyCredential>,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
    A: Attestor,
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
