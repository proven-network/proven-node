use crate::FullContext;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;

use axum::extract::Path;

const IFRAME_HTML: &str = include_str!("../../static/iframe.html");
const IFRAME_JS: &str = include_str!("../../static/iframe.js");
const SDK_JS: &str = include_str!("../../static/sdk.js");
const WS_WORKER_JS: &str = include_str!("../../static/ws-worker.js");

pub(crate) async fn iframe_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], IFRAME_JS)
}

pub(crate) async fn iframe_html_handler<AM, RM, SM, A, G>(
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

// TODO: temporary - should be extracted to external package (not served via node)
pub(crate) async fn sdk_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], SDK_JS)
}

pub(crate) async fn ws_worker_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], WS_WORKER_JS)
}
