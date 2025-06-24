use crate::FullContext;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;

use axum::extract::Path;

// SDK
const SDK_JS: &str = include_str!("../../static/sdk.js");

// Iframe HTML
const BRIDGE_IFRAME_HTML: &str = include_str!("../../static/iframes/bridge/bridge.html");
const CONNECT_IFRAME_HTML: &str = include_str!("../../static/iframes/connect/connect.html");
const REGISTER_IFRAME_HTML: &str = include_str!("../../static/iframes/register/register.html");
const RPC_IFRAME_HTML: &str = include_str!("../../static/iframes/rpc/rpc.html");

// Iframe JS
const BRIDGE_IFRAME_JS: &str = include_str!("../../static/iframes/bridge/bridge.js");
const CONNECT_IFRAME_JS: &str = include_str!("../../static/iframes/connect/connect.js");
const REGISTER_IFRAME_JS: &str = include_str!("../../static/iframes/register/register.js");
const RPC_IFRAME_JS: &str = include_str!("../../static/iframes/rpc/rpc.js");

// Shared workers
const BROKER_WORKER_JS: &str = include_str!("../../static/workers/broker-worker.js");
const RPC_WORKER_JS: &str = include_str!("../../static/workers/rpc-worker.js");

// TODO: temporary - should be extracted to external package (not served via node)
pub(crate) async fn sdk_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], SDK_JS)
}

pub(crate) async fn bridge_iframe_js_handler() -> impl IntoResponse {
    (
        [("Content-Type", "application/javascript")],
        BRIDGE_IFRAME_JS,
    )
}

pub(crate) async fn connect_iframe_js_handler() -> impl IntoResponse {
    (
        [("Content-Type", "application/javascript")],
        CONNECT_IFRAME_JS,
    )
}

pub(crate) async fn register_iframe_js_handler() -> impl IntoResponse {
    (
        [("Content-Type", "application/javascript")],
        REGISTER_IFRAME_JS,
    )
}

pub(crate) async fn rpc_iframe_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], RPC_IFRAME_JS)
}

pub(crate) async fn bridge_iframe_html_handler<AM, RM, IM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { .. }): State<FullContext<AM, RM, IM, SM, A, G>>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
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
        Html(BRIDGE_IFRAME_HTML),
    )
}

pub(crate) async fn connect_iframe_html_handler<AM, RM, IM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { .. }): State<FullContext<AM, RM, IM, SM, A, G>>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
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
        Html(CONNECT_IFRAME_HTML),
    )
}

pub(crate) async fn register_iframe_html_handler<AM, RM, IM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { .. }): State<FullContext<AM, RM, IM, SM, A, G>>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
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
        Html(REGISTER_IFRAME_HTML),
    )
}

pub(crate) async fn rpc_iframe_html_handler<AM, RM, IM, SM, A, G>(
    Path(_application_id): Path<String>,
    State(FullContext { .. }): State<FullContext<AM, RM, IM, SM, A, G>>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
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
        Html(RPC_IFRAME_HTML),
    )
}

pub(crate) async fn broker_worker_js_handler() -> impl IntoResponse {
    (
        [("Content-Type", "application/javascript")],
        BROKER_WORKER_JS,
    )
}

pub(crate) async fn rpc_worker_js_handler() -> impl IntoResponse {
    ([("Content-Type", "application/javascript")], RPC_WORKER_JS)
}
