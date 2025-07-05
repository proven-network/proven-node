use crate::FullContext;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum_extra::TypedHeader;
use headers::Referer;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use proven_util::Origin;

use axum::extract::Path;
use uuid::Uuid;

macro_rules! iframe_handler {
    ($handler_name:ident, $html_const:ident) => {
        pub(crate) async fn $handler_name<AM, RM, IM, PM, SM, A, G>(
            Path(application_id): Path<Uuid>,
            State(FullContext {
                application_manager,
                ..
            }): State<FullContext<AM, RM, IM, PM, SM, A, G>>,
            referer_header: TypedHeader<Referer>,
        ) -> impl IntoResponse
        where
            AM: ApplicationManagement,
            RM: RuntimePoolManagement,
            IM: IdentityManagement,
            PM: PasskeyManagement,
            SM: SessionManagement,
            A: Attestor,
            G: Governance,
        {
            let application = match application_manager.get_application(&application_id).await {
                Ok(Some(application)) => application,
                Ok(None) => return (StatusCode::NOT_FOUND, "Application not found").into_response(),
                Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            };

            let referer_origin: Origin = match referer_header.0.to_string().try_into() {
                Ok(referer) => referer,
                Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            };

            if !application.allowed_origins.contains(&referer_origin) {
                return (StatusCode::FORBIDDEN, "Referer not allowed").into_response();
            }

            (
                [
                    (
                        "Content-Security-Policy",
                        format!("frame-ancestors {referer_origin}"),
                    ),
                    (
                        "X-Frame-Options",
                        format!("ALLOW-FROM frame-ancestors {referer_origin}"),
                    ),
                ],
                Html($html_const),
            )
                .into_response()
        }
    };
}

macro_rules! js_handler {
    ($handler_name:ident, $js_const:ident) => {
        pub(crate) async fn $handler_name() -> impl IntoResponse {
            ([("Content-Type", "application/javascript")], $js_const)
        }
    };
}

// Iframe HTML
const BRIDGE_IFRAME_HTML: &str = include_str!("../../static/iframes/bridge/bridge.html");
const CONNECT_IFRAME_HTML: &str = include_str!("../../static/iframes/connect/connect.html");
const REGISTER_IFRAME_HTML: &str = include_str!("../../static/iframes/register/register.html");
const RPC_IFRAME_HTML: &str = include_str!("../../static/iframes/rpc/rpc.html");
iframe_handler!(bridge_iframe_html_handler, BRIDGE_IFRAME_HTML);
iframe_handler!(connect_iframe_html_handler, CONNECT_IFRAME_HTML);
iframe_handler!(register_iframe_html_handler, REGISTER_IFRAME_HTML);
iframe_handler!(rpc_iframe_html_handler, RPC_IFRAME_HTML);

// Iframe JS
const BRIDGE_IFRAME_JS: &str = include_str!("../../dist/iframes/bridge/bridge.js");
const CONNECT_IFRAME_JS: &str = include_str!("../../dist/iframes/connect/connect.js");
const REGISTER_IFRAME_JS: &str = include_str!("../../dist/iframes/register/register.js");
const RPC_IFRAME_JS: &str = include_str!("../../dist/iframes/rpc/rpc.js");
js_handler!(bridge_iframe_js_handler, BRIDGE_IFRAME_JS);
js_handler!(connect_iframe_js_handler, CONNECT_IFRAME_JS);
js_handler!(register_iframe_js_handler, REGISTER_IFRAME_JS);
js_handler!(rpc_iframe_js_handler, RPC_IFRAME_JS);

// Shared workers
const BROKER_WORKER_JS: &str = include_str!("../../dist/workers/broker-worker.js");
const RPC_WORKER_JS: &str = include_str!("../../dist/workers/rpc-worker.js");
js_handler!(broker_worker_js_handler, BROKER_WORKER_JS);
js_handler!(rpc_worker_js_handler, RPC_WORKER_JS);
