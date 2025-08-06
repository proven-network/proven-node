use crate::FullContext;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum_extra::TypedHeader;
use headers::Referer;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use proven_topology::TopologyAdaptor;
use proven_util::Origin;

use axum::extract::Path;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

// Helper function to read file content with hot reload support
fn read_file_content(compile_time_content: &'static str, runtime_path: &str) -> String {
    #[cfg(debug_assertions)]
    {
        // In debug builds, try to read from disk for hot reload
        let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let file_path = current_dir.join("crates/gateway").join(runtime_path);

        match fs::read_to_string(&file_path) {
            Ok(content) => content,
            Err(_) => compile_time_content.to_string(),
        }
    }
    #[cfg(not(debug_assertions))]
    {
        // In release builds, use compile-time embedded content
        compile_time_content.to_string()
    }
}

macro_rules! iframe_handler {
    ($handler_name:ident, $html_const:ident, $runtime_path:expr) => {
        pub(crate) async fn $handler_name<A, G, AM, RM, IM, PM, SM>(
            Path(application_id): Path<Uuid>,
            State(FullContext {
                application_manager,
                ..
            }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
            referer_header: TypedHeader<Referer>,
        ) -> impl IntoResponse
        where
            A: Attestor,
            G: TopologyAdaptor,
            AM: ApplicationManagement,
            RM: RuntimePoolManagement,
            IM: IdentityManagement,
            PM: PasskeyManagement,
            SM: SessionManagement,
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

            let html_content = read_file_content($html_const, $runtime_path);

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
                Html(html_content),
            )
                .into_response()
        }
    };
}

macro_rules! js_handler {
    ($handler_name:ident, $js_const:ident, $runtime_path:expr) => {
        pub(crate) async fn $handler_name() -> impl IntoResponse {
            let js_content = read_file_content($js_const, $runtime_path);
            ([("Content-Type", "application/javascript")], js_content)
        }
    };
}

// Iframe HTML
const BRIDGE_IFRAME_HTML: &str = include_str!("../../static/iframes/bridge/bridge.html");
const CONNECT_IFRAME_HTML: &str = include_str!("../../static/iframes/connect/connect.html");
const REGISTER_IFRAME_HTML: &str = include_str!("../../static/iframes/register/register.html");
const RPC_IFRAME_HTML: &str = include_str!("../../static/iframes/rpc/rpc.html");
const STATE_IFRAME_HTML: &str = include_str!("../../static/iframes/state/state.html");
iframe_handler!(
    bridge_iframe_html_handler,
    BRIDGE_IFRAME_HTML,
    "static/iframes/bridge/bridge.html"
);
iframe_handler!(
    connect_iframe_html_handler,
    CONNECT_IFRAME_HTML,
    "static/iframes/connect/connect.html"
);
iframe_handler!(
    register_iframe_html_handler,
    REGISTER_IFRAME_HTML,
    "static/iframes/register/register.html"
);
iframe_handler!(
    rpc_iframe_html_handler,
    RPC_IFRAME_HTML,
    "static/iframes/rpc/rpc.html"
);
iframe_handler!(
    state_iframe_html_handler,
    STATE_IFRAME_HTML,
    "static/iframes/state/state.html"
);

// Iframe JS
const BRIDGE_IFRAME_JS: &str = include_str!("../../dist/iframes/bridge/bridge.js");
const CONNECT_IFRAME_JS: &str = include_str!("../../dist/iframes/connect/connect.js");
const REGISTER_IFRAME_JS: &str = include_str!("../../dist/iframes/register/register.js");
const RPC_IFRAME_JS: &str = include_str!("../../dist/iframes/rpc/rpc.js");
const STATE_IFRAME_JS: &str = include_str!("../../dist/iframes/state/state.js");
js_handler!(
    bridge_iframe_js_handler,
    BRIDGE_IFRAME_JS,
    "dist/iframes/bridge/bridge.js"
);
js_handler!(
    connect_iframe_js_handler,
    CONNECT_IFRAME_JS,
    "dist/iframes/connect/connect.js"
);
js_handler!(
    register_iframe_js_handler,
    REGISTER_IFRAME_JS,
    "dist/iframes/register/register.js"
);
js_handler!(
    rpc_iframe_js_handler,
    RPC_IFRAME_JS,
    "dist/iframes/rpc/rpc.js"
);
js_handler!(
    state_iframe_js_handler,
    STATE_IFRAME_JS,
    "dist/iframes/state/state.js"
);

// Shared workers
const BROKER_WORKER_JS: &str = include_str!("../../dist/workers/broker-worker.js");
const RPC_WORKER_JS: &str = include_str!("../../dist/workers/rpc-worker.js");
const STATE_WORKER_JS: &str = include_str!("../../dist/workers/state-worker.js");
js_handler!(
    broker_worker_js_handler,
    BROKER_WORKER_JS,
    "dist/workers/broker-worker.js"
);
js_handler!(
    rpc_worker_js_handler,
    RPC_WORKER_JS,
    "dist/workers/rpc-worker.js"
);
js_handler!(
    state_worker_js_handler,
    STATE_WORKER_JS,
    "dist/workers/state-worker.js"
);
