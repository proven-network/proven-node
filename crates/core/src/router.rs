//! Router building and installation logic
//!
//! This module contains the logic for building and installing HTTP routers
//! for the core system in different modes.

use crate::Error;
use crate::handlers::{
    bridge_iframe_html_handler, bridge_iframe_js_handler, broker_worker_js_handler,
    connect_iframe_html_handler, connect_iframe_js_handler, create_management_session_handler,
    create_session_handler, http_rpc_handler, management_http_rpc_handler,
    management_ws_rpc_handler, nats_cluster_endpoint_handler, register_iframe_html_handler,
    register_iframe_js_handler, rpc_iframe_html_handler, rpc_iframe_js_handler,
    rpc_worker_js_handler, webauthn_authentication_finish_handler,
    webauthn_authentication_start_handler, webauthn_registration_finish_handler,
    webauthn_registration_start_handler, whoami_handler, ws_rpc_handler,
};
use crate::state::{FullContext, LightContext};

use axum::http::StatusCode;
use axum::response::Response as AxumResponse;
use axum::routing::{any, get, post};
use axum::{Json, Router};
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_network::NATS_CLUSTER_ENDPOINT_API_PATH;
use serde_json::json;
use tower_http::cors::CorsLayer;

/// Route constants for API endpoints
pub mod routes {
    /// Management session creation endpoint
    pub const NEW_MANAGEMENT_SESSION: &str = "/session";
    /// Management RPC endpoint
    pub const MANAGEMENT_HTTP_RPC: &str = "/rpc/http";
    /// Websocket for management
    pub const MANAGEMENT_WS_RPC: &str = "/rpc/ws";

    /// Application session creation endpoint
    pub const NEW_APPLICATION_SESSION: &str = "/app/{application_id}/session";
    /// Application RPC endpoint
    pub const APPLICATION_HTTP_RPC: &str = "/app/{application_id}/rpc/http";
    /// Application WebSocket endpoint
    pub const APPLICATION_WS_RPC: &str = "/app/{application_id}/rpc/ws";

    /// `WebAuthn` endpoints
    pub const WEBAUTHN_REGISTER_START: &str = "/webauthn/register/start";
    /// `WebAuthn` registration finish endpoint
    pub const WEBAUTHN_REGISTER_FINISH: &str = "/webauthn/register/finish";
    /// `WebAuthn` authentication start endpoint
    pub const WEBAUTHN_AUTHENTICATE_START: &str = "/webauthn/authenticate/start";
    /// `WebAuthn` authentication finish endpoint
    pub const WEBAUTHN_AUTHENTICATE_FINISH: &str = "/webauthn/authenticate/finish";

    /// Inter-node communication endpoint
    pub const WHOAMI: &str = "/whoami";

    /// Static file endpoints
    pub const IFRAME_BRIDGE_HTML: &str = "/app/{application_id}/iframes/bridge.html";
    /// Connect iframe HTML endpoint
    pub const IFRAME_CONNECT_HTML: &str = "/app/{application_id}/iframes/connect.html";
    /// Register iframe HTML endpoint
    pub const IFRAME_REGISTER_HTML: &str = "/app/{application_id}/iframes/register.html";
    /// RPC iframe HTML endpoint
    pub const IFRAME_RPC_HTML: &str = "/app/{application_id}/iframes/rpc.html";
    /// Bridge iframe JS endpoint
    pub const IFRAME_BRIDGE_JS: &str = "/app/{application_id}/iframes/bridge.js";
    /// Connect iframe JS endpoint
    pub const IFRAME_CONNECT_JS: &str = "/app/{application_id}/iframes/connect.js";
    /// Register iframe JS endpoint
    pub const IFRAME_REGISTER_JS: &str = "/app/{application_id}/iframes/register.js";
    /// RPC iframe JS endpoint
    pub const IFRAME_RPC_JS: &str = "/app/{application_id}/iframes/rpc.js";
    /// Broker worker JS endpoint
    pub const WORKER_BROKER_JS: &str = "/app/{application_id}/workers/broker-worker.js";
    /// RPC worker JS endpoint
    pub const WORKER_RPC_JS: &str = "/app/{application_id}/workers/rpc-worker.js";
}

/// Type alias for a function that can build bootstrapped routes
pub type BootstrappedRouterBuilder = Box<dyn Fn(Router) -> Router + Send + Sync>;

/// Router building utilities
pub struct RouterBuilder;

impl RouterBuilder {
    /// Create a base router with common routes
    pub fn create_base_router<A, G>(network: &proven_network::ProvenNetwork<G, A>) -> Router
    where
        A: Attestor,
        G: Governance,
    {
        let redirect_response = AxumResponse::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .expect("Failed to create redirect response");

        let light_ctx = LightContext {
            network: network.clone(),
        };

        let stateful_router = Router::new()
            .route(
                NATS_CLUSTER_ENDPOINT_API_PATH,
                get(nats_cluster_endpoint_handler),
            )
            .with_state(light_ctx);

        Router::new()
            .route("/", get(|| async { redirect_response }))
            .merge(stateful_router)
    }

    /// Add bootstrapped routes to a router
    pub fn add_bootstrapped_routes<AM, RM, IM, PM, SM, A, G>(
        router: Router,
        full_ctx: FullContext<AM, RM, IM, PM, SM, A, G>,
    ) -> Router
    where
        AM: proven_applications::ApplicationManagement + Clone,
        RM: proven_runtime::RuntimePoolManagement + Clone,
        IM: proven_identity::IdentityManagement + Clone,
        PM: proven_passkeys::PasskeyManagement + Clone,
        SM: proven_sessions::SessionManagement + Clone,
        A: Attestor + Clone,
        G: Governance + Clone,
    {
        let stateful_router = Router::new()
            // ** Sessions **
            .route(
                routes::NEW_APPLICATION_SESSION,
                post(create_session_handler),
            )
            .route(
                routes::NEW_MANAGEMENT_SESSION,
                post(create_management_session_handler),
            )
            // ** RPC **
            .route(
                routes::MANAGEMENT_HTTP_RPC,
                post(management_http_rpc_handler),
            )
            .route(routes::MANAGEMENT_WS_RPC, get(management_ws_rpc_handler))
            .route(routes::APPLICATION_HTTP_RPC, post(http_rpc_handler))
            .route(routes::APPLICATION_WS_RPC, get(ws_rpc_handler))
            // ** WebAuthn **
            .route(
                routes::WEBAUTHN_REGISTER_START,
                post(webauthn_registration_start_handler),
            )
            .route(
                routes::WEBAUTHN_REGISTER_FINISH,
                post(webauthn_registration_finish_handler),
            )
            .route(
                routes::WEBAUTHN_AUTHENTICATE_START,
                post(webauthn_authentication_start_handler),
            )
            .route(
                routes::WEBAUTHN_AUTHENTICATE_FINISH,
                post(webauthn_authentication_finish_handler),
            )
            // ** Inter-node communication **
            .route(routes::WHOAMI, get(whoami_handler))
            // ** Static files **
            // Iframe HTML
            .route(routes::IFRAME_BRIDGE_HTML, get(bridge_iframe_html_handler))
            .route(
                routes::IFRAME_CONNECT_HTML,
                get(connect_iframe_html_handler),
            )
            .route(
                routes::IFRAME_REGISTER_HTML,
                get(register_iframe_html_handler),
            )
            .route(routes::IFRAME_RPC_HTML, get(rpc_iframe_html_handler))
            // Iframe JS
            .route(routes::IFRAME_BRIDGE_JS, get(bridge_iframe_js_handler))
            .route(routes::IFRAME_CONNECT_JS, get(connect_iframe_js_handler))
            .route(routes::IFRAME_REGISTER_JS, get(register_iframe_js_handler))
            .route(routes::IFRAME_RPC_JS, get(rpc_iframe_js_handler))
            // Shared workers
            .route(routes::WORKER_BROKER_JS, get(broker_worker_js_handler))
            .route(routes::WORKER_RPC_JS, get(rpc_worker_js_handler))
            .with_state(full_ctx);

        router.merge(stateful_router)
    }

    /// Create a `WebAuthn` configuration router
    pub fn create_webauthn_router(alternates_auth_gateways: Vec<String>) -> Router {
        Router::new().route(
            "/.well-known/webauthn",
            get(move || {
                let gateways = alternates_auth_gateways.clone();
                async move {
                    Json(json!({
                        "origins": gateways
                    }))
                }
            }),
        )
    }

    /// Finalize a router with common layers
    pub fn finalize_router(router: Router) -> Router {
        router
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive())
    }
}

/// Router installation utilities
pub struct RouterInstaller;

impl RouterInstaller {
    /// Install a router to an HTTP server for a specific hostname
    pub async fn install_router<HS>(
        http_server: &HS,
        hostname: String,
        router: Router,
    ) -> Result<(), Error>
    where
        HS: proven_http::HttpServer,
    {
        http_server
            .set_router_for_hostname(hostname, router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))
    }
}

/// Create a bootstrapped router builder closure
pub fn create_bootstrapped_router_builder<AM, RM, IM, PM, SM, A, G>(
    full_ctx: FullContext<AM, RM, IM, PM, SM, A, G>,
) -> BootstrappedRouterBuilder
where
    AM: proven_applications::ApplicationManagement + Clone + 'static,
    RM: proven_runtime::RuntimePoolManagement + Clone + 'static,
    IM: proven_identity::IdentityManagement + Clone + 'static,
    PM: proven_passkeys::PasskeyManagement + Clone + 'static,
    SM: proven_sessions::SessionManagement + Clone + 'static,
    A: Attestor + Clone + 'static,
    G: Governance + Clone + 'static,
{
    Box::new(move |router| RouterBuilder::add_bootstrapped_routes(router, full_ctx.clone()))
}
