mod application_http;
mod node;
mod rpc_http;
mod rpc_ws;
mod sessions;
mod webauthn;
mod whoami;

pub(crate) use application_http::{ApplicationHttpContext, application_http_handler};
pub(crate) use node::nats_cluster_endpoint_handler;
pub(crate) use rpc_http::http_rpc_handler;
pub(crate) use rpc_ws::ws_rpc_handler;
pub(crate) use sessions::{
    create_rola_challenge_handler, create_session_handler, verify_rola_handler,
};
pub(crate) use webauthn::{
    iframe_js_handler, webauthn_iframe_handler, webauthn_js_handler,
    webauthn_registration_finish_handler, webauthn_registration_start_handler,
    ws_worker_js_handler,
};
pub(crate) use whoami::whoami_handler;

fn parse_bearer_token(auth_header: &str) -> Result<String, &'static str> {
    if let Some(token) = auth_header.strip_prefix("Bearer ") {
        Ok(token.trim().to_string())
    } else {
        Err("Invalid authorization header format. Expected 'Bearer <token>'")
    }
}
