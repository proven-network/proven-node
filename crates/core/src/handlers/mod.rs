mod application_http;
mod node;
mod rpc_http;
mod rpc_ws;
mod sessions;
mod static_files;
mod webauthn;
mod whoami;

pub(crate) use application_http::{ApplicationHttpContext, application_http_handler};
pub(crate) use node::nats_cluster_endpoint_handler;
pub(crate) use rpc_http::http_rpc_handler;
pub(crate) use rpc_ws::ws_rpc_handler;
pub(crate) use sessions::create_session_handler;
pub(crate) use static_files::{
    bridge_iframe_html_handler, bridge_iframe_js_handler, broker_worker_js_handler,
    button_iframe_html_handler, button_iframe_js_handler, register_iframe_html_handler,
    register_iframe_js_handler, rpc_iframe_html_handler, rpc_iframe_js_handler,
    rpc_worker_js_handler, sdk_js_handler,
};
pub(crate) use webauthn::{
    webauthn_authentication_finish_handler, webauthn_authentication_start_handler,
    webauthn_registration_finish_handler, webauthn_registration_start_handler,
};
pub(crate) use whoami::whoami_handler;

fn parse_bearer_token(auth_header: &str) -> Result<String, &'static str> {
    if let Some(token) = auth_header.strip_prefix("Bearer ") {
        Ok(token.trim().to_string())
    } else {
        Err("Invalid authorization header format. Expected 'Bearer <token>'")
    }
}
