mod application_http;
mod rpc_http;
mod rpc_ws;
mod sessions;

pub(crate) use application_http::{ApplicationHttpContext, application_http_handler};
pub(crate) use rpc_http::http_rpc_handler;
pub(crate) use rpc_ws::ws_rpc_handler;
pub(crate) use sessions::{create_challenge_handler, verify_session_handler};

fn parse_bearer_token(auth_header: &str) -> Result<String, &'static str> {
    if let Some(token) = auth_header.strip_prefix("Bearer ") {
        Ok(token.trim().to_string())
    } else {
        Err("Invalid authorization header format. Expected 'Bearer <token>'")
    }
}
