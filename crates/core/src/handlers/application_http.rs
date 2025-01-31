use super::parse_bearer_token;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::IntoResponse;
use axum::response::Response;
use bytes::Bytes;
use proven_runtime::{ExecutionRequest, HandlerSpecifier, ModuleLoader, RuntimePoolManagement};
use proven_sessions::SessionManagement;

#[derive(Clone)]
pub(crate) struct ApplicationHttpContext<RM, SM>
where
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    pub application_id: String,
    pub handler_specifier: HandlerSpecifier,
    pub module_loader: ModuleLoader,
    pub requires_session: bool,
    pub runtime_pool_manager: RM,
    pub session_manager: SM,
}

pub(crate) async fn application_http_handler<RM, SM>(
    State(ApplicationHttpContext {
        application_id,
        handler_specifier,
        module_loader,
        requires_session,
        runtime_pool_manager,
        session_manager,
    }): State<ApplicationHttpContext<RM, SM>>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse
where
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    let maybe_session_id = match headers.get("Authorization") {
        Some(header) => match header.to_str() {
            Ok(header_str) => match parse_bearer_token(header_str) {
                Ok(token) => Some(token),
                Err(e) => return Response::builder().status(401).body(Body::from(e)).unwrap(),
            },
            Err(_) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid authorization header"))
                    .unwrap()
            }
        },
        None => None,
    };

    let maybe_session = if let Some(session_id) = maybe_session_id {
        match session_manager
            .get_session(&application_id, &session_id)
            .await
        {
            Ok(Some(session)) => Some(session),
            Ok(None) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid session"))
                    .unwrap()
            }
            Err(_) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid token"))
                    .unwrap()
            }
        }
    } else if requires_session {
        return Response::builder()
            .status(401)
            .body(Body::from("Authorization header required"))
            .unwrap();
    } else {
        None
    };

    let path = uri.path();
    let query = uri.query();
    let body = if body.is_empty() { None } else { Some(body) };

    let execution_request = if let Some(session) = maybe_session {
        ExecutionRequest::HttpWithUserContext {
            accounts: session.account_addresses,
            body,
            dapp_definition_address: application_id,
            handler_specifier,
            identity: session.identity_address,
            method,
            path: path.to_string(),
            query: query.map(String::from),
        }
    } else {
        ExecutionRequest::Http {
            body,
            dapp_definition_address: application_id,
            handler_specifier,
            method,
            path: path.to_string(),
            query: query.map(String::from),
        }
    };

    let result = runtime_pool_manager
        .execute(module_loader, execution_request)
        .await;

    if let Err(err) = result {
        return Response::builder()
            .status(500)
            .body(Body::from(format!("Error: {err:?}")))
            .unwrap();
    }

    let execution_result = result.unwrap();

    let json_output = serde_json::to_string(&execution_result.output).unwrap();

    Response::builder()
        .status(200)
        .body(Body::from(json_output))
        .unwrap()
}
