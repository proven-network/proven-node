use super::parse_bearer_token;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::IntoResponse;
use axum::response::Response;
use bytes::Bytes;
use proven_attestation::{AttestationParams, Attestor};
use proven_identity::IdentityManagement;
use proven_runtime::{
    ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RuntimePoolManagement,
};
use proven_sessions::SessionManagement;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ApplicationHttpContext<RM, IM, SM, A>
where
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
{
    pub application_id: Uuid,
    pub attestor: A,
    pub handler_specifier: HandlerSpecifier,
    pub module_loader: ModuleLoader,
    pub requires_session: bool,
    pub runtime_pool_manager: RM,
    pub _identity_manager: IM,
    pub sessions_manager: SM,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn application_http_handler<RM, IM, SM, A>(
    State(ApplicationHttpContext {
        application_id,
        attestor,
        handler_specifier,
        module_loader,
        requires_session,
        runtime_pool_manager,
        sessions_manager,
        ..
    }): State<ApplicationHttpContext<RM, IM, SM, A>>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse
where
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
    A: Attestor,
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
                    .unwrap();
            }
        },
        None => None,
    };

    let maybe_session = if let Some(session_id) = maybe_session_id {
        match sessions_manager
            .get_session(&application_id, &Uuid::parse_str(&session_id).unwrap())
            .await
        {
            Ok(Some(session)) => Some(session),
            Ok(None) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid session"))
                    .unwrap();
            }
            Err(_) => {
                return Response::builder()
                    .status(401)
                    .body(Body::from("Invalid token"))
                    .unwrap();
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
        ExecutionRequest::HttpWithSession {
            application_id,
            body,
            handler_specifier,
            method,
            path: path.to_string(),
            query: query.map(String::from),
            session,
        }
    } else {
        ExecutionRequest::Http {
            application_id,
            body,
            handler_specifier,
            method,
            path: path.to_string(),
            query: query.map(String::from),
        }
    };

    let result = runtime_pool_manager
        .execute(module_loader, execution_request)
        .await;

    if let Err(error) = result {
        return Response::builder()
            .status(500)
            .body(Body::from(format!("Error: {error:?}")))
            .unwrap();
    }

    let execution_result = result.expect("becuase checked for err above");

    let response_code = match execution_result {
        ExecutionResult::Ok { .. } => 200,
        ExecutionResult::Error { .. } => 500,
    };

    let response_bytes = match execution_result {
        ExecutionResult::Ok { output, .. } => Bytes::from(serde_json::to_vec(&output).unwrap()),
        ExecutionResult::Error { error, .. } => Bytes::from(serde_json::to_vec(&error).unwrap()),
    };

    match headers
        .get("X-Request-Attestation")
        .and_then(|h| h.to_str().ok())
        .map(ToString::to_string)
        .map(Bytes::from)
        .map(|nonce| async move {
            attestor
                .attest(AttestationParams {
                    nonce: Some(nonce),
                    user_data: None,
                    public_key: None,
                })
                .await
                .unwrap() // TODO: handle error
        }) {
        Some(attestation_future) => {
            let attestation_document = attestation_future.await;
            let attestation_hex = hex::encode(attestation_document);

            Response::builder()
                .status(response_code)
                .header("Content-Type", "application/json")
                .header("X-Attestation", attestation_hex)
                .body(Body::from(response_bytes))
                .unwrap()
        }
        None => Response::builder()
            .status(response_code)
            .body(Body::from(response_bytes))
            .unwrap(),
    }
}
