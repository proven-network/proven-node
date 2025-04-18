use crate::FullContext;

use super::parse_bearer_token;
use crate::rpc::RpcHandler;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
use tracing::error;

pub(crate) async fn http_rpc_handler<AM, RM, SM, A, G>(
    Path(application_id): Path<String>,
    State(FullContext {
        application_manager,
        network: _,
        runtime_pool_manager,
        session_manager,
    }): State<FullContext<AM, RM, SM, A, G>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
    A: Attestor,
    G: Governance,
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
        match session_manager
            .get_session(&application_id, &session_id)
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
    } else {
        None
    };

    match maybe_session {
        Some(session) => {
            match RpcHandler::new(
                application_manager,
                runtime_pool_manager,
                application_id,
                session,
            ) {
                Ok(mut rpc_handler) => match rpc_handler.handle_rpc(body).await {
                    Ok(response) => {
                        let body = Body::from(response);
                        Response::builder().body(body).unwrap()
                    }
                    Err(e) => {
                        error!("Error: {:?}", e);
                        Response::builder()
                            .status(400)
                            .body(format!("Error: {e:?}").into())
                            .unwrap()
                    }
                },
                Err(e) => {
                    error!("Error: {:?}", e);
                    Response::builder()
                        .status(400)
                        .body(format!("Error: {e:?}").into())
                        .unwrap()
                }
            }
        }
        None => Response::builder()
            .status(401)
            .body("Unauthenticated RPC not yet supported".into())
            .unwrap(),
    }
}
