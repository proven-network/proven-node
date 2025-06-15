use crate::FullContext;
use crate::rpc::RpcHandler;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
use serde::Deserialize;
use tracing::error;

#[derive(Deserialize)]
pub struct SessionQuery {
    pub session: String,
}

pub(crate) async fn http_rpc_handler<AM, RM, IM, A, G>(
    Path(application_id): Path<String>,
    Query(SessionQuery {
        session: session_id,
    }): Query<SessionQuery>,
    State(FullContext {
        application_manager,
        runtime_pool_manager,
        identity_manager,
        ..
    }): State<FullContext<AM, RM, IM, A, G>>,
    body: Bytes,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    // TODO: Could probably do a local LRU cache for sessions to avoid lookup on every request
    let session = match identity_manager
        .get_session(&application_id, &session_id)
        .await
    {
        Ok(Some(session)) => session,
        Ok(None) => {
            return Response::builder()
                .status(401)
                .body(Body::from("Session not found"))
                .unwrap();
        }
        Err(e) => {
            error!("Error getting session: {:?}", e);
            return Response::builder()
                .status(401)
                .body(Body::from("Invalid session"))
                .unwrap();
        }
    };

    let Ok(mut rpc_handler) = RpcHandler::new(
        application_manager,
        runtime_pool_manager,
        identity_manager,
        application_id,
        session,
    ) else {
        error!("Error creating RpcHandler");
        return Response::builder()
            .status(400)
            .body(Body::from("Unrecoverable error."))
            .unwrap();
    };

    match rpc_handler.handle_rpc(body).await {
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
    }
}
