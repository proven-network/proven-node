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
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use serde::Deserialize;
use tracing::error;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct SessionQuery {
    pub session: Uuid,
}

pub(crate) async fn http_rpc_handler<AM, RM, IM, PM, SM, A, G>(
    Path(application_id): Path<Uuid>,
    Query(SessionQuery {
        session: session_id,
    }): Query<SessionQuery>,
    State(FullContext {
        application_manager,
        runtime_pool_manager,
        identity_manager,
        sessions_manager,
        ..
    }): State<FullContext<AM, RM, IM, PM, SM, A, G>>,
    body: Bytes,
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
    // TODO: Could probably do a local LRU cache for sessions to avoid lookup on every request
    let session = match sessions_manager
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

    let mut rpc_handler = RpcHandler::new(
        application_id,
        application_manager,
        runtime_pool_manager,
        identity_manager,
        sessions_manager,
        session,
    );

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
