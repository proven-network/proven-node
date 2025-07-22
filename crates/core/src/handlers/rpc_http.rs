use crate::FullContext;
use crate::rpc::RpcHandler;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_identity::IdentityManagement;
use proven_logger::error;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use proven_topology::TopologyAdaptor;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct SessionQuery {
    pub session: Uuid,
}

pub(crate) async fn http_rpc_handler<A, G, AM, RM, IM, PM, SM>(
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
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    body: Bytes,
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
            error!("Error getting session: {e:?}");
            return Response::builder()
                .status(401)
                .body(Body::from("Invalid session"))
                .unwrap();
        }
    };

    let mut rpc_handler = RpcHandler::new(
        application_manager,
        identity_manager,
        sessions_manager,
        runtime_pool_manager,
        session,
    );

    match rpc_handler.handle_rpc(body).await {
        Ok(response) => {
            let body = Body::from(response);
            Response::builder().body(body).unwrap()
        }
        Err(e) => {
            error!("Error: {e:?}");
            Response::builder()
                .status(400)
                .body(format!("Error: {e:?}").into())
                .unwrap()
        }
    }
}

pub(crate) async fn management_http_rpc_handler<A, G, AM, RM, IM, PM, SM>(
    Query(SessionQuery {
        session: session_id,
    }): Query<SessionQuery>,
    State(FullContext {
        application_manager,
        runtime_pool_manager,
        identity_manager,
        sessions_manager,
        ..
    }): State<FullContext<A, G, AM, RM, IM, PM, SM>>,
    body: Bytes,
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
    // TODO: Could probably do a local LRU cache for sessions to avoid lookup on every request
    let session = match sessions_manager.get_management_session(&session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return Response::builder()
                .status(401)
                .body(Body::from("Management session not found"))
                .unwrap();
        }
        Err(e) => {
            error!("Error getting management session: {e:?}");
            return Response::builder()
                .status(401)
                .body(Body::from("Invalid management session"))
                .unwrap();
        }
    };

    let mut rpc_handler = RpcHandler::new(
        application_manager,
        identity_manager,
        sessions_manager,
        runtime_pool_manager,
        session,
    );

    match rpc_handler.handle_rpc(body).await {
        Ok(response) => {
            let body = Body::from(response);
            Response::builder().body(body).unwrap()
        }
        Err(e) => {
            error!("Error: {e:?}");
            Response::builder()
                .status(400)
                .body(format!("Error: {e:?}").into())
                .unwrap()
        }
    }
}
