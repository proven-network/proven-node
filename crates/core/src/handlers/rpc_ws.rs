use crate::FullContext;
use crate::rpc::RpcHandler;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures::{sink::SinkExt, stream::StreamExt};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use serde::Deserialize;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct SessionQuery {
    pub session: Uuid,
}

async fn handle_socket_error(mut socket: WebSocket, reason: &str) {
    error!("Error handling websocket: {}", reason);

    socket
        .send(Message::Close(Some(CloseFrame {
            code: axum::extract::ws::close_code::ERROR,
            reason: reason.into(),
        })))
        .await
        .ok();
}

pub(crate) async fn ws_rpc_handler<AM, RM, IM, PM, SM, A, G>(
    Path(application_id): Path<Uuid>,
    Query(SessionQuery {
        session: session_id,
    }): Query<SessionQuery>,
    State(FullContext {
        application_manager,
        identity_manager,
        sessions_manager,
        runtime_pool_manager,
        ..
    }): State<FullContext<AM, RM, IM, PM, SM, A, G>>,
    ws: WebSocketUpgrade,
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
    let session = match sessions_manager
        .get_session(&application_id, &session_id)
        .await
    {
        Ok(Some(session)) => session,
        Ok(None) => {
            return ws.on_upgrade(|socket| handle_socket_error(socket, "Session not found"));
        }
        Err(e) => {
            error!("Error getting session: {:?}", e);
            return ws.on_upgrade(|socket| handle_socket_error(socket, "Invalid session"));
        }
    };

    let rpc_handler = RpcHandler::new(
        application_manager,
        identity_manager,
        sessions_manager,
        runtime_pool_manager,
        session,
    );

    ws.on_upgrade(move |socket| handle_socket(socket, rpc_handler))
}

pub(crate) async fn management_ws_rpc_handler<AM, RM, IM, PM, SM, A, G>(
    Query(SessionQuery {
        session: session_id,
    }): Query<SessionQuery>,
    State(FullContext {
        application_manager,
        identity_manager,
        sessions_manager,
        runtime_pool_manager,
        ..
    }): State<FullContext<AM, RM, IM, PM, SM, A, G>>,
    ws: WebSocketUpgrade,
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
    let session = match sessions_manager.get_management_session(&session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return ws
                .on_upgrade(|socket| handle_socket_error(socket, "Management session not found"));
        }
        Err(e) => {
            error!("Error getting management session: {:?}", e);
            return ws
                .on_upgrade(|socket| handle_socket_error(socket, "Invalid management session"));
        }
    };

    let rpc_handler = RpcHandler::new(
        application_manager,
        identity_manager,
        sessions_manager,
        runtime_pool_manager,
        session,
    );

    ws.on_upgrade(move |socket| handle_socket(socket, rpc_handler))
}

async fn handle_socket<AM, IM, SM, RM>(
    socket: WebSocket,
    mut rpc_handler: RpcHandler<AM, IM, SM, RM>,
) where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
    RM: RuntimePoolManagement,
{
    let (mut sender, mut receiver) = socket.split();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(100);
    let mut send_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            match sender.send(Message::Binary(message)).await {
                Ok(()) => {}
                Err(e) => {
                    error!("Error sending message: {:?}", e);
                    break;
                }
            }
        }
    });

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(message)) = receiver.next().await {
            match message {
                Message::Binary(data) => match rpc_handler.handle_rpc(data).await {
                    Ok(response) => {
                        tx.send(response).await.ok();
                    }
                    Err(e) => {
                        error!("Error handling RPC: {:?}", e);
                        break;
                    }
                },
                // Plaintext is always unexpected
                Message::Text(_) | Message::Close(None) => break,
                Message::Close(Some(cf)) => {
                    info!("Close with code {} and reason `{}`", cf.code, cf.reason);
                    break;
                }
                Message::Ping(_) | Message::Pong(_) => {}
            }
        }
    });

    // If any one of the tasks exit, abort the other.
    tokio::select! {
        _ = (&mut send_task) => {
            recv_task.abort();
        },
        _ = (&mut recv_task) => {
            send_task.abort();
        }
    }

    info!("Websocket context destroyed");
}
