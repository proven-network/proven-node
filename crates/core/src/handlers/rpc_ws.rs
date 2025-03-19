use super::parse_bearer_token;
use crate::PrimaryContext;
use crate::rpc::RpcHandler;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use bytes::Bytes;
use futures::{sink::SinkExt, stream::StreamExt};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use tracing::{error, info};

async fn handle_socket_error(mut socket: WebSocket, reason: &str) {
    socket
        .send(Message::Close(Some(CloseFrame {
            code: axum::extract::ws::close_code::ERROR,
            reason: reason.into(),
        })))
        .await
        .ok();
}

pub(crate) async fn ws_rpc_handler<AM, RM, SM, A, G>(
    Path(application_id): Path<String>,
    State(PrimaryContext {
        application_manager,
        _attestor: _,
        network: _,
        runtime_pool_manager,
        session_manager,
    }): State<PrimaryContext<AM, RM, SM, A, G>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
{
    let Some(header) = headers.get("Authorization") else {
        return ws
            .on_upgrade(|socket| handle_socket_error(socket, "Authorization header required"));
    };

    let Ok(header_str) = header.to_str() else {
        return ws.on_upgrade(|socket| handle_socket_error(socket, "Invalid authorization header"));
    };

    let Ok(token) = parse_bearer_token(header_str) else {
        return ws.on_upgrade(|socket| handle_socket_error(socket, "Invalid bearer token format"));
    };

    let Ok(maybe_session) = session_manager.get_session(&application_id, &token).await else {
        return ws.on_upgrade(|socket| handle_socket_error(socket, "Invalid token"));
    };

    let Some(session) = maybe_session else {
        return ws.on_upgrade(|socket| handle_socket_error(socket, "Invalid session"));
    };

    let Ok(rpc_handler) = RpcHandler::new(
        application_manager,
        runtime_pool_manager,
        application_id,
        session,
    ) else {
        error!("Error creating RpcHandler");
        return ws.on_upgrade(|socket| handle_socket_error(socket, "Unrecoverable error."));
    };

    ws.on_upgrade(move |socket| handle_socket(socket, rpc_handler))
}

async fn handle_socket<AM: ApplicationManagement, RM: RuntimePoolManagement>(
    socket: WebSocket,
    mut rpc_handler: RpcHandler<AM, RM>,
) {
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
