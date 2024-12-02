use super::RpcHandler;

use axum::extract::ws::CloseFrame;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::Query;
use axum::routing::get;
use axum::Router;
use futures::{sink::SinkExt, stream::StreamExt};
use proven_applications::ApplicationManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use serde::Deserialize;
use std::borrow::Cow;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
struct QueryParams {
    session: String,
}

pub fn create_rpc_router<AM, RM, SM>(
    application_manager: AM,
    runtime_pool_manager: RM,
    session_manager: SM,
) -> Router
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    Router::new().route(
        "/ws",
        get(
            |ws: WebSocketUpgrade, query: Query<QueryParams>| async move {
                match session_manager
                    .get_session("TODO_APPLICATION_ID".to_string(), query.session.clone())
                    .await
                {
                    Ok(Some(session)) => {
                        match RpcHandler::new(
                            application_manager,
                            runtime_pool_manager,
                            session.clone(),
                        ) {
                            Ok(rpc_handler) => {
                                ws.on_upgrade(move |socket| handle_socket(socket, rpc_handler))
                            }
                            Err(e) => {
                                error!("Error creating RpcHandler: {:?}", e);
                                ws.on_upgrade(move |socket| {
                                    handle_socket_error(socket, Cow::from("Unrecoverable error."))
                                })
                            }
                        }
                    }
                    _ => ws.on_upgrade(move |socket| {
                        handle_socket_error(socket, Cow::from("Session not valid."))
                    }),
                }
            },
        ),
    )
}

async fn handle_socket_error(mut socket: WebSocket, reason: Cow<'static, str>) {
    socket
        .send(Message::Close(Some(CloseFrame {
            code: axum::extract::ws::close_code::ERROR,
            reason,
        })))
        .await
        .ok();
}

async fn handle_socket<AM: ApplicationManagement, RM: RuntimePoolManagement>(
    socket: WebSocket,
    mut rpc_handler: RpcHandler<AM, RM>,
) {
    let (mut sender, mut receiver) = socket.split();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);
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
