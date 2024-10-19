use super::RpcHandler;

use std::sync::Arc;

use axum::extract::ws::CloseFrame;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::Query;
use axum::routing::get;
use axum::Router;
use futures::{sink::SinkExt, stream::StreamExt};
use proven_runtime::Pool;
use proven_sessions::SessionManagement;
use proven_store::Store1;
use serde::Deserialize;
use std::borrow::Cow;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
struct QueryParams {
    session: String,
}

pub async fn create_rpc_router<T: SessionManagement + 'static, AS: Store1>(
    session_manager: T,
    runtime_pool: Arc<Pool<AS>>,
) -> Router {
    Router::new().route(
        "/ws",
        get(
            |ws: WebSocketUpgrade, query: Query<QueryParams>| async move {
                match session_manager.get_session(query.session.clone()).await {
                    Ok(Some(session)) => match RpcHandler::new(session.clone(), runtime_pool) {
                        Ok(rpc_handler) => {
                            ws.on_upgrade(move |socket| handle_socket(socket, rpc_handler))
                        }
                        Err(e) => {
                            error!("Error creating RpcHandler: {:?}", e);
                            ws.on_upgrade(move |socket| {
                                handle_socket_error(socket, Cow::from("Unrecoverable error."))
                            })
                        }
                    },
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

async fn handle_socket<AS: Store1>(socket: WebSocket, mut rpc_handler: RpcHandler<AS>) {
    let (mut sender, mut receiver) = socket.split();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);
    let mut send_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            match sender.send(Message::Binary(message)).await {
                Ok(_) => {}
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
                Message::Text(_) => break,
                Message::Close(Some(cf)) => {
                    info!("Close with code {} and reason `{}`", cf.code, cf.reason);
                    break;
                }
                Message::Close(None) => break,
                Message::Ping(_) => {}
                Message::Pong(_) => {}
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
