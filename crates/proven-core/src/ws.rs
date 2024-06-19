use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::CloseFrame;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::Query;
use axum::routing::get;
use axum::Router;
use axum_extra::TypedHeader;
use coset::{iana, CborSerializable};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::Deserialize;
use std::borrow::Cow;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use tracing::{debug, error, info};

#[derive(Debug, Deserialize)]
struct ConnectParams {
    session: String,
}

pub async fn create_websocket_handler(nats_client: async_nats::Client) -> Router {
    let sessions_js_context = async_nats::jetstream::new(nats_client.clone());
    let sessions_store = sessions_js_context.get_key_value("sessions").await.unwrap();

    Router::new().route(
        "/ws",
        get(
            |ws: WebSocketUpgrade,
             user_agent: Option<TypedHeader<headers::UserAgent>>,
             query: Query<ConnectParams>,
             ConnectInfo(addr): ConnectInfo<SocketAddr>| async move {
                let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
                    user_agent.to_string()
                } else {
                    String::from("Unknown browser")
                };
                info!("`{user_agent}` at {addr} connected.");
                info!("Query: {:?}", query);

                match sessions_store.get(query.session.clone()).await {
                    Ok(Some(session_cbor)) => {
                        match serde_cbor::from_slice::<Session>(&session_cbor) {
                            Ok(session) => {
                                info!("{:?}", session);
                                ws.on_upgrade(move |socket| handle_socket(socket, session, addr))
                            }
                            Err(e) => {
                                info!("Error deserializing session {}: {:?}", query.session, e);
                                ws.on_upgrade(move |socket| {
                                    handle_socket_error(socket, Cow::from("Session not valid."))
                                })
                            }
                        }
                    }
                    Ok(None) => {
                        info!("No session {}", query.session);

                        ws.on_upgrade(move |socket| {
                            handle_socket_error(socket, Cow::from("Session not valid."))
                        })
                    }
                    Err(e) => {
                        info!(
                            "Error getting challenge for session {}: {:?}",
                            query.session, e
                        );

                        ws.on_upgrade(move |socket| {
                            handle_socket_error(socket, Cow::from("Could not restore session."))
                        })
                    }
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

/// Actual websocket statemachine (one will be spawned per connection)
async fn handle_socket(socket: WebSocket, session: Session, who: SocketAddr) {
    let signing_key_bytes: [u8; 32] = match session.clone().signing_key.try_into() {
        Ok(skb) => skb,
        Err(e) => {
            error!("Error converting signing key bytes: {:?}", e);
            handle_socket_error(socket, Cow::from("Unrecoverable error.")).await;
            return;
        }
    };
    let mut signing_key = SigningKey::from_bytes(&signing_key_bytes);

    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);

    let hello_string = format!("Hello {}", session.identity_address);
    let hello_bytes = serde_cbor::to_vec(&hello_string).unwrap();
    tx.send(hello_bytes.clone()).await.unwrap();
    tx.send(hello_bytes.clone()).await.unwrap();
    tx.send(hello_bytes).await.unwrap();

    // Spawn a task that will push several messages to the client (does not matter what client does)
    let mut send_task = tokio::spawn(async move {
        let mut msg_count = 0;

        while let Some(message) = rx.recv().await {
            let protected = coset::HeaderBuilder::new()
                .algorithm(iana::Algorithm::EdDSA)
                .build();

            let sign1 = coset::CoseSign1Builder::new()
                .protected(protected)
                .payload(message)
                .create_signature(b"", |pt| signing_key.sign(pt).to_vec())
                .build();

            match sign1.to_vec() {
                Ok(signed_bytes) => {
                    msg_count += 1;
                    let _ = sender.send(Message::Binary(signed_bytes)).await;
                }
                Err(e) => {
                    error!("Error signing message for {who}: {:?}", e);
                }
            }
        }

        msg_count
    });

    // This second task will receive messages from client and print them on server console
    let mut recv_task = tokio::spawn(async move {
        let mut cnt = 0;
        while let Some(Ok(msg)) = receiver.next().await {
            cnt += 1;
            // print message and break if instructed to do so
            if process_message(msg, session.clone(), who).is_break() {
                break;
            }
        }
        cnt
    });

    // If any one of the tasks exit, abort the other.
    tokio::select! {
        rv_a = (&mut send_task) => {
            match rv_a {
                Ok(a) => info!("{a} messages sent to {who}"),
                Err(a) => info!("Error sending messages {a:?}")
            }
            recv_task.abort();
        },
        rv_b = (&mut recv_task) => {
            match rv_b {
                Ok(b) => info!("Received {b} messages"),
                Err(b) => info!("Error receiving messages {b:?}")
            }
            send_task.abort();
        }
    }

    // returning from the handler closes the websocket connection
    info!("Websocket context {who} destroyed");
}

/// helper to print contents of messages to stdout. Has special treatment for Close.
fn process_message(msg: Message, session: Session, who: SocketAddr) -> ControlFlow<(), ()> {
    match msg {
        Message::Text(t) => {
            debug!("Unexpected text message from {who}: {t}");

            return ControlFlow::Break(());
        }
        Message::Binary(d) => {
            let verifying_key_bytes: [u8; 32] = match session.verifying_key.try_into() {
                Ok(vkb) => vkb,
                Err(e) => {
                    error!("Error converting signing key bytes: {:?}", e);
                    return ControlFlow::Break(());
                }
            };

            let verifying_key = match VerifyingKey::from_bytes(&verifying_key_bytes) {
                Ok(vk) => vk,
                Err(e) => {
                    error!("Error creating verifying key: {:?}", e);
                    return ControlFlow::Break(());
                }
            };

            let sign1 = match coset::CoseSign1::from_slice(&d) {
                Ok(s1) => s1,
                Err(e) => {
                    error!("Error parsing COSE message: {:?}", e);
                    return ControlFlow::Break(());
                }
            };

            let verification = sign1.verify_signature(b"", |signature_bytes, pt| {
                match Signature::from_slice(signature_bytes) {
                    Ok(signature) => verifying_key.verify(pt, &signature),
                    Err(e) => Err(e),
                }
            });

            match verification {
                Ok(()) => {
                    if let Some(p) = sign1.payload {
                        info!(">>> {who} sent str: {p:?}")
                    }
                }
                Err(e) => {
                    error!("Error verifying signed message: {:?}", e);
                    return ControlFlow::Break(());
                }
            }
        }
        Message::Close(c) => {
            if let Some(cf) = c {
                info!(
                    ">>> {} sent close with code {} and reason `{}`",
                    who, cf.code, cf.reason
                );
            } else {
                info!(">>> {who} somehow sent close message without CloseFrame");
            }
            return ControlFlow::Break(());
        }

        Message::Ping(_) => {}
        Message::Pong(_) => {}
    }
    ControlFlow::Continue(())
}
