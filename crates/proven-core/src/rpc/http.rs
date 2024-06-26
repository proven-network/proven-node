use super::RpcHandler;

use axum::extract::Query;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use proven_sessions::SessionManagement;
use serde::Deserialize;
use tracing::error;

#[derive(Debug, Deserialize)]
struct QueryParams {
    session: String,
}

pub async fn create_rpc_router<T: SessionManagement + 'static>(session_manager: T) -> Router {
    Router::new().route(
        "/rpc",
        post(|query: Query<QueryParams>, body: Bytes| async move {
            match session_manager.get_session(query.session.clone()).await {
                Ok(Some(session)) => match RpcHandler::new(session) {
                    Ok(mut rpc_handler) => match rpc_handler.handle_rpc(body.to_vec()).await {
                        Ok(response) => {
                            let bytes = bytes::Bytes::from(response);
                            let body = http_body_util::Full::new(bytes);
                            Response::builder().body(body).unwrap()
                        }
                        Err(e) => {
                            error!("Error: {:?}", e);
                            Response::builder()
                                .status(400)
                                .body(format!("Error: {:?}", e).into())
                                .unwrap()
                        }
                    },
                    Err(e) => {
                        error!("Error: {:?}", e);
                        Response::builder()
                            .status(400)
                            .body(format!("Error: {:?}", e).into())
                            .unwrap()
                    }
                },
                _ => Response::builder()
                    .status(400)
                    .body("Session not found".into())
                    .unwrap(),
            }
        }),
    )
}
