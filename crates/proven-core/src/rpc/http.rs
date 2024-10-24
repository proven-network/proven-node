use super::RpcHandler;

use std::sync::Arc;

use axum::extract::Query;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use proven_runtime::Pool;
use proven_sessions::SessionManagement;
use proven_store::{Store2, Store3};
use serde::Deserialize;
use tracing::error;

#[derive(Debug, Deserialize)]
struct QueryParams {
    session: String,
}

pub async fn create_rpc_router<
    T: SessionManagement + 'static,
    AS: Store2,
    PS: Store3,
    NS: Store3,
>(
    session_manager: T,
    runtime_pool: Arc<Pool<AS, PS, NS>>,
) -> Router {
    Router::new().route(
        "/rpc",
        post(|query: Query<QueryParams>, body: Bytes| async move {
            match session_manager.get_session(query.session.clone()).await {
                Ok(Some(session)) => match RpcHandler::new(session, runtime_pool) {
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
