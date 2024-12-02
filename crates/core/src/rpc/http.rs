use super::RpcHandler;

use std::sync::Arc;

use axum::extract::Query;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_runtime::Pool;
use proven_sessions::SessionManagement;
use proven_sql::{SqlStore2, SqlStore3};
use proven_store::{Store2, Store3};
use serde::Deserialize;
use tracing::error;

#[derive(Debug, Deserialize)]
struct QueryParams {
    session: String,
}

pub fn create_rpc_router<AM, SM, AS, PS, NS, ASS, PSS, NSS>(
    application_manager: AM,
    session_manager: SM,
    runtime_pool: Arc<Pool<AS, PS, NS, ASS, PSS, NSS>>,
) -> Router
where
    AM: ApplicationManagement,
    SM: SessionManagement,
    AS: Store2,
    PS: Store3,
    NS: Store3,
    ASS: SqlStore2,
    PSS: SqlStore3,
    NSS: SqlStore3,
{
    Router::new().route(
        "/rpc",
        post(|query: Query<QueryParams>, body: Bytes| async move {
            match session_manager
                .get_session("TODO_APPLICATION_ID".to_string(), query.session.clone())
                .await
            {
                Ok(Some(session)) => {
                    match RpcHandler::new(application_manager, session, runtime_pool) {
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
                                    .body(format!("Error: {e:?}").into())
                                    .unwrap()
                            }
                        },
                        Err(e) => {
                            error!("Error: {:?}", e);
                            Response::builder()
                                .status(400)
                                .body(format!("Error: {e:?}").into())
                                .unwrap()
                        }
                    }
                }
                _ => Response::builder()
                    .status(400)
                    .body("Session not found".into())
                    .unwrap(),
            }
        }),
    )
}
