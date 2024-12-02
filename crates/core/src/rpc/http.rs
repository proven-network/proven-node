use super::RpcHandler;

use axum::extract::Query;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use serde::Deserialize;
use tracing::error;

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
        "/rpc",
        post(|query: Query<QueryParams>, body: Bytes| async move {
            match session_manager
                .get_session("TODO_APPLICATION_ID".to_string(), query.session.clone())
                .await
            {
                Ok(Some(session)) => {
                    match RpcHandler::new(application_manager, runtime_pool_manager, session) {
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
