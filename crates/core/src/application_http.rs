use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, Uri};
use axum::response::IntoResponse;
use axum::response::Response;
use bytes::Bytes;
use proven_runtime::{ExecutionRequest, HandlerSpecifier, ModuleLoader, RuntimePoolManagement};

#[derive(Clone)]
pub(crate) struct ApplicationHttpContext<RM>
where
    RM: RuntimePoolManagement,
{
    pub application_id: String,
    pub module_loader: ModuleLoader,
    pub runtime_pool_manager: RM,
    pub handler_specifier: HandlerSpecifier,
}

pub(crate) async fn execute_handler<RM>(
    State(ApplicationHttpContext {
        application_id,
        handler_specifier,
        module_loader,
        runtime_pool_manager,
    }): State<ApplicationHttpContext<RM>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse
where
    RM: RuntimePoolManagement,
{
    let path = uri.path();
    let query = uri.query();

    let body = if body.is_empty() { None } else { Some(body) };

    let execution_request = ExecutionRequest::Http {
        body,
        dapp_definition_address: application_id,
        handler_specifier,
        method,
        path: path.to_string(),
        query: query.map(String::from),
    };

    let result = runtime_pool_manager
        .execute(module_loader, execution_request)
        .await;

    if let Err(err) = result {
        return Response::builder()
            .status(500)
            .body(Body::from(format!("Error: {err:?}")))
            .unwrap();
    }

    let execution_result = result.unwrap();

    let json_output = serde_json::to_string(&execution_result.output).unwrap();

    Response::builder()
        .status(200)
        .body(Body::from(json_output))
        .unwrap()
}
