use axum::body::Body;
use axum::http::{Method, Uri};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_code_package::{CodePackage, ModuleSpecifier};
use proven_runtime::{ExecutionRequest, ModuleLoader, PoolRuntimeOptions, RuntimePoolManagement};
use proven_sessions::SessionManagement;

pub fn create_application_http_router<AM, RM, SM>(
    _application_manager: AM,
    runtime_pool_manager: RM,
    _session_manager: SM,
) -> Router
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    Router::new().fallback(any(|method: Method, uri: Uri, body: Bytes| async move {
        let path = uri.path();
        let query = uri.query();

        let body = if body.is_empty() { None } else { Some(body) };

        let execution_request = ExecutionRequest::Http {
            body,
            dapp_definition_address: "dapp_definition_address".to_string(),
            method,
            path: path.to_string(),
            query: query.map(String::from),
        };

        let code_package = CodePackage::from_str(
            r#"
            import { runOnHttp } from "@proven-network/handler";

            export const test = runOnHttp({ path: "/" }, (request) => {
                    return `Hello ${request.queryVariables.name || 'World'} from runtime!`;
                }
            );
        "#,
        )
        .unwrap();

        let result = runtime_pool_manager
            .execute(
                PoolRuntimeOptions {
                    handler_name: Some("test".to_string()),
                    module_loader: ModuleLoader::new(code_package),
                    module_specifier: ModuleSpecifier::parse("file:///main.ts").unwrap(),
                },
                execution_request,
            )
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
    }))
}
