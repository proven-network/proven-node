use crate::error::Error;
use crate::handlers::{
    ApplicationHttpContext, application_http_handler, bridge_iframe_html_handler,
    bridge_iframe_js_handler, broker_worker_js_handler, connect_iframe_html_handler,
    connect_iframe_js_handler, create_session_handler, http_rpc_handler,
    nats_cluster_endpoint_handler, register_iframe_html_handler, register_iframe_js_handler,
    rpc_iframe_html_handler, rpc_iframe_js_handler, rpc_worker_js_handler, sdk_js_handler,
    webauthn_authentication_finish_handler, webauthn_authentication_start_handler,
    webauthn_registration_finish_handler, webauthn_registration_start_handler, whoami_handler,
    ws_rpc_handler,
};
use crate::{FullContext, LightContext};

use std::collections::HashSet;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{any, delete, get, patch, post, put};
use axum::{Json, Router};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_code_package::{CodePackage, ModuleSpecifier};
use proven_governance::Governance;
use proven_http::HttpServer;
use proven_identity::IdentityManagement;
use proven_network::{NATS_CLUSTER_ENDPOINT_API_PATH, ProvenNetwork};
use proven_passkeys::PasskeyManagement;
use proven_runtime::{HttpEndpoint, ModuleLoader, ModuleOptions, RuntimePoolManagement};
use proven_sessions::SessionManagement;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
use tracing::{error, info};
use uuid::Uuid;

/// Options for creating a new core.
pub struct CoreOptions<AM, RM, IM, PM, SM, A, G, HS>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    /// The application manager.
    pub application_manager: AM,

    /// The remote attestation attestor.
    pub attestor: A,

    /// The HTTP server.
    pub http_server: HS,

    /// The identity manager.
    pub identity_manager: IM,

    /// The passkey manager.
    pub passkey_manager: PM,

    /// The sessions manager.
    pub sessions_manager: SM,

    /// The network for peer discovery.
    pub network: ProvenNetwork<G, A>,

    /// The runtime pool manager.
    pub runtime_pool_manager: RM,
}

/// Core logic for handling user interactions.
#[derive(Clone)]
pub struct Core<AM, RM, IM, PM, SM, A, G, HS>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    application_manager: AM,
    attestor: A,
    http_server: HS,
    identity_manager: IM,
    passkey_manager: PM,
    sessions_manager: SM,
    network: ProvenNetwork<G, A>,
    runtime_pool_manager: RM,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<AM, RM, IM, PM, SM, A, G, HS> Core<AM, RM, IM, PM, SM, A, G, HS>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    /// Create new core.
    pub fn new(
        CoreOptions {
            application_manager,
            attestor,
            http_server,
            network,
            runtime_pool_manager,
            identity_manager,
            passkey_manager,
            sessions_manager,
        }: CoreOptions<AM, RM, IM, PM, SM, A, G, HS>,
    ) -> Self {
        Self {
            application_manager,
            attestor,
            http_server,
            network,
            runtime_pool_manager,
            identity_manager,
            passkey_manager,
            sessions_manager,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Creates a router for handling HTTP requests to application endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error if the module options cannot be created from the code package,
    /// or if there are issues setting up the router with the provided endpoints.
    pub async fn create_application_http_router(
        &self,
        code_package: CodePackage,
        module_specifier: ModuleSpecifier,
    ) -> Result<Router, Error> {
        let module_options = ModuleOptions::from_code_package(&code_package, &module_specifier)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;

        // Validate before creating the router
        Self::ensure_no_overlapping_routes(&module_options.http_endpoints)?;

        let mut router = Router::new()
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive());

        for endpoint in module_options.http_endpoints {
            let ctx = ApplicationHttpContext {
                application_id: Uuid::max(), // TODO: Fix this
                attestor: self.attestor.clone(),
                handler_specifier: endpoint.handler_specifier.clone(),
                module_loader: ModuleLoader::new(code_package.clone()),
                requires_session: false, // TODO: Make this configurable
                runtime_pool_manager: self.runtime_pool_manager.clone(),
                _identity_manager: self.identity_manager.clone(),
                sessions_manager: self.sessions_manager.clone(),
            };

            let method_router = match endpoint.method.as_deref() {
                Some("GET") => get(application_http_handler),
                Some("POST") => post(application_http_handler),
                Some("PUT") => put(application_http_handler),
                Some("DELETE") => delete(application_http_handler),
                Some("PATCH") => patch(application_http_handler),
                _ => any(application_http_handler),
            };

            let axum_friedly_path = Self::convert_path_use_axum_capture_groups(&endpoint.path);

            router = router.route(&axum_friedly_path, method_router.with_state(ctx));
        }

        Ok(router)
    }

    /// Switches the path parameters from the colon-prefixed style, used in `Runtime`, to Axum capture groups.
    /// This is necessary because Axum uses a curly brace style for path parameters.
    fn convert_path_use_axum_capture_groups(path: &str) -> String {
        path.split('/')
            .map(|segment| {
                if let Some(without_colon) = segment.strip_prefix(':') {
                    format!("{{{without_colon}}}")
                } else {
                    segment.to_string()
                }
            })
            .collect::<Vec<String>>()
            .join("/")
    }

    /// Validates that there are no overlapping routes in the endpoint set.
    /// Without this check, it would be possible to for two endpoints to overlap in path and method.
    /// Axum panics if this occurs, so we check for it here.
    ///
    /// # Errors
    ///
    /// Returns an error if any endpoints overlap in path or method.
    fn ensure_no_overlapping_routes(endpoints: &HashSet<HttpEndpoint>) -> Result<(), Error> {
        let mut routes: Vec<(&str, &str)> = Vec::new();

        for endpoint in endpoints {
            let method = endpoint.method.as_deref().unwrap_or("*");
            let path = endpoint.path.as_str();

            for (existing_method, existing_path) in &routes {
                if (method == *existing_method || method == "*" || *existing_method == "*")
                    && normalize_path_parameters(path) == normalize_path_parameters(existing_path)
                {
                    return Err(Error::OverlappingRoutes(
                        format!("{existing_method} {existing_path}"),
                        format!("{method} {path}"),
                    ));
                }
            }

            routes.push((method, path));
        }

        Ok(())
    }
}

#[async_trait]
impl<AM, RM, IM, PM, SM, A, G, HS> Bootable for Core<AM, RM, IM, PM, SM, A, G, HS>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    fn name(&self) -> &'static str {
        "core (full)"
    }

    /// Start the core.
    ///
    /// # Errors
    ///
    /// This function will return an error if the core has already been started or if the HTTP server fails to start.
    #[allow(clippy::missing_panics_doc)] // TODO: Remove with test code
    #[allow(clippy::too_many_lines)] // TODO: Refactor this to be more readable
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        let redirect_response = Response::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .map_err(Error::Http)?;

        let full_ctx = FullContext {
            application_manager: self.application_manager.clone(),
            network: self.network.clone(),
            runtime_pool_manager: self.runtime_pool_manager.clone(),
            identity_manager: self.identity_manager.clone(),
            passkey_manager: self.passkey_manager.clone(),
            sessions_manager: self.sessions_manager.clone(),
        };

        let light_ctx = LightContext {
            network: self.network.clone(),
        };

        let primary_router = Router::new()
            .route("/", get(|| async { redirect_response }))
            // ** Sessions **
            .route(
                "/app/{application_id}/auth/create_session",
                post(create_session_handler).with_state(full_ctx.clone()),
            )
            // ** RPC **
            .route(
                "/app/{application_id}/rpc/http",
                post(http_rpc_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/app/{application_id}/rpc/ws",
                get(ws_rpc_handler).with_state(full_ctx.clone()),
            )
            // ** WebAuthn **
            .route(
                "/webauthn/register/start",
                post(webauthn_registration_start_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/webauthn/register/finish",
                post(webauthn_registration_finish_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/webauthn/auth/start",
                post(webauthn_authentication_start_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/webauthn/auth/finish",
                post(webauthn_authentication_finish_handler).with_state(full_ctx.clone()),
            )
            // ** Inter-node communication **
            .route("/whoami", get(whoami_handler).with_state(full_ctx.clone()))
            .route(
                NATS_CLUSTER_ENDPOINT_API_PATH,
                get(nats_cluster_endpoint_handler).with_state(light_ctx.clone()),
            )
            // ** Static files **
            // TODO: SDK is temporary - should be extracted to external package (not served via node)
            .route("/sdk.js", get(sdk_js_handler))
            // Iframe HTML
            .route(
                "/app/{application_id}/iframes/bridge.html",
                get(bridge_iframe_html_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/app/{application_id}/iframes/connect.html",
                get(connect_iframe_html_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/app/{application_id}/iframes/register.html",
                get(register_iframe_html_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/app/{application_id}/iframes/rpc.html",
                get(rpc_iframe_html_handler).with_state(full_ctx.clone()),
            )
            // Iframe JS
            .route(
                "/app/{application_id}/iframes/bridge.js",
                get(bridge_iframe_js_handler),
            )
            .route(
                "/app/{application_id}/iframes/connect.js",
                get(connect_iframe_js_handler),
            )
            .route(
                "/app/{application_id}/iframes/register.js",
                get(register_iframe_js_handler),
            )
            .route(
                "/app/{application_id}/iframes/rpc.js",
                get(rpc_iframe_js_handler),
            )
            // Shared workers
            .route(
                "/app/{application_id}/workers/broker-worker.js",
                get(broker_worker_js_handler),
            )
            .route(
                "/app/{application_id}/workers/rpc-worker.js",
                get(rpc_worker_js_handler),
            )
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive());

        // Add a test http endpoint
        let code_package = CodePackage::from_str(
            r#"
            import { runOnHttp } from "@proven-network/handler";

            export const root = runOnHttp({ path: "/" }, (request) => {
                    return `Hello ${request.queryParameters.name || 'World'} from runtime!`;
                }
            );

            export const another = runOnHttp({ path: "/another" }, (request) => {
                    return `Hello from another endpoint!`;
                }
            );

            export const getPost = runOnHttp({ method: "GET", path: "/post/:id" }, (request) => {
                    return `Hello from post endpoint with id ${request.pathParameters.id}!`;
                }
            );

            export const updatePost = runOnHttp({ method: "PUT", path: "/post/:id" }, (request) => {
                    return `Hello from post endpoint with id ${request.pathParameters.id}!`;
                }
            );

            export const error = runOnHttp({ path: "/error" }, (request) => {
                    throw new Error("This is an error");
                }
            );
        "#,
        )?;

        let module_specifier = ModuleSpecifier::parse("file:///main.ts").unwrap();

        let fqdn = self
            .network
            .fqdn()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        self.http_server
            .set_router_for_hostname(fqdn, primary_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        // Router for WebAuthn related origin requests
        let primary_auth_gateway = self
            .network
            .governance()
            .get_primary_auth_gateway()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let alternates_auth_gateways = self
            .network
            .governance()
            .get_alternates_auth_gateways()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let webauthn_router = Router::new().route(
            "/.well-known/webauthn",
            get(|| async move {
                Json(json!({
                    "origins": alternates_auth_gateways
                }))
            }),
        );

        self.http_server
            .set_router_for_hostname(primary_auth_gateway, webauthn_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        // Test router for application endpoints
        let test_router = self
            .create_application_http_router(code_package, module_specifier)
            .await?;

        self.http_server
            .set_router_for_hostname("applications.proven.local".to_string(), test_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        let http_server = self.http_server.clone();
        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            http_server
                .start()
                .await
                .map_err(|e| Error::HttpServer(e.to_string()))?;

            tokio::select! {
                () = shutdown_token.cancelled() => {
                    info!("shutdown command received");
                    let _ = http_server.shutdown().await;

                    Ok(())
                }
                () = http_server.wait() => {
                    error!("https server stopped unexpectedly");

                    Err(Error::HttpServer("https server stopped unexpectedly".to_string()))
                }
            }
        });

        self.task_tracker.close();

        Ok(())
    }

    /// Shutdown the core.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("core shutdown");

        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}

fn normalize_path_parameters(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.starts_with(':') {
                ":param".to_string()
            } else {
                segment.to_string()
            }
        })
        .collect::<Vec<String>>()
        .join("/")
}
