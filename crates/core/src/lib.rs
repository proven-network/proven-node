//! Core logic for the Proven node and the entrypoint for all user
//! interactions.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

mod application_http;
mod error;
mod rpc;
mod sessions;

use application_http::{execute_handler, ApplicationHttpContext};
pub use error::{Error, Result};
use proven_code_package::{CodePackage, ModuleSpecifier};
use sessions::create_session_router;

use std::collections::HashSet;

use axum::response::Response;
use axum::routing::{any, delete, get, patch, post, put};
use axum::Router;
use proven_applications::ApplicationManagement;
use proven_http::HttpServer;
use proven_runtime::{ModuleLoader, ModuleOptions, RuntimePoolManagement};
use proven_sessions::SessionManagement;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

/// Options for creating a new core.
pub struct CoreOptions<AM, RM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    /// The application manager.
    pub application_manager: AM,

    /// The primary hostnames for RPC, WS, etc. Everything not matching is assumed to be an application domain.
    pub primary_hostnames: HashSet<String>,

    /// The runtime pool manager.
    pub runtime_pool_manager: RM,

    /// The session manager.
    pub session_manager: SM,
}

/// Core logic for handling user interactions.
pub struct Core<AM, RM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    application_manager: AM,
    primary_hostnames: HashSet<String>,
    runtime_pool_manager: RM,
    session_manager: SM,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<AM, RM, SM> Core<AM, RM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: SessionManagement,
{
    /// Create new core.
    pub fn new(
        CoreOptions {
            application_manager,
            primary_hostnames,
            runtime_pool_manager,
            session_manager,
        }: CoreOptions<AM, RM, SM>,
    ) -> Self {
        Self {
            application_manager,
            primary_hostnames,
            runtime_pool_manager,
            session_manager,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Start the core.
    ///
    /// # Errors
    ///
    /// This function will return an error if the core has already been started or if the HTTP server fails to start.
    #[allow(clippy::missing_panics_doc)] // TODO: Remove with test code
    pub async fn start<HS>(&self, http_server: HS) -> Result<JoinHandle<Result<()>>>
    where
        HS: HttpServer,
    {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let session_router = create_session_router(self.session_manager.clone());
        let http_rpc_router = rpc::http::create_rpc_router(
            self.application_manager.clone(),
            self.runtime_pool_manager.clone(),
            self.session_manager.clone(),
        );
        let websocket_router = rpc::ws::create_rpc_router(
            self.application_manager.clone(),
            self.runtime_pool_manager.clone(),
            self.session_manager.clone(),
        );

        let redirect_response = Response::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .map_err(Error::Http)?;

        let primary_router = Router::new()
            .route("/", get(|| async { redirect_response }))
            .merge(session_router)
            .merge(http_rpc_router)
            .merge(websocket_router)
            .fallback(any(|| async { "404" }))
            .layer(CorsLayer::very_permissive());

        let error_404_router = Router::new()
            .fallback(any(|| async { "404" }))
            .layer(CorsLayer::very_permissive());

        // Add a test http endpoint
        let code_package = CodePackage::from_str(
            r#"
            import { runOnHttp } from "@proven-network/handler";

            export const root = runOnHttp({ path: "/" }, (request) => {
                    return `Hello ${request.queryVariables.name || 'World'} from runtime!`;
                }
            );

            export const another = runOnHttp({ path: "/another" }, (request) => {
                    return `Hello from another endpoint!`;
                }
            );
        "#,
        )?;

        let module_specifier = ModuleSpecifier::parse("file:///main.ts").unwrap();

        let test_router = self
            .create_application_http_router(code_package, module_specifier)
            .await?;

        http_server
            .set_router_for_hostname("applications.proven.local".to_string(), test_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        let primary_hostnames = self.primary_hostnames.clone();
        let shutdown_token = self.shutdown_token.clone();
        let handle = self.task_tracker.spawn(async move {
            let https_handle = http_server
                .start(primary_hostnames, primary_router, error_404_router)
                .await
                .map_err(|e| Error::HttpServer(e.to_string()))?;

            tokio::select! {
                () = shutdown_token.cancelled() => {
                    info!("shutdown command received");
                    http_server.shutdown().await;

                    Ok(())
                }
                _ = https_handle => {
                    error!("https server stopped unexpectedly");

                    Err(Error::HttpServer("https server stopped unexpectedly".to_string()))
                }
            }
        });

        self.task_tracker.close();

        Ok(handle)
    }

    /// Shutdown the core.
    pub async fn shutdown(&self) {
        info!("core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("core shutdown");
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
    ) -> Result<Router> {
        let module_options =
            ModuleOptions::from_code_package(&code_package, &module_specifier).await?;

        // TODO: Use application-defined CORS settings rather than very_permissive
        let mut router = Router::new().layer(CorsLayer::very_permissive());

        for endpoint in module_options.http_endpoints {
            let ctx = ApplicationHttpContext {
                application_id: "TODO".to_string(),
                handler_specifier: endpoint.handler_specifier.clone(),
                module_loader: ModuleLoader::new(code_package.clone()),
                requires_session: false, // TODO: Make this configurable
                runtime_pool_manager: self.runtime_pool_manager.clone(),
                session_manager: self.session_manager.clone(),
            };

            let method_router = match endpoint.method.as_deref() {
                Some("GET") => get(execute_handler),
                Some("POST") => post(execute_handler),
                Some("PUT") => put(execute_handler),
                Some("DELETE") => delete(execute_handler),
                Some("PATCH") => patch(execute_handler),
                _ => any(execute_handler),
            };

            router = router.route(&endpoint.path, method_router.with_state(ctx));
        }

        Ok(router)
    }
}
