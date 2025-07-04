//! Core logic for the Proven node and the entrypoint for all user
//! interactions.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

mod error;
mod handlers;
mod rpc;

pub use error::Error;

use handlers::{
    ApplicationHttpContext, application_http_handler, bridge_iframe_html_handler,
    bridge_iframe_js_handler, broker_worker_js_handler, connect_iframe_html_handler,
    connect_iframe_js_handler, create_management_session_handler, create_session_handler,
    http_rpc_handler, management_http_rpc_handler, management_ws_rpc_handler,
    nats_cluster_endpoint_handler, register_iframe_html_handler, register_iframe_js_handler,
    rpc_iframe_html_handler, rpc_iframe_js_handler, rpc_worker_js_handler, sdk_js_handler,
    webauthn_authentication_finish_handler, webauthn_authentication_start_handler,
    webauthn_registration_finish_handler, webauthn_registration_start_handler, whoami_handler,
    ws_rpc_handler,
};

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use axum::response::Response as AxumResponse;
use axum::routing::{any, delete, get, patch, post, put};
use axum::{Json, Router};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_code_package::{CodePackage, ModuleSpecifier};
use proven_consensus::Consensus;
use proven_governance::Governance;
use proven_http::HttpServer;
use proven_identity::IdentityManagement;
use proven_network::{NATS_CLUSTER_ENDPOINT_API_PATH, ProvenNetwork};
use proven_passkeys::PasskeyManagement;
use proven_runtime::{HttpEndpoint, ModuleLoader, ModuleOptions, RuntimePoolManagement};
use proven_sessions::SessionManagement;
use serde_json::json;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

pub use rpc::{
    AnonymizeCommand, AnonymizeResponse, Command, CreateApplicationCommand,
    CreateApplicationResponse, IdentifyCommand, IdentifyResponse, Response, WhoAmICommand,
    WhoAmIResponse,
};

#[derive(Clone)]
pub(crate) struct FullContext<AM, RM, IM, PM, SM, A, G>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
{
    pub application_manager: AM,
    pub identity_manager: IM,
    pub passkey_manager: PM,
    pub sessions_manager: SM,
    pub network: ProvenNetwork<G, A>,
    pub runtime_pool_manager: RM,
}

#[derive(Clone)]
pub(crate) struct LightContext<A, G>
where
    A: Attestor,
    G: Governance,
{
    pub network: ProvenNetwork<G, A>,
}

/// Core mode state
#[derive(Debug, Clone)]
pub enum CoreMode {
    /// Bootstrapping mode with minimal functionality
    Bootstrapping,
    /// Bootstrapped mode with all features enabled
    Bootstrapped,
}

/// Additional managers needed to bootstrap from Bootstrapping to Bootstrapped
pub struct BootstrapUpgrade<AM, RM, IM, PM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
{
    /// Application manager for handling application lifecycle
    pub application_manager: AM,
    /// Runtime pool manager for JavaScript runtime management
    pub runtime_pool_manager: RM,
    /// Identity manager for user identity operations
    pub identity_manager: IM,
    /// Passkey manager for `WebAuthn` operations
    pub passkey_manager: PM,
    /// Session manager for user session handling
    pub sessions_manager: SM,
}

/// Options for creating a new unified core (starts in Bootstrapping mode)
pub struct CoreOptions<A, G, HS>
where
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    /// The consensus system
    pub consensus: Arc<Consensus<G, A>>,

    /// The HTTP server
    pub http_server: HS,

    /// The network for peer discovery
    pub network: ProvenNetwork<G, A>,
}

/// Internal state for the bootstrapped context managers
#[allow(clippy::struct_field_names)]
struct BootstrappedState<AM, RM, IM, PM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
{
    application_manager: AM,
    runtime_pool_manager: RM,
    identity_manager: IM,
    passkey_manager: PM,
    sessions_manager: SM,
}

/// Unified core that can operate in Bootstrapping or Bootstrapped mode
pub struct Core<A, G, HS>
where
    A: Attestor,
    G: Governance,
    HS: HttpServer,
{
    http_server: HS,
    network: ProvenNetwork<G, A>,
    consensus: Arc<Consensus<G, A>>,
    mode: Arc<RwLock<CoreMode>>,
    bootstrapped_state: Arc<RwLock<Option<Box<dyn std::any::Any + Send + Sync>>>>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
    router_installed: Arc<RwLock<bool>>,
}

impl<A, G, HS> Core<A, G, HS>
where
    A: Attestor + Clone + 'static,
    G: Governance + Clone + 'static,
    HS: HttpServer,
{
    /// Create new unified core in Bootstrapping mode
    pub fn new(
        CoreOptions {
            http_server,
            network,
            consensus,
        }: CoreOptions<A, G, HS>,
    ) -> Self {
        Self {
            http_server,
            network,
            consensus,
            mode: Arc::new(RwLock::new(CoreMode::Bootstrapping)),
            bootstrapped_state: Arc::new(RwLock::new(None)),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            router_installed: Arc::new(RwLock::new(false)),
        }
    }

    /// Get current core mode
    pub async fn mode(&self) -> CoreMode {
        self.mode.read().await.clone()
    }

    /// Get a reference to the consensus system
    pub const fn consensus(&self) -> &Arc<Consensus<G, A>> {
        &self.consensus
    }

    /// Install consensus routes using the transport's router creation method
    async fn install_consensus_routes(&self) -> Result<(), Error> {
        // Use the transport's create_router method
        // For TCP transport, this will panic which is expected behavior
        // For WebSocket transport, this will return the appropriate router
        let consensus_router = self
            .consensus
            .create_router()
            .map_err(|e| Error::Consensus(e.to_string()))?;

        // Mount consensus routes under the main domain with a /ws prefix
        let fqdn = self
            .network
            .fqdn()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        // Get the current router and merge consensus routes
        let current_router = Router::new().merge(consensus_router);

        self.http_server
            .set_router_for_hostname(fqdn, current_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        info!("Mounted consensus routes under /consensus/ws");
        Ok(())
    }

    /// Bootstrap from Bootstrapping to Bootstrapped mode by adding the required managers.
    ///
    /// This method can be called while the HTTP server is already running (after `start()` has been called).
    /// It will dynamically add the bootstrapped routes (sessions, RPC, `WebAuthn`, etc.) to the running server
    /// without requiring a restart. This allows seamless upgrades from basic to full functionality.
    ///
    /// # Arguments
    ///
    /// * `upgrade` - The bootstrap upgrade configuration containing the components to initialize
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful bootstrap, or an error if any step fails.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Any of the bootstrap steps fail to complete
    /// - Component initialization encounters issues
    /// - System configuration is invalid
    /// - Required dependencies are not available
    pub async fn bootstrap<AM, RM, IM, PM, SM>(
        &self,
        upgrade: BootstrapUpgrade<AM, RM, IM, PM, SM>,
    ) -> Result<(), Error>
    where
        AM: ApplicationManagement + Clone + 'static,
        RM: RuntimePoolManagement + Clone + 'static,
        IM: IdentityManagement + Clone + 'static,
        PM: PasskeyManagement + Clone + 'static,
        SM: SessionManagement + Clone + 'static,
    {
        let mut mode = self.mode.write().await;
        if matches!(*mode, CoreMode::Bootstrapped) {
            return Err(Error::AlreadyStarted); // Already in bootstrapped mode
        }

        // Store the bootstrapped state
        let bootstrapped_state = BootstrappedState {
            application_manager: upgrade.application_manager,
            runtime_pool_manager: upgrade.runtime_pool_manager,
            identity_manager: upgrade.identity_manager,
            passkey_manager: upgrade.passkey_manager,
            sessions_manager: upgrade.sessions_manager,
        };

        *self.bootstrapped_state.write().await = Some(Box::new(bootstrapped_state));
        *mode = CoreMode::Bootstrapped;

        // Add bootstrapped routes if we're already running
        if *self.router_installed.read().await {
            self.install_bootstrapped_routes::<AM, RM, IM, PM, SM>()
                .await?;
        }

        info!("Core bootstrapped from Bootstrapping to Bootstrapped mode");
        Ok(())
    }

    /// Reset from Bootstrapped to Bootstrapping mode (removes bootstrapped functionality)
    ///
    /// This method removes all dynamic routes and components, returning the system
    /// to its initial state where only basic routes are available. This is useful
    /// for testing scenarios or when a clean restart is needed.
    ///
    /// # Errors
    ///
    /// This function will return an error if the system fails to reset properly.
    pub async fn reset_to_bootstrapping(&self) -> Result<(), Error> {
        let mut mode = self.mode.write().await;
        if matches!(*mode, CoreMode::Bootstrapping) {
            return Err(Error::AlreadyStarted); // Already in bootstrapping mode
        }

        // Clear the bootstrapped state
        *self.bootstrapped_state.write().await = None;
        *mode = CoreMode::Bootstrapping;

        // Remove bootstrapped routes and reinstall only bootstrapping routes
        if *self.router_installed.read().await {
            self.install_bootstrapping_routes().await?;
        }

        info!("Core reset from Bootstrapped to Bootstrapping mode");
        Ok(())
    }

    /// Install bootstrapping mode routes
    async fn install_bootstrapping_routes(&self) -> Result<(), Error> {
        let redirect_response = AxumResponse::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .map_err(Error::Http)?;

        let light_ctx = LightContext {
            network: self.network.clone(),
        };

        let light_router = Router::new()
            .route("/", get(|| async { redirect_response }))
            .route(
                NATS_CLUSTER_ENDPOINT_API_PATH,
                get(nats_cluster_endpoint_handler).with_state(light_ctx),
            )
            .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
            .layer(CorsLayer::very_permissive());

        let fqdn = self
            .network
            .fqdn()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        self.http_server
            .set_router_for_hostname(fqdn, light_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        self.install_webauthn_routes().await?;

        Ok(())
    }

    /// Install bootstrapped mode routes (includes bootstrapping routes plus bootstrapped-specific ones)
    #[allow(clippy::too_many_lines)]
    async fn install_bootstrapped_routes<AM, RM, IM, PM, SM>(&self) -> Result<(), Error>
    where
        AM: ApplicationManagement + Clone + 'static,
        RM: RuntimePoolManagement + Clone + 'static,
        IM: IdentityManagement + Clone + 'static,
        PM: PasskeyManagement + Clone + 'static,
        SM: SessionManagement + Clone + 'static,
    {
        // Get the bootstrapped state
        let bootstrapped_state_guard = self.bootstrapped_state.read().await;
        let bootstrapped_state = bootstrapped_state_guard
            .as_ref()
            .ok_or_else(|| Error::HttpServer("No bootstrapped state available".to_string()))?
            .downcast_ref::<BootstrappedState<AM, RM, IM, PM, SM>>()
            .ok_or_else(|| Error::HttpServer("Invalid bootstrapped state type".to_string()))?;

        let redirect_response = AxumResponse::builder()
            .status(301)
            .header("Location", "https://proven.network")
            .body(String::new())
            .map_err(Error::Http)?;

        let full_ctx = FullContext {
            application_manager: bootstrapped_state.application_manager.clone(),
            network: self.network.clone(),
            runtime_pool_manager: bootstrapped_state.runtime_pool_manager.clone(),
            identity_manager: bootstrapped_state.identity_manager.clone(),
            passkey_manager: bootstrapped_state.passkey_manager.clone(),
            sessions_manager: bootstrapped_state.sessions_manager.clone(),
        };

        let light_ctx = LightContext {
            network: self.network.clone(),
        };

        let full_router = Router::new()
            .route("/", get(|| async { redirect_response }))
            // ** Sessions **
            .route(
                "/app/{application_id}/auth/create_session",
                post(create_session_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/auth/create_management_session",
                post(create_management_session_handler).with_state(full_ctx.clone()),
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
            .route(
                "/management/rpc/http",
                post(management_http_rpc_handler).with_state(full_ctx.clone()),
            )
            .route(
                "/management/rpc/ws",
                get(management_ws_rpc_handler).with_state(full_ctx.clone()),
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
                get(nats_cluster_endpoint_handler).with_state(light_ctx),
            )
            // ** Static files **
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

        let fqdn = self
            .network
            .fqdn()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        self.http_server
            .set_router_for_hostname(fqdn, full_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        // Install WebAuthn routes
        self.install_webauthn_routes().await?;

        // Install application test router (only in bootstrapped mode)
        self.install_application_test_router(&full_ctx).await?;

        Ok(())
    }

    /// Install WebAuthn-related routes (common to both modes)
    async fn install_webauthn_routes(&self) -> Result<(), Error> {
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

        Ok(())
    }

    /// Install application test router (only available in bootstrapped mode)
    async fn install_application_test_router<AM, RM, IM, PM, SM>(
        &self,
        bootstrapped_ctx: &FullContext<AM, RM, IM, PM, SM, A, G>,
    ) -> Result<(), Error>
    where
        AM: ApplicationManagement + Clone,
        RM: RuntimePoolManagement + Clone,
        IM: IdentityManagement + Clone,
        PM: PasskeyManagement + Clone,
        SM: SessionManagement + Clone,
    {
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
        )
        .map_err(|e| Error::Runtime(e.to_string()))?;

        let module_specifier =
            ModuleSpecifier::parse("file:///main.ts").map_err(|e| Error::Runtime(e.to_string()))?;

        let test_router = self
            .create_application_http_router(
                code_package,
                module_specifier,
                &bootstrapped_ctx.application_manager,
                &bootstrapped_ctx.runtime_pool_manager,
                &bootstrapped_ctx.identity_manager,
                &bootstrapped_ctx.sessions_manager,
            )
            .await?;

        self.http_server
            .set_router_for_hostname("applications.proven.local".to_string(), test_router)
            .await
            .map_err(|e| Error::HttpServer(e.to_string()))?;

        Ok(())
    }

    /// Creates a router for handling HTTP requests to application endpoints.
    async fn create_application_http_router<AM, RM, IM, SM>(
        &self,
        code_package: CodePackage,
        module_specifier: ModuleSpecifier,
        _application_manager: &AM,
        runtime_pool_manager: &RM,
        identity_manager: &IM,
        sessions_manager: &SM,
    ) -> Result<Router, Error>
    where
        AM: ApplicationManagement + Clone,
        RM: RuntimePoolManagement + Clone,
        IM: IdentityManagement + Clone,
        SM: SessionManagement + Clone,
    {
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
                attestor: self.network.attestor().clone(),
                handler_specifier: endpoint.handler_specifier.clone(),
                module_loader: ModuleLoader::new(code_package.clone()),
                requires_session: false, // TODO: Make this configurable
                runtime_pool_manager: runtime_pool_manager.clone(),
                _identity_manager: identity_manager.clone(),
                sessions_manager: sessions_manager.clone(),
            };

            let method_router = match endpoint.method.as_deref() {
                Some("GET") => get(application_http_handler),
                Some("POST") => post(application_http_handler),
                Some("PUT") => put(application_http_handler),
                Some("DELETE") => delete(application_http_handler),
                Some("PATCH") => patch(application_http_handler),
                _ => any(application_http_handler),
            };

            let axum_friendly_path = Self::convert_path_use_axum_capture_groups(&endpoint.path);
            router = router.route(&axum_friendly_path, method_router.with_state(ctx));
        }

        Ok(router)
    }

    /// Switches the path parameters from the colon-prefixed style, used in `Runtime`, to Axum capture groups.
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
impl<A, G, HS> Bootable for Core<A, G, HS>
where
    A: Attestor + Clone + 'static,
    G: Governance + Clone + 'static,
    HS: HttpServer,
{
    fn bootable_name(&self) -> &'static str {
        "core"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        // Install routes based on current mode
        match *self.mode.read().await {
            CoreMode::Bootstrapping => {
                self.install_bootstrapping_routes().await?;
            }
            CoreMode::Bootstrapped => {
                // We need to handle this case differently since we don't know the types
                // This should not happen in normal flow - bootstrap should be called before start
                warn!(
                    "Starting in Bootstrapped mode without proper type information, falling back to Bootstrapping mode"
                );
                self.install_bootstrapping_routes().await?;
            }
        }

        // Always install consensus routes regardless of mode (if needed by transport)
        self.install_consensus_routes().await?;

        *self.router_installed.write().await = true;

        // Start HTTP server first so WebSocket endpoints are available
        if let Err(e) = self.http_server.start().await {
            error!("http server failed to start: {e}");
            return Err(Box::new(Error::HttpServer(e.to_string())));
        }

        // Now start consensus system (it can connect to peers' WebSocket endpoints)
        if let Err(e) = self.consensus.start().await {
            error!("consensus system failed to start: {e}");
            return Err(Box::new(e));
        }

        let http_server = self.http_server.clone();
        let consensus = Arc::clone(&self.consensus);
        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            tokio::select! {
                () = shutdown_token.cancelled() => {
                    info!("shutdown command received");
                    let _ = http_server.shutdown().await;
                    let _ = consensus.shutdown().await;
                    Ok(())
                }
                () = http_server.wait() => {
                    error!("https server stopped unexpectedly");
                    let _ = consensus.shutdown().await;
                    Err(Error::HttpServer("https server stopped unexpectedly".to_string()))
                }
            }
        });

        self.task_tracker.close();
        info!("Unified core started in {:?} mode", *self.mode.read().await);

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("unified core shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("unified core shutdown");
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
