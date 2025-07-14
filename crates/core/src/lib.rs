//! Core logic for the Proven node and the entrypoint for all user
//! interactions.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

mod application;
mod error;
mod handlers;
mod router;
mod rpc;
mod state;
mod utils;

pub use error::Error;
use proven_attestation::Attestor;
pub use router::routes;
pub use state::{BootstrapUpgrade, CoreMode, CoreOptions, FullContext};

use application::ApplicationRouter;
// Handlers are imported by the router module
use router::{
    BootstrappedRouterBuilder, RouterBuilder, RouterInstaller, create_bootstrapped_router_builder,
};
use state::BootstrappedState;

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use proven_applications::ApplicationManagement;
use proven_bootable::Bootable;
use proven_governance::Governance;
use proven_http::HttpServer;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

pub use rpc::{
    AddAllowedOriginCommand, AddAllowedOriginResponse, AnonymizeCommand, AnonymizeResponse,
    Command, CreateApplicationCommand, CreateApplicationResponse, IdentifyCommand,
    IdentifyResponse, ListApplicationsByOwnerCommand, ListApplicationsByOwnerResponse, Response,
    WhoAmICommand, WhoAmIResponse,
};

/// Unified core that can operate in Bootstrapping or Bootstrapped mode
pub struct Core<A, G, H>
where
    A: Attestor,
    G: Governance,
    H: HttpServer,
{
    application_test_router: Arc<RwLock<Option<Router>>>,
    attestor: A,
    bootstrapped_router_builder: Arc<RwLock<Option<BootstrappedRouterBuilder>>>,
    bootstrapped_state: Arc<RwLock<Option<Box<dyn std::any::Any + Send + Sync>>>>,
    engine_router: Router,
    governance: G,
    http_server: H,
    mode: Arc<RwLock<CoreMode>>,
    origin: String,
    router_installed: Arc<RwLock<bool>>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl<A, G, H> Core<A, G, H>
where
    A: Attestor,
    G: Governance,
    H: HttpServer,
{
    /// Create new unified core in Bootstrapping mode
    pub fn new(
        CoreOptions {
            attestor,
            governance,
            engine_router,
            http_server,
            origin,
        }: CoreOptions<A, G, H>,
    ) -> Self {
        Self {
            application_test_router: Arc::new(RwLock::new(None)),
            attestor,
            bootstrapped_state: Arc::new(RwLock::new(None)),
            bootstrapped_router_builder: Arc::new(RwLock::new(None)),
            engine_router,
            governance,
            http_server,
            mode: Arc::new(RwLock::new(CoreMode::Bootstrapping)),
            origin,
            router_installed: Arc::new(RwLock::new(false)),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Get the current mode
    pub async fn mode(&self) -> CoreMode {
        self.mode.read().await.clone()
    }

    /// Bootstrap from Bootstrapping to Bootstrapped mode
    ///
    /// # Errors
    ///
    /// Returns an error if the core is already in bootstrapped mode or if
    /// router installation fails.
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
            return Err(Error::AlreadyStarted);
        }

        // Create the bootstrapped state
        let bootstrapped_state = BootstrappedState::new(
            upgrade.application_manager,
            self.attestor.clone(),
            self.governance.clone(),
            upgrade.identity_manager,
            self.origin.clone(),
            upgrade.passkey_manager.clone(),
            upgrade.runtime_pool_manager,
            upgrade.sessions_manager,
        );

        // Create the full context
        let full_ctx = bootstrapped_state.to_full_context();

        // Create and store the bootstrapped router builder
        let router_builder = create_bootstrapped_router_builder(full_ctx.clone());
        *self.bootstrapped_router_builder.write().await = Some(router_builder);

        // Create and store the application test router
        let test_router = ApplicationRouter::create_application_test_router(&full_ctx).await?;
        *self.application_test_router.write().await = Some(test_router);

        // Update the mode
        *mode = CoreMode::Bootstrapped;

        // Store the bootstrapped state
        *self.bootstrapped_state.write().await = Some(Box::new(bootstrapped_state));

        // Release the mode write lock before calling build_and_install_main_router
        // to avoid deadlock (build_and_install_main_router needs to read mode)
        drop(mode);

        // Rebuild and install the router to include the new bootstrapped routes
        if *self.router_installed.read().await {
            self.build_and_install_main_router().await?;
            info!("Router reinstalled with bootstrapped routes");
        }

        Ok(())
    }

    /// Reset from Bootstrapped to Bootstrapping mode
    ///
    /// # Errors
    ///
    /// Returns an error if the core is already in bootstrapping mode or if
    /// router installation fails.
    pub async fn reset_to_bootstrapping(&self) -> Result<(), Error> {
        let mut mode = self.mode.write().await;
        if matches!(*mode, CoreMode::Bootstrapping) {
            return Err(Error::AlreadyStarted); // Already in bootstrapping mode
        }

        // Clear the bootstrapped state
        *self.bootstrapped_state.write().await = None;
        *self.bootstrapped_router_builder.write().await = None;
        *self.application_test_router.write().await = None;
        *mode = CoreMode::Bootstrapping;

        // Rebuild and install the complete router if we're already running
        if *self.router_installed.read().await {
            self.build_and_install_main_router().await?;
        }

        info!("Core reset from Bootstrapped to Bootstrapping mode");
        Ok(())
    }

    /// Build and install the complete router for the main hostname atomically
    async fn build_and_install_main_router(&self) -> Result<(), Error> {
        let router = RouterBuilder::create_base_router(self.engine_router.clone());

        // Add bootstrapped routes if in bootstrapped mode
        let router = if let CoreMode::Bootstrapped = *self.mode.read().await {
            self.add_bootstrapped_routes_to_router(router)
        } else {
            router
        };

        // Finalize the router
        let router = RouterBuilder::finalize_router(router);

        // Set the complete router atomically
        let fqdn = self.origin.clone();

        RouterInstaller::install_router(&self.http_server, fqdn, router).await?;

        Ok(())
    }

    /// Add bootstrapped routes to the provided router using the stored callback
    fn add_bootstrapped_routes_to_router(&self, router: Router) -> Router {
        if let Ok(guard) = self.bootstrapped_router_builder.try_read() {
            if let Some(builder) = guard.as_ref() {
                builder(router)
            } else {
                router
            }
        } else {
            router
        }
    }

    /// Install the application test router if it's available
    async fn install_application_test_router_if_available(&self) -> Result<(), Error> {
        if let Some(test_router) = self.application_test_router.read().await.clone() {
            RouterInstaller::install_router(
                &self.http_server,
                "applications.proven.local".to_string(),
                test_router,
            )
            .await?;
            info!("Application test router installed");
        }
        Ok(())
    }

    /// Install WebAuthn-related routes
    async fn install_webauthn_routes(&self) -> Result<(), Error> {
        let alternates_auth_gateways = self
            .governance
            .get_alternates_auth_gateways()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        let webauthn_router = RouterBuilder::create_webauthn_router(alternates_auth_gateways);

        let primary_auth_gateway = self
            .governance
            .get_primary_auth_gateway()
            .await
            .map_err(|e| Error::Network(e.to_string()))?;

        RouterInstaller::install_router(&self.http_server, primary_auth_gateway, webauthn_router)
            .await?;

        info!("WebAuthn routes installed");
        Ok(())
    }
}

#[async_trait]
impl<A, G, H> Bootable for Core<A, G, H>
where
    A: Attestor,
    G: Governance,
    H: HttpServer,
{
    fn bootable_name(&self) -> &'static str {
        "core"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        // Install the unified router for the main hostname
        self.build_and_install_main_router().await?;

        // Install WebAuthn routes (separate hostname)
        self.install_webauthn_routes().await?;

        // Install application test router if in bootstrapped mode (separate hostname)
        if let CoreMode::Bootstrapped = *self.mode.read().await {
            self.install_application_test_router_if_available().await?;
        }

        *self.router_installed.write().await = true;

        // Start HTTP server first so WebSocket endpoints are available
        if let Err(e) = self.http_server.start().await {
            error!("http server failed to start: {e}");
            return Err(Box::new(Error::HttpServer(e.to_string())));
        }

        let http_server = self.http_server.clone();
        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
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
