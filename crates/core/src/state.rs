//! Core state management types and transitions
//!
//! This module contains the types and logic for managing the core state transitions
//! between bootstrapping and bootstrapped modes.

use axum::Router;
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_http::HttpServer;
use proven_identity::IdentityManagement;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
use proven_topology::TopologyAdaptor;

/// Core mode state
#[derive(Debug, Clone, Copy)]
pub enum CoreMode {
    /// Bootstrapping mode with minimal functionality
    Bootstrapping,
    /// Bootstrapped mode with all features enabled
    Bootstrapped,
}

/// Options for creating a new unified core (starts in Bootstrapping mode)
pub struct CoreOptions<A, G, H>
where
    A: Attestor,
    G: TopologyAdaptor,
    H: HttpServer,
{
    /// The attestor
    pub attestor: A,

    /// The engine router (websocket transport)
    pub engine_router: Router,

    /// The governance
    pub governance: G,

    /// The HTTP server
    pub http_server: H,

    /// The origin of this node
    pub origin: String,
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

/// Full context with all bootstrapped managers
#[derive(Clone)]
pub struct FullContext<A, G, AM, RM, IM, PM, SM>
where
    A: Attestor,
    G: TopologyAdaptor,
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
{
    /// Application manager for handling application lifecycle
    pub application_manager: AM,
    /// The attestor
    pub attestor: A,
    /// The governance
    pub governance: G,
    /// Identity manager for user identity operations
    pub identity_manager: IM,
    /// Origin for this node
    pub origin: String,
    /// Passkey manager for `WebAuthn` operations
    pub passkey_manager: PM,
    /// Session manager for user session handling
    pub sessions_manager: SM,
    /// Runtime pool manager for JavaScript runtime management
    pub runtime_pool_manager: RM,
}

/// Internal state for the bootstrapped context managers
#[allow(clippy::struct_field_names)]
pub struct BootstrappedState<A, G, AM, RM, IM, PM, SM>
where
    A: Attestor,
    G: TopologyAdaptor,
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
{
    pub application_manager: AM,
    pub attestor: A,
    pub governance: G,
    pub identity_manager: IM,
    pub origin: String,
    pub passkey_manager: PM,
    pub runtime_pool_manager: RM,
    pub sessions_manager: SM,
}

impl<A, G, AM, RM, IM, PM, SM> BootstrappedState<A, G, AM, RM, IM, PM, SM>
where
    A: Attestor,
    G: TopologyAdaptor,
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
{
    /// Create a new bootstrapped state
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        application_manager: AM,
        attestor: A,
        governance: G,
        identity_manager: IM,
        origin: String,
        passkey_manager: PM,
        runtime_pool_manager: RM,
        sessions_manager: SM,
    ) -> Self {
        Self {
            application_manager,
            attestor,
            governance,
            identity_manager,
            origin,
            passkey_manager,
            runtime_pool_manager,
            sessions_manager,
        }
    }

    /// Create a full context from this bootstrapped state
    pub fn to_full_context(&self) -> FullContext<A, G, AM, RM, IM, PM, SM>
    where
        A: Clone,
        G: Clone,
        AM: Clone,
        RM: Clone,
        IM: Clone,
        PM: Clone,
        SM: Clone,
    {
        FullContext {
            application_manager: self.application_manager.clone(),
            attestor: self.attestor.clone(),
            governance: self.governance.clone(),
            identity_manager: self.identity_manager.clone(),
            origin: self.origin.clone(),
            passkey_manager: self.passkey_manager.clone(),
            sessions_manager: self.sessions_manager.clone(),
            runtime_pool_manager: self.runtime_pool_manager.clone(),
        }
    }
}
