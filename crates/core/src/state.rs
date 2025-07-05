//! Core state management types and transitions
//!
//! This module contains the types and logic for managing the core state transitions
//! between bootstrapping and bootstrapped modes.

use std::sync::Arc;

use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_consensus::Consensus;
use proven_governance::Governance;
use proven_http::HttpServer;
use proven_identity::IdentityManagement;
use proven_network::ProvenNetwork;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;

/// Core mode state
#[derive(Debug, Clone)]
pub enum CoreMode {
    /// Bootstrapping mode with minimal functionality
    Bootstrapping,
    /// Bootstrapped mode with all features enabled
    Bootstrapped,
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
pub struct FullContext<AM, RM, IM, PM, SM, A, G>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
    A: Attestor,
    G: Governance,
{
    /// Application manager for handling application lifecycle
    pub application_manager: AM,
    /// Identity manager for user identity operations
    pub identity_manager: IM,
    /// Passkey manager for `WebAuthn` operations
    pub passkey_manager: PM,
    /// Session manager for user session handling
    pub sessions_manager: SM,
    /// Network for peer discovery and communication
    pub network: ProvenNetwork<G, A>,
    /// Runtime pool manager for JavaScript runtime management
    pub runtime_pool_manager: RM,
}

/// Light context for basic functionality
#[derive(Clone)]
pub struct LightContext<A, G>
where
    A: Attestor,
    G: Governance,
{
    /// Network for peer discovery and communication
    pub network: ProvenNetwork<G, A>,
}

/// Internal state for the bootstrapped context managers
#[allow(clippy::struct_field_names)]
pub struct BootstrappedState<AM, RM, IM, PM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
{
    pub application_manager: AM,
    pub runtime_pool_manager: RM,
    pub identity_manager: IM,
    pub passkey_manager: PM,
    pub sessions_manager: SM,
}

impl<AM, RM, IM, PM, SM> BootstrappedState<AM, RM, IM, PM, SM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    IM: IdentityManagement,
    PM: PasskeyManagement,
    SM: SessionManagement,
{
    /// Create a new bootstrapped state
    pub fn new(
        application_manager: AM,
        runtime_pool_manager: RM,
        identity_manager: IM,
        passkey_manager: PM,
        sessions_manager: SM,
    ) -> Self {
        Self {
            application_manager,
            runtime_pool_manager,
            identity_manager,
            passkey_manager,
            sessions_manager,
        }
    }

    /// Create a full context from this bootstrapped state
    pub fn to_full_context<A, G>(
        &self,
        network: ProvenNetwork<G, A>,
    ) -> FullContext<AM, RM, IM, PM, SM, A, G>
    where
        AM: Clone,
        RM: Clone,
        IM: Clone,
        PM: Clone,
        SM: Clone,
        A: Attestor,
        G: Governance,
    {
        FullContext {
            application_manager: self.application_manager.clone(),
            identity_manager: self.identity_manager.clone(),
            passkey_manager: self.passkey_manager.clone(),
            sessions_manager: self.sessions_manager.clone(),
            network,
            runtime_pool_manager: self.runtime_pool_manager.clone(),
        }
    }
}
