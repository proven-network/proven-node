//! Core logic for the Proven node and the entrypoint for all user
//! interactions.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

mod error;
mod full;
mod handlers;
mod light;
mod rpc;

pub use error::Error;
pub use full::{Core, CoreOptions};
pub use light::{LightCore, LightCoreOptions};

// Export RPC command and response types for client use
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_network::ProvenNetwork;
use proven_passkeys::PasskeyManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::SessionManagement;
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
