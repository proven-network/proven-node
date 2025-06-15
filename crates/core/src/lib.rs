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

pub use error::{Error, Result};
pub use full::{Core, CoreOptions};
pub use light::{LightCore, LightCoreOptions};
use proven_applications::ApplicationManagement;
use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_identity::IdentityManagement;
use proven_network::ProvenNetwork;
use proven_runtime::RuntimePoolManagement;

#[derive(Clone)]
pub(crate) struct FullContext<AM, RM, SM, A, G>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
    SM: IdentityManagement,
    A: Attestor,
    G: Governance,
{
    pub application_manager: AM,
    pub identity_manager: SM,
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
