//! Discovery submodule for cluster service

mod coordinator;
mod manager;
mod protocol;
mod state_machine;

pub use coordinator::CoordinatorElection;
pub use manager::{DiscoveryConfig, DiscoveryManager, DiscoveryOutcome};
pub use protocol::DiscoveryProtocol;
pub use state_machine::{DiscoveryEvent, DiscoveryState};
