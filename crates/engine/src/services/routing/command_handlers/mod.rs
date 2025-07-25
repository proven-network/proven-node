//! Event subscribers for routing service

pub mod client;
pub mod global;
pub mod group;
pub mod membership;
pub mod query;

pub use client::ClientServiceSubscriber;
pub use global::GlobalConsensusSubscriber;
pub use group::GroupConsensusSubscriber;
pub use membership::MembershipSubscriber;
pub use query::{GetRoutingInfoHandler, GetStreamRoutingInfoHandler, IsGroupLocalHandler};
