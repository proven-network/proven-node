//! Event subscribers for routing service

pub mod global;
pub mod group;

pub use global::GlobalConsensusSubscriber;
pub use group::GroupConsensusSubscriber;
