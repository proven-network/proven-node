//! Group allocator module
//!
//! This module provides intelligent allocation of nodes to consensus groups,
//! integrated as a core part of the consensus system.

pub mod allocator;
pub mod migration_coordinator;
pub mod rebalancer;
pub mod scoring;
pub mod topology;

#[cfg(test)]
mod tests;

// Re-export main types from allocator
pub use allocator::{GroupAllocations, GroupAllocator, GroupAllocatorConfig};

// Re-export types from topology
pub use topology::GroupRecommendation;

// Re-export types from scoring

// Re-export types from rebalancer
pub use rebalancer::RebalancingPlan;

// Re-export types from migration coordinator
