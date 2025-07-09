//! Group allocator module
//!
//! This module provides intelligent allocation of nodes to consensus groups.

pub mod allocator;
pub mod migration_coordinator;
pub mod rebalancer;
pub mod scoring;
pub mod topology;

#[cfg(test)]
mod tests;

// Re-export main types from allocator
pub use allocator::{
    ConsensusGroup, GroupAllocations, GroupAllocator, GroupAllocatorConfig, GroupState,
};

// Re-export types from topology
pub use topology::{
    GroupAnalysis, GroupImpact, GroupRecommendation, RecommendedAction, RegionAnalysis,
    TopologyAnalyzer,
};

// Re-export types from scoring
pub use scoring::{AllocationScore, AllocationScorer, ScoringConfig};

// Re-export types from rebalancer
pub use rebalancer::{
    GroupRebalancer, MigrationPhase, MigrationState, NodeMigration, NodeMigrationState,
    RebalancingPlan,
};

// Re-export types from migration coordinator
pub use migration_coordinator::{
    MigrationConfig, MigrationCoordinator, MigrationEvent, MigrationProgress,
};
