//! Consensus-related traits

use async_trait::async_trait;

use crate::error::ConsensusResult;
use crate::foundation::types::{ConsensusRole, OperationId, Term};

/// Trait for consensus layers (global and group)
#[async_trait]
pub trait ConsensusLayer: Send + Sync {
    /// The type of operations this layer handles
    type Operation: Send + Sync;

    /// The type of state this layer maintains
    type State: Send + Sync;

    /// Initialize the consensus layer
    async fn initialize(&self) -> ConsensusResult<()>;

    /// Propose an operation
    async fn propose(&self, operation: Self::Operation) -> ConsensusResult<OperationId>;

    /// Check if this node is the leader
    async fn is_leader(&self) -> bool;

    /// Get current term
    async fn current_term(&self) -> Term;

    /// Get current role
    async fn current_role(&self) -> ConsensusRole;
}

/// Trait for state storage
#[async_trait]
pub trait StateStore: Send + Sync {
    /// The type of state being stored
    type State: Send + Sync;

    /// Save state
    async fn save(&self, state: &Self::State) -> ConsensusResult<()>;

    /// Load state
    async fn load(&self) -> ConsensusResult<Option<Self::State>>;

    /// Clear state
    async fn clear(&self) -> ConsensusResult<()>;
}
