//! Handler traits for operations and events

use async_trait::async_trait;

use crate::error::ConsensusResult;

/// Handler for consensus operations
#[async_trait]
pub trait OperationHandler: Send + Sync {
    /// The type of operations this handler processes
    type Operation: Send + Sync;

    /// The type of response returned
    type Response: Send + Sync;

    /// Handle an operation
    async fn handle(
        &self,
        operation: Self::Operation,
        is_replay: bool,
    ) -> ConsensusResult<Self::Response>;

    /// Validate an operation before applying
    async fn validate(&self, operation: &Self::Operation) -> ConsensusResult<()>;
}

/// Handler for events
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// The type of events this handler processes
    type Event: Send + Sync;

    /// Handle an event
    async fn handle(&self, event: Self::Event) -> ConsensusResult<()>;

    /// Check if this handler should handle the given event
    async fn should_handle(&self, _event: &Self::Event) -> bool {
        true
    }
}
