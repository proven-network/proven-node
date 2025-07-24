//! Core trait definitions

pub mod consensus;
pub mod handlers;
pub mod lifecycle;

// Re-export commonly used traits
pub use consensus::ConsensusLayer;
pub use handlers::{EventHandler, OperationHandler};
pub use lifecycle::{GroupManager, ServiceCoordinator, ServiceLifecycle, StreamManager};
