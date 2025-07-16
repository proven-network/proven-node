//! Lifecycle service for consensus system management
//!
//! This service handles:
//! - Component initialization and startup
//! - Graceful shutdown procedures
//! - Health monitoring and status tracking
//! - Cluster initialization (single-node and multi-node)
//! - Component dependency management
//!
//! ## Overview
//!
//! The lifecycle service ensures that all consensus components are properly
//! initialized, started, and stopped in the correct order. It also monitors
//! the health of components and provides status information.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let lifecycle_service = LifecycleService::new(config);
//!
//! // Start all components
//! lifecycle_service.start_all().await?;
//!
//! // Check component health
//! let health = lifecycle_service.check_health().await?;
//!
//! // Graceful shutdown
//! lifecycle_service.shutdown().await?;
//! ```

mod health;
mod service;
mod shutdown;
mod startup;
mod types;

pub use health::{HealthChecker, HealthReport};
pub use service::{LifecycleConfig, LifecycleService};
pub use shutdown::{ShutdownCoordinator, ShutdownOptions};
pub use startup::{StartupCoordinator, StartupOptions};
pub use types::{
    ClusterFormationStrategy, ComponentHealth, ComponentState, HealthStatus, InitializationMode,
    LifecycleError, LifecycleResult, ShutdownPhase, StartupPhase,
};
