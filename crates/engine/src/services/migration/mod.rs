//! Migration service for consensus system
//!
//! This service handles:
//! - Stream migration between consensus groups
//! - Node migration between groups
//! - Group allocation and rebalancing
//! - Load distribution optimization
//!
//! ## Overview
//!
//! The migration service manages the movement of data and nodes between
//! consensus groups to maintain balanced load and optimal performance.
//!
//! ## Components
//!
//! - **Stream Migration**: Moves streams between groups for load balancing
//! - **Node Migration**: Moves nodes between groups for resource optimization
//! - **Group Allocator**: Intelligently assigns nodes to groups based on regions
//! - **Rebalancer**: Monitors and suggests rebalancing operations
//!
//! ## Usage
//!
//! ```rust,ignore
//! let migration_service = MigrationService::new(config);
//! migration_service.start().await?;
//!
//! // Start a stream migration
//! migration_service.start_stream_migration(
//!     "my-stream",
//!     source_group,
//!     target_group
//! ).await?;
//!
//! // Check migration progress
//! let progress = migration_service.get_stream_migration_progress("my-stream").await?;
//! ```

mod allocator;
mod node_migration;
mod rebalancer;
mod service;
mod stream_migration;
mod types;

pub use service::{MigrationConfig, MigrationService};
pub use types::MigrationError;
