//! Membership service for unified cluster membership management
//!
//! This service handles:
//! - Node discovery and cluster formation
//! - Health monitoring and failure detection
//! - Membership change coordination
//! - Event publishing for membership updates

mod config;
mod discovery;
mod health;
mod messages;
mod service;
mod types;

pub use config::MembershipConfig;
pub use messages::{MembershipMessage, MembershipResponse};
pub use service::MembershipService;
pub use types::{
    ClusterFormationState, HealthInfo, MembershipEvent, MembershipView, NodeMembership, NodeRole,
    NodeStatus,
};
