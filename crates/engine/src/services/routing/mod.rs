//! Routing service for consensus operations
//!
//! This service handles:
//! - Global operation routing to consensus layers
//! - Stream operation routing to appropriate groups
//! - Load-aware routing decisions
//! - Routing table management
//! - Cross-layer operation coordination
//!
//! ## Overview
//!
//! The routing service acts as a central dispatcher for all consensus operations,
//! ensuring they reach the correct consensus group or global layer based on the
//! operation type and target.
//!
//! ## Architecture
//!
//! - **Global Router**: Routes cluster-wide operations
//! - **Stream Router**: Routes stream-specific operations to groups
//! - **Load Balancer**: Makes routing decisions based on group load
//! - **Routing Table**: Maintains stream-to-group mappings
//!
//! ## Usage
//!
//! ```rust,ignore
//! let routing_service = RoutingService::new(config);
//! routing_service.start().await?;
//!
//! // Route a stream operation
//! let response = routing_service.route_stream_operation(
//!     "my-stream",
//!     operation
//! ).await?;
//!
//! // Get routing info
//! let info = routing_service.get_routing_info("my-stream").await?;
//! ```

mod balancer;
mod router;
mod service;
mod table;
mod types;

pub use balancer::{LoadBalancer, LoadStrategy};
pub use router::{OperationRouter, RouteDecision};
pub use service::{RoutingConfig, RoutingService};
pub use table::{RouteEntry, RoutingTable};
pub use types::{
    GroupRoute, LoadInfo, RoutingDecision, RoutingError, RoutingHealth, RoutingInfo,
    RoutingMetrics, RoutingResult, RoutingStrategy, StreamRoute,
};
