//! Cluster service for managing consensus cluster operations
//!
//! This service handles:
//! - Cluster discovery and formation
//! - Node joining and leaving
//! - Cluster state management
//! - Leader election coordination
//! - Cluster health monitoring
//!
//! ## Overview
//!
//! The cluster service manages the lifecycle of consensus clusters, from initial
//! discovery through formation and ongoing membership management.
//!
//! ## Architecture
//!
//! - **Discovery Manager**: Handles peer discovery and cluster detection
//! - **Formation Manager**: Manages cluster initialization and formation
//! - **Membership Manager**: Handles nodes joining and leaving
//! - **State Manager**: Tracks cluster state and transitions
//!
//! ## Usage
//!
//! ```rust,ignore
//! let cluster_service = ClusterService::new(config, node_id);
//! cluster_service.start().await?;
//!
//! // Discover peers
//! let peers = cluster_service.discover_peers().await?;
//!
//! // Form or join cluster
//! if peers.is_empty() {
//!     cluster_service.form_cluster(ClusterFormationMode::SingleNode).await?;
//! } else {
//!     cluster_service.join_cluster(peers[0].clone()).await?;
//! }
//! ```

mod discovery;
mod formation;
mod membership;
mod messages;
mod service;
mod state;
mod types;

pub use discovery::{DiscoveryConfig, DiscoveryManager, DiscoveryOutcome, DiscoveryState};
pub use formation::{FormationManager, FormationRequest, FormationResult};
pub use membership::{JoinRequest, LeaveRequest, MembershipManager};
pub use messages::{
    CLUSTER_NAMESPACE, ClusterHeartbeat, ConsensusGroupJoinRequest, ConsensusGroupJoinResponse,
    DiscoveryRequest, DiscoveryResponse, DiscoveryRound, JoinRequest as ClusterJoinRequest,
    JoinResponse as ClusterJoinResponse,
};
pub use service::{ClusterConfig, ClusterFormationCallback, ClusterFormationEvent, ClusterService};
pub use state::{ClusterState as ClusterStateInfo, StateManager, StateTransition, TransitionGuard};
pub use types::{
    ClusterError, ClusterHealth, ClusterInfo, ClusterMetrics, ClusterResult, ClusterState,
    ClusterTransition, DiscoveryMode, DiscoveryStrategy, FormationMode, FormationStrategy,
    HealthCheck, HealthStatus, MembershipChange, MembershipStatus, NodeInfo, NodeRole, NodeState,
};
