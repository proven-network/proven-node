//! Events emitted by the routing service

use crate::foundation::types::ConsensusGroupId;
use crate::services::event::ServiceEvent;
use proven_topology::NodeId;

/// Group location update details
#[derive(Debug, Clone)]
pub struct GroupLocationUpdate {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// New leader
    pub leader: Option<NodeId>,
    /// All nodes in the group
    pub nodes: Vec<NodeId>,
    /// Whether this node is a member
    pub is_local: bool,
}

/// Events emitted by the routing service
#[derive(Debug, Clone)]
pub enum RoutingEvent {
    /// Group location updated
    GroupLocationUpdated(Box<GroupLocationUpdate>),

    /// Group location removed
    GroupLocationRemoved {
        /// Group ID that was removed
        group_id: ConsensusGroupId,
    },

    /// Routing table cleared
    RoutingTableCleared,
}

impl ServiceEvent for RoutingEvent {
    fn event_name(&self) -> &'static str {
        match self {
            Self::GroupLocationUpdated(_) => "RoutingEvent::GroupLocationUpdated",
            Self::GroupLocationRemoved { .. } => "RoutingEvent::GroupLocationRemoved",
            Self::RoutingTableCleared => "RoutingEvent::RoutingTableCleared",
        }
    }
}
