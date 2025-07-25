//! Foundation-level routing table for sharing routing information across services

mod table;
mod types;

pub use table::RoutingTable;
pub use types::{GroupLocation, GroupRoute, RoutingDecision, RoutingError, StreamRoute};
