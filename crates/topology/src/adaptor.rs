//! Topology adaptor trait for retrieving network topology and version information.

use std::fmt::Debug;

use async_trait::async_trait;

use crate::{Node, Version, error::TopologyAdaptorError};

/// Abstract interface for getting active version information, network topology, etc. from a topology source.
#[async_trait]
pub trait TopologyAdaptor
where
    Self: Debug + Send + Sync + Clone + 'static,
{
    /// The error type for this adaptor.
    type Error: TopologyAdaptorError;

    /// Get the active versions of the node.
    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error>;

    /// Get the alternates auth gateways.
    async fn get_alternates_auth_gateways(&self) -> Result<Vec<String>, Self::Error>;

    /// Get the primary auth gateway.
    async fn get_primary_auth_gateway(&self) -> Result<String, Self::Error>;

    /// Get the network topology.
    async fn get_topology(&self) -> Result<Vec<Node>, Self::Error>;
}
