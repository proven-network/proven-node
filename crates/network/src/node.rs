use std::collections::HashSet;

use proven_governance::{NodeSpecialization, TopologyNode};

/// A node in the network.
#[derive(Clone, Debug)]
pub struct Node(TopologyNode);

impl Node {
    /// Get the availability zone of the node.
    pub fn availability_zone(&self) -> &str {
        &self.0.availability_zone
    }

    /// Get the fqdn of the node.
    pub fn fqdn(&self) -> &str {
        &self.0.fqdn
    }

    /// Get the public key of the node.
    pub fn public_key(&self) -> &str {
        &self.0.public_key
    }

    /// Get the region of the node.
    pub fn region(&self) -> &str {
        &self.0.region
    }

    /// Get the specializations of the node.
    pub fn specializations(&self) -> &HashSet<NodeSpecialization> {
        &self.0.specializations
    }
}

impl From<TopologyNode> for Node {
    fn from(node: TopologyNode) -> Self {
        Self(node)
    }
}
