//! Mock implementation of the governance interface for testing purposes.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::sync::Arc;

use async_trait::async_trait;
use proven_governance::{Governance, Node, Version};

mod error;
pub use error::Error;

/// Mock implementation of the governance interface.
#[derive(Debug, Clone)]
pub struct MockGovernance {
    nodes: Arc<Vec<Node>>,
    versions: Arc<Vec<Version>>,
}

impl MockGovernance {
    /// Create a new mock governance implementation with the given nodes and versions.
    #[must_use]
    pub fn new(nodes: Vec<Node>, versions: Vec<Version>) -> Self {
        Self {
            nodes: Arc::new(nodes),
            versions: Arc::new(versions),
        }
    }
}

#[async_trait]
impl Governance for MockGovernance {
    type Error = Error;

    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error> {
        Ok((*self.versions).clone())
    }

    async fn get_topology(&self) -> Result<Vec<Node>, Self::Error> {
        Ok((*self.nodes).clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::SystemTime;

    use proven_governance::{Node, NodeSpecialization, Version};

    use super::*;

    #[tokio::test]
    async fn test_mock_governance() {
        // Create test nodes
        let node_1 = Node {
            availability_zone: "az1".to_string(),
            fqdn: "node1.example.com".to_string(),
            public_key: "key1".to_string(),
            region: "region1".to_string(),
            specializations: HashSet::new(),
        };

        let node_2 = Node {
            availability_zone: "az2".to_string(),
            fqdn: "node2.example.com".to_string(),
            public_key: "key2".to_string(),
            region: "region2".to_string(),
            specializations: {
                let mut specs = HashSet::new();
                specs.insert(NodeSpecialization::RadixMainnet);
                specs
            },
        };

        // Create test versions
        let version_1 = Version {
            activated_at: SystemTime::now(),
            ne_pcr0: "pcr0-1".to_string(),
            ne_pcr1: "pcr1-1".to_string(),
            ne_pcr2: "pcr2-1".to_string(),
            sequence: 1,
        };

        let version_2 = Version {
            activated_at: SystemTime::now(),
            ne_pcr0: "pcr0-2".to_string(),
            ne_pcr1: "pcr1-2".to_string(),
            ne_pcr2: "pcr2-2".to_string(),
            sequence: 2,
        };

        // Create mock governance
        let nodes = vec![node_1.clone(), node_2.clone()];
        let versions = vec![version_1.clone(), version_2.clone()];
        let governance = MockGovernance::new(nodes, versions);

        // Test get_topology
        let topology = governance.get_topology().await.unwrap();
        assert_eq!(topology.len(), 2);
        assert!(topology.contains(&node_1));
        assert!(topology.contains(&node_2));

        // Test get_active_versions
        let active_versions = governance.get_active_versions().await.unwrap();
        assert_eq!(active_versions.len(), 2);
        assert!(active_versions.contains(&version_1));
        assert!(active_versions.contains(&version_2));
    }
}
