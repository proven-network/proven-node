//! Test utilities for local cluster

use crate::{ClusterBuilder, LocalCluster, UserIdentity};
use anyhow::Result;
use std::time::Duration;
use tempfile::TempDir;
use tracing::info;

/// Test cluster wrapper that manages temporary directories
pub struct TestCluster {
    /// The actual cluster
    pub cluster: LocalCluster,
    /// Temporary directory (kept alive for the test duration)
    _temp_dir: Option<TempDir>,
}

impl TestCluster {
    /// Get a mutable reference to the cluster
    pub const fn cluster_mut(&mut self) -> &mut LocalCluster {
        &mut self.cluster
    }

    /// Get a reference to the cluster
    #[must_use]
    pub const fn cluster(&self) -> &LocalCluster {
        &self.cluster
    }

    /// Cleanup the test cluster
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails
    pub async fn cleanup(mut self) -> Result<()> {
        self.cluster.shutdown_all().await?;
        // Drop the cluster in a blocking context to avoid runtime drop issues
        tokio::task::spawn_blocking(move || {
            drop(self);
        })
        .await?;
        Ok(())
    }
}

/// Create a test cluster with temporary directories
///
/// # Errors
///
/// Returns an error if creating the temporary directory or cluster fails
pub async fn create_test_cluster(node_count: usize) -> Result<TestCluster> {
    let temp_dir = TempDir::new()?;
    let session_id = format!("test-{}", uuid::Uuid::new_v4());

    let mut cluster = ClusterBuilder::default()
        .with_session_id(session_id)
        .with_log_dir(temp_dir.path().join("logs"))
        .with_stdout_logging() // Enable stdout logging for debugging
        .build()
        .await?;

    // Create nodes
    for i in 0..node_count {
        let name = format!("test-node-{i}");
        let node_info = cluster.create_node(&name)?;
        info!("Created test node: {} with ID {}", name, node_info.id);
    }

    Ok(TestCluster {
        cluster,
        _temp_dir: Some(temp_dir),
    })
}

/// Helper to wait for cluster formation
///
/// # Errors
///
/// Returns an error if timeout is exceeded or node operations fail
pub async fn wait_for_cluster_ready(cluster: &mut LocalCluster, timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();

    // First, start all nodes
    let nodes = cluster.get_all_nodes();
    for node_id in nodes.keys() {
        cluster.start_node(node_id).await?;
    }

    // Wait for nodes to be running
    loop {
        if start.elapsed() > timeout {
            // Log the current status of all nodes for debugging
            for node_id in nodes.keys() {
                if let Some(status) = cluster.get_node_status(node_id) {
                    eprintln!("Node {node_id} status: {status:?}");
                }
            }
            return Err(anyhow::anyhow!("Timeout waiting for cluster to be ready"));
        }

        let mut all_running = true;
        for node_id in nodes.keys() {
            if let Some(status) = cluster.get_node_status(node_id) {
                if !matches!(status, proven_local::NodeStatus::Running) {
                    all_running = false;
                    break;
                }
            } else {
                all_running = false;
                break;
            }
        }

        if all_running {
            info!("All nodes are running");
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Give services time to fully initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    Ok(())
}

/// Create a set of test users
#[must_use]
pub fn create_test_user_set() -> Vec<UserIdentity> {
    LocalCluster::create_test_users(&["alice", "bob", "charlie", "david", "eve"])
}

/// Helper to setup nodes and add them to topology
///
/// # Errors
///
/// Returns an error if node creation or topology operations fail
pub async fn setup_nodes_with_topology(
    cluster: &mut LocalCluster,
    count: usize,
) -> Result<Vec<proven_topology::NodeId>> {
    let mut node_ids = Vec::new();

    for i in 0..count {
        let name = format!("node-{i}");
        let info = cluster.create_node(&name)?;
        cluster.start_node(&info.id).await?;
        cluster.add_to_topology(&info.id).await?;
        node_ids.push(info.id);
    }

    // Wait for nodes to see each other
    tokio::time::sleep(Duration::from_secs(2)).await;

    Ok(node_ids)
}

/// Helper to verify topology state
///
/// # Errors
///
/// Returns an error if topology size doesn't match expected
pub async fn verify_topology_size(cluster: &LocalCluster, expected_size: usize) -> Result<()> {
    let topology = cluster.get_topology().await?;
    if topology.len() != expected_size {
        return Err(anyhow::anyhow!(
            "Expected {} nodes in topology, found {}",
            expected_size,
            topology.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_test_cluster() {
        let test_cluster = create_test_cluster(3).await.unwrap();
        let nodes = test_cluster.cluster.get_all_nodes();
        assert_eq!(nodes.len(), 3);

        // Verify nodes are created but not started
        for (_, node_info) in nodes {
            assert!(!node_info.name.is_empty());
            assert!(node_info.port > 0);
        }

        // Cleanup
        test_cluster.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_cluster_ready() {
        let mut test_cluster = create_test_cluster(1).await.unwrap();

        // Add nodes to topology
        let nodes = test_cluster.cluster.get_all_nodes();
        for node_id in nodes.keys() {
            test_cluster
                .cluster_mut()
                .add_to_topology(node_id)
                .await
                .unwrap();
        }

        // Wait for ready
        wait_for_cluster_ready(&mut test_cluster.cluster, Duration::from_secs(10))
            .await
            .unwrap();

        // Verify all running
        for node_id in nodes.keys() {
            let status = test_cluster.cluster.get_node_status(node_id).unwrap();
            assert!(matches!(status, proven_local::NodeStatus::Running));
        }

        // Cleanup
        test_cluster.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_topology_management() {
        let mut test_cluster = create_test_cluster(3).await.unwrap();

        // Initially topology should be empty
        let topology = test_cluster.cluster.get_topology().await.unwrap();
        assert_eq!(topology.len(), 0);

        // Add one node to topology
        let nodes = test_cluster.cluster.get_all_nodes();
        let first_node = nodes.keys().next().unwrap();
        test_cluster
            .cluster_mut()
            .add_to_topology(first_node)
            .await
            .unwrap();

        // Verify topology has one node
        let topology = test_cluster.cluster.get_topology().await.unwrap();
        assert_eq!(topology.len(), 1);

        // Remove from topology
        test_cluster
            .cluster_mut()
            .remove_from_topology(first_node)
            .unwrap();

        // Verify topology is empty again
        let topology = test_cluster.cluster.get_topology().await.unwrap();
        assert_eq!(topology.len(), 0);

        // Cleanup
        test_cluster.cleanup().await.unwrap();
    }
}
