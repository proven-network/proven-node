//! Integration tests for HLC time sync with Chrony multi-source validation.

use proven_hlc::{HLCProvider, HlcConfig};
use proven_hlc_time_sync::TimeSyncHlcProvider;
use proven_topology::NodeId;
use std::time::Duration;
use tokio::time::sleep;

/// Check if chrony is available.
async fn is_chrony_available() -> bool {
    // Try to connect to chronyd using chronyc
    use std::process::Command;

    let output = Command::new("chronyc").arg("tracking").output();

    if output.is_err() {
        eprintln!("chronyc command not found");
        return false;
    }

    let output = output.unwrap();
    if !output.status.success() {
        eprintln!("chronyd daemon not running or not accessible");
        return false;
    }

    true
}

/// Test basic HLC monotonicity with multi-source validation.
#[tokio::test]
async fn test_hlc_monotonicity() {
    // Skip test if chrony is not available
    if !is_chrony_available().await {
        eprintln!("Skipping test: chrony not available");
        return;
    }

    let node_id = NodeId::from_seed(1);
    let config = HlcConfig::new(node_id);

    let hlc = TimeSyncHlcProvider::new(config)
        .await
        .expect("Failed to create HLC provider");

    // Get multiple timestamps
    let ts1 = hlc.now().await.expect("Failed to get timestamp 1");
    sleep(Duration::from_millis(10)).await;
    let ts2 = hlc.now().await.expect("Failed to get timestamp 2");
    sleep(Duration::from_millis(10)).await;
    let ts3 = hlc.now().await.expect("Failed to get timestamp 3");

    // Verify monotonicity
    assert!(ts1.hlc < ts2.hlc, "Timestamps must be strictly increasing");
    assert!(ts2.hlc < ts3.hlc, "Timestamps must be strictly increasing");

    // Verify node ID is preserved
    assert_eq!(ts1.hlc.node_id, node_id);
    assert_eq!(ts2.hlc.node_id, node_id);
    assert_eq!(ts3.hlc.node_id, node_id);
}

/// Test causality preservation between nodes.
#[tokio::test]
async fn test_causality_preservation() {
    // Skip test if chrony is not available
    if !is_chrony_available().await {
        eprintln!("Skipping test: chrony not available");
        return;
    }

    let node1_id = NodeId::from_seed(1);
    let node2_id = NodeId::from_seed(2);

    let hlc1 = TimeSyncHlcProvider::new(HlcConfig::new(node1_id))
        .await
        .expect("Failed to create HLC1");
    let hlc2 = TimeSyncHlcProvider::new(HlcConfig::new(node2_id))
        .await
        .expect("Failed to create HLC2");

    // Node 1 creates a timestamp
    let ts1 = hlc1
        .now()
        .await
        .expect("Failed to get timestamp from node 1");

    // Node 2 receives and updates from node 1's timestamp
    hlc2.update_from(&ts1.hlc)
        .expect("Failed to update from remote timestamp");

    // Node 2's next timestamp should be after node 1's
    let ts2 = hlc2
        .now()
        .await
        .expect("Failed to get timestamp from node 2");

    assert!(
        ts2.hlc > ts1.hlc,
        "Node 2's timestamp should be after node 1's (causality)"
    );
    assert_eq!(ts2.hlc.node_id, node2_id, "Node ID should be preserved");
}

/// Test health check functionality.
#[tokio::test]
async fn test_health_check() {
    // Skip test if chrony is not available
    if !is_chrony_available().await {
        eprintln!("Skipping test: chrony not available");
        return;
    }

    let node_id = NodeId::from_seed(1);
    let config = HlcConfig::new(node_id);

    let hlc = TimeSyncHlcProvider::new(config)
        .await
        .expect("Failed to create HLC provider");

    // Check health
    let is_healthy = hlc.is_healthy().await.expect("Failed to check health");

    // With mock data or real chrony, should be healthy initially
    assert!(is_healthy, "HLC provider should be healthy");
}

/// Test uncertainty bounds.
#[tokio::test]
async fn test_uncertainty_bounds() {
    // Skip test if chrony is not available
    if !is_chrony_available().await {
        eprintln!("Skipping test: chrony not available");
        return;
    }

    let node_id = NodeId::from_seed(1);
    let config = HlcConfig::new(node_id);

    let hlc = TimeSyncHlcProvider::new(config)
        .await
        .expect("Failed to create HLC provider");

    // Get uncertainty
    let uncertainty_us = hlc.uncertainty_us().await;

    // Uncertainty should be reasonable (not zero, not huge)
    // Note: Real-world uncertainty can be 100-200ms when using public NTP
    assert!(uncertainty_us > 0.0, "Uncertainty should be positive");
    assert!(
        uncertainty_us < 500_000.0,
        "Uncertainty should be less than 500ms, got {} us",
        uncertainty_us
    );

    // Get timestamp with uncertainty
    let ts = hlc.now().await.expect("Failed to get timestamp");
    assert!(ts.uncertainty_us > 0.0, "Timestamp should have uncertainty");
}
