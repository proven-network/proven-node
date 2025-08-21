//! Integration tests for HLC time sync with local chrony.

use proven_hlc::{HLCProvider, HlcConfig};
use proven_hlc_time_sync::{TimeServer, TimeSyncHlcProvider};
use proven_topology::NodeId;
use std::time::Duration;
use tokio::time::sleep;

#[cfg(not(target_os = "linux"))]
use std::net::SocketAddr;

/// Test basic time synchronization between client and server.
/// Note: Both client and server use the same system clock in this test,
/// but we're validating the messaging protocol and synchronization logic.
#[tokio::test]
async fn test_time_sync_client_server() {
    // Skip test if chrony is not available
    if !is_chrony_available().await {
        eprintln!("Skipping test: chrony not available");
        return;
    }

    // Create node IDs
    let server_node_id = NodeId::from_seed(1);
    let client_node_id = NodeId::from_seed(2);

    // Configure server address for the platform
    #[cfg(not(target_os = "linux"))]
    let server_addr: SocketAddr = "127.0.0.1:19876".parse().unwrap();
    #[cfg(target_os = "linux")]
    let server_addr = tokio_vsock::VsockAddr::new(2, 19876); // CID 2 for host

    let server = TimeServer::new(
        #[cfg(not(target_os = "linux"))]
        server_addr,
        #[cfg(target_os = "linux")]
        server_addr,
    );

    // Run server in background
    let server_handle = tokio::spawn(async move {
        server.serve().await.expect("Server failed");
    });

    // Give server time to start
    sleep(Duration::from_millis(500)).await;

    // Create client with enclave mode
    let client_config = HlcConfig::new(client_node_id);
    let client_hlc = TimeSyncHlcProvider::new_enclave(
        &client_config,
        #[cfg(not(target_os = "linux"))]
        server_addr,
        #[cfg(target_os = "linux")]
        server_addr,
    )
    .expect("Failed to create client HLC");

    // Create host HLC for comparison
    let host_config = HlcConfig::new(server_node_id);
    let host_hlc = TimeSyncHlcProvider::new_host(host_config)
        .await
        .expect("Failed to create host HLC");

    // Test timestamp generation
    let client_ts1 = client_hlc
        .now()
        .await
        .expect("Failed to get client timestamp");
    let host_ts1 = host_hlc.now().await.expect("Failed to get host timestamp");

    // Timestamps should be close (within a few milliseconds)
    let time_diff =
        (client_ts1.wall_time.timestamp_millis() - host_ts1.wall_time.timestamp_millis()).abs();
    assert!(
        time_diff < 1000,
        "Time difference too large: {}ms",
        time_diff
    );

    // Test monotonicity
    let client_ts2 = client_hlc
        .now()
        .await
        .expect("Failed to get second client timestamp");
    assert!(client_ts2.hlc > client_ts1.hlc, "Timestamps not monotonic");

    // Test uncertainty tracking
    let uncertainty = client_hlc.uncertainty_us().await;
    assert!(uncertainty > 0.0, "Uncertainty should be positive");
    assert!(
        uncertainty < 100_000.0,
        "Uncertainty too large: {}μs",
        uncertainty
    );

    // Test health check
    assert!(client_hlc.check_health(), "Client HLC should be healthy");
    assert!(host_hlc.check_health(), "Host HLC should be healthy");

    // Clean up
    server_handle.abort();
}

/// Test NTS validation with local chrony.
#[tokio::test]
async fn test_nts_validation() {
    // Skip test if chrony is not available
    if !is_chrony_available().await {
        eprintln!("Skipping test: chrony not available");
        return;
    }

    let node_id = NodeId::from_seed(3);
    let config = HlcConfig::new(node_id);

    let hlc = TimeSyncHlcProvider::new_host(config)
        .await
        .expect("Failed to create HLC with NTS");

    // Generate some timestamps to populate measurements
    for _ in 0..5 {
        let ts = hlc.now().await.expect("Failed to get timestamp");
        assert!(ts.uncertainty_us > 0.0, "Should have uncertainty");
        sleep(Duration::from_millis(10)).await;
    }

    // Force NTS update
    hlc.force_nts_update()
        .await
        .expect("Failed to force NTS update");

    // Check health after update
    assert!(hlc.check_health(), "HLC should be healthy after NTS update");

    // Verify uncertainty is reasonable
    let uncertainty = hlc.uncertainty_us().await;
    assert!(uncertainty > 0.0, "Uncertainty should be positive");
    assert!(
        uncertainty < 50_000.0,
        "Uncertainty too large after calibration: {}μs",
        uncertainty
    );
}

/// Test causality preservation between nodes.
#[tokio::test]
async fn test_causality_preservation() {
    // Skip test if chrony is not available
    if !is_chrony_available().await {
        eprintln!("Skipping test: chrony not available");
        return;
    }

    let node1_id = NodeId::from_seed(4);
    let node2_id = NodeId::from_seed(5);

    let hlc1 = TimeSyncHlcProvider::new_host(HlcConfig::new(node1_id.clone()))
        .await
        .expect("Failed to create HLC1");
    let hlc2 = TimeSyncHlcProvider::new_host(HlcConfig::new(node2_id.clone()))
        .await
        .expect("Failed to create HLC2");

    // Node 1 creates a timestamp
    let ts1 = hlc1
        .now()
        .await
        .expect("Failed to get timestamp from node 1");

    // Node 2 updates from node 1's timestamp
    hlc2.update_from(&ts1.hlc)
        .expect("Failed to update from remote timestamp");

    // Node 2's next timestamp should be after node 1's
    let ts2 = hlc2
        .now()
        .await
        .expect("Failed to get timestamp from node 2");
    assert!(ts2.hlc > ts1.hlc, "Causality not preserved");

    // Verify the timestamps have correct node IDs
    assert_eq!(ts1.hlc.node_id, node1_id);
    assert_eq!(ts2.hlc.node_id, node2_id);
}

/// Check if chrony is available on the system.
async fn is_chrony_available() -> bool {
    // Try to connect to chrony's command socket
    match tokio::process::Command::new("chronyc")
        .arg("tracking")
        .output()
        .await
    {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}
