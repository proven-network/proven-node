//! Simple integration test for vsock-fuse
//!
//! This example demonstrates how to run both host and enclave components
//! together using TCP for testing on non-Linux platforms.

#![allow(clippy::field_reassign_with_default)]

use proven_vsock_fuse::{
    BlobId, TierHint,
    config::{Config, HotTierConfig, RpcConfig},
    enclave::client::EnclaveStorageClient,
    host::HostServiceBuilder,
    storage::BlobStorage,
};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    println!("Starting vsock-fuse integration test");
    println!(
        "Platform: {}",
        if cfg!(target_os = "linux") {
            "Linux (VSOCK)"
        } else {
            "Non-Linux (TCP)"
        }
    );

    // Create temporary directory for storage
    let temp_dir = TempDir::new()?;
    let storage_path = temp_dir.path().to_path_buf();

    // Create a simple config
    let mut config = Config::default();

    config.rpc = RpcConfig {
        host_cid: 3, // Standard host CID (ignored on non-Linux)
        port: 5555,
        connection_timeout: Duration::from_secs(30),
        request_timeout: Duration::from_secs(60),
        max_request_size: 100 * 1024 * 1024,  // 100MB
        max_response_size: 100 * 1024 * 1024, // 100MB
    };
    config.storage.hot_tier = HotTierConfig {
        base_path: storage_path.clone(),
        max_size: 100 * 1024 * 1024,     // 100MB
        target_free_space_percent: 30,   // Keep 30% free
        emergency_threshold_percent: 10, // Emergency at 10% free
    };

    // Disable S3 cold tier for testing by setting an empty bucket
    config.storage.cold_tier.bucket = String::new();

    // Start the host service
    println!("\nStarting host service on port {}", config.rpc.port);
    let mut host_service = HostServiceBuilder::new()
        .config(config.clone())
        .build()
        .await?;

    host_service.start().await?;

    // Give the server time to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create enclave client with reduced connection pool for testing
    println!("\nCreating enclave client");
    let rpc_client = proven_vsock_rpc::RpcClient::builder()
        .vsock_addr(
            #[cfg(target_os = "linux")]
            tokio_vsock::VsockAddr::new(config.rpc.host_cid, config.rpc.port),
            #[cfg(not(target_os = "linux"))]
            std::net::SocketAddr::from(([127, 0, 0, 1], config.rpc.port as u16)),
        )
        .pool_size(1) // Use single connection for testing
        .default_timeout(Duration::from_secs(5)) // Add timeout
        .build()?;

    let client = EnclaveStorageClient::from_client(std::sync::Arc::new(rpc_client));

    // Test storing a blob
    let blob_id = BlobId([1; 32]);
    let test_data = b"Hello, VSOCK-FUSE!".to_vec();

    println!("\nStoring blob {blob_id:?}");
    client
        .store_blob(blob_id, test_data.clone(), TierHint::PreferHot)
        .await?;

    // Add small delay between operations
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test retrieving the blob
    println!("Retrieving blob {blob_id:?}");
    let retrieved = client.get_blob(blob_id).await?;

    assert_eq!(retrieved.data, test_data);
    println!("✓ Data matches!");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test listing blobs
    println!("\nListing blobs");
    let blobs = client.list_blobs(None).await?;
    println!("Found {} blobs", blobs.len());

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test getting stats
    println!("\nGetting storage stats");
    let stats = client.get_stats().await?;
    println!("Hot tier: {} bytes used", stats.hot_tier.used_bytes);
    println!("Cold tier: {} bytes used", stats.cold_tier.used_bytes);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test deleting the blob
    println!("\nDeleting blob {blob_id:?}");
    client.delete_blob(blob_id).await?;

    // Verify it's gone
    match client.get_blob(blob_id).await {
        Err(_) => println!("✓ Blob successfully deleted"),
        Ok(_) => panic!("Blob should have been deleted!"),
    }

    // Shutdown
    println!("\nShutting down");
    host_service.stop().await?;

    println!("\n✓ Integration test completed successfully!");

    Ok(())
}
