//! Simple mount test to debug FUSE issues

#![allow(clippy::field_reassign_with_default)]

use proven_vsock_fuse::{
    config::{Config, HotTierConfig, RpcConfig},
    enclave::EnclaveServiceBuilder,
    encryption::MasterKey,
    host::HostServiceBuilder,
};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_simple_mount() -> anyhow::Result<()> {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    println!("Creating directories...");
    let storage_dir = TempDir::new()?;
    let mount_dir = TempDir::new()?;
    let mount_path = mount_dir.path().to_path_buf();

    println!("Storage: {:?}", storage_dir.path());
    println!("Mount: {mount_path:?}");

    // Setup config
    let mut config = Config::default();
    config.rpc = RpcConfig {
        host_cid: 3,
        port: proven_util::port_allocator::allocate_port() as u32,
        connection_timeout: Duration::from_secs(30),
        request_timeout: Duration::from_secs(60),
        max_request_size: 100 * 1024 * 1024,
        max_response_size: 100 * 1024 * 1024,
    };
    config.storage.hot_tier = HotTierConfig {
        base_path: storage_dir.path().to_path_buf(),
        max_size: 100 * 1024 * 1024,
        target_free_space_percent: 30,
        emergency_threshold_percent: 10,
    };
    config.storage.cold_tier.bucket = String::new();

    // Start host service
    println!("Starting host service...");
    let mut host_service = HostServiceBuilder::new()
        .config(config.clone())
        .build()
        .await?;
    host_service.start().await?;
    println!("Host service started");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create enclave service
    println!("Creating enclave service...");
    let master_key = MasterKey::generate();
    let enclave_service = EnclaveServiceBuilder::new()
        .config(config.clone())
        .master_key(master_key)
        .build()
        .await?;
    println!("Enclave service created");

    // Mount filesystem
    println!("Mounting filesystem...");
    let _fuse_handle = enclave_service.mount(&mount_path)?;
    println!("Mount returned");

    // Wait a bit
    println!("Waiting for mount to stabilize...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // First test: statfs (this was hanging)
    println!("Test 1: Getting filesystem stats (statfs)...");
    let mount_path_clone = mount_path.clone();
    let statfs_result = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || std::fs::metadata(&mount_path_clone)),
    )
    .await;

    match statfs_result {
        Ok(Ok(Ok(_))) => {
            println!("✓ statfs completed successfully");
        }
        Ok(Ok(Err(e))) => {
            println!("✗ statfs failed: {}", e);
        }
        Ok(Err(e)) => {
            println!("✗ statfs task panicked: {}", e);
        }
        Err(_) => {
            println!("✗ statfs timed out after 5 seconds");
            return Err(anyhow::anyhow!("statfs operation timed out"));
        }
    }

    // Second test: list directory
    println!("Test 2: Listing mount directory...");
    let mount_path_clone = mount_path.clone();
    let list_result = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || std::fs::read_dir(&mount_path_clone)),
    )
    .await;

    match list_result {
        Ok(Ok(Ok(entries))) => {
            let count = entries.count();
            println!("✓ Listed {} entries", count);
        }
        Ok(Ok(Err(e))) => {
            println!("✗ List failed: {}", e);
        }
        Ok(Err(e)) => {
            println!("✗ List task panicked: {}", e);
        }
        Err(_) => {
            println!("✗ List timed out after 5 seconds");
        }
    }

    // Clean up
    println!("Cleaning up...");
    host_service.stop().await?;

    Ok(())
}
