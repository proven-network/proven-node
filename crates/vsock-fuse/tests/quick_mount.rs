//! Quick test to check FUSE mount

#![allow(clippy::field_reassign_with_default)]

use proven_logger_macros::logged_tokio_test;
use proven_vsock_fuse::{
    config::{Config, HotTierConfig, RpcConfig},
    enclave::EnclaveServiceBuilder,
    encryption::MasterKey,
    host::HostServiceBuilder,
};
use std::{fs, time::Duration};
use tempfile::TempDir;

#[logged_tokio_test(flavor = "multi_thread", worker_threads = 4)]
async fn test_quick_mount() -> anyhow::Result<()> {
    println!("Starting quick mount test...");

    // Create temporary directories
    let storage_dir = TempDir::new()?;
    let mount_dir = TempDir::new()?;

    // Setup config
    let mut config = Config::default();
    config.rpc = RpcConfig {
        host_cid: 3,
        port: 45557,
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create enclave service
    println!("Creating enclave service...");
    let master_key = MasterKey::generate();
    let enclave_service = EnclaveServiceBuilder::new()
        .config(config.clone())
        .master_key(master_key)
        .build()
        .await?;

    // Mount filesystem
    println!("Mounting filesystem at {:?}...", mount_dir.path());
    let _fuse_handle = enclave_service.mount(mount_dir.path())?;

    // Wait for mount
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Try to list directory
    println!("Listing mount directory...");
    match fs::read_dir(mount_dir.path()) {
        Ok(entries) => {
            let count = entries.count();
            println!("SUCCESS! Found {count} entries");
        }
        Err(e) => println!("ERROR listing directory: {e}"),
    }

    // Clean up
    println!("Cleaning up...");
    host_service.stop().await?;

    Ok(())
}
