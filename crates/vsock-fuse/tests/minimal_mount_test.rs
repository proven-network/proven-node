//! Minimal test to debug mount hanging

#![allow(clippy::field_reassign_with_default)]

use proven_logger_macros::logged_tokio_test;
use proven_vsock_fuse::{
    config::{Config, HotTierConfig, RpcConfig},
    enclave::EnclaveServiceBuilder,
    encryption::MasterKey,
    host::HostServiceBuilder,
};
use std::time::Duration;
use tempfile::TempDir;

#[logged_tokio_test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Debug test"]
async fn test_minimal_mount() -> anyhow::Result<()> {
    // Disable AWS metadata service to avoid timeouts
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }

    eprintln!("=== Starting minimal mount test ===");

    // Create temporary directories
    let storage_dir = TempDir::new()?;
    let mount_dir = TempDir::new()?;
    let mount_path = mount_dir.path().to_path_buf();

    eprintln!("Storage dir: {:?}", storage_dir.path());
    eprintln!("Mount dir: {mount_path:?}");

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
    config.storage.cold_tier.bucket = String::new(); // Disable S3

    eprintln!("Config created, RPC port: {}", config.rpc.port);

    // Start host service
    eprintln!("Building host service...");
    let mut host_service = HostServiceBuilder::new()
        .config(config.clone())
        .build()
        .await?;

    eprintln!("Starting host service...");
    host_service.start().await?;
    eprintln!("Host service started");

    // Give server time to start
    eprintln!("Waiting for server to stabilize...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create master key for encryption
    let master_key = MasterKey::generate();

    // Create enclave service
    eprintln!("Building enclave service...");
    let enclave_service = EnclaveServiceBuilder::new()
        .config(config.clone())
        .master_key(master_key)
        .build()
        .await?;
    eprintln!("Enclave service built");

    // Mount filesystem with detailed timing
    eprintln!("=== Attempting mount ===");
    let mount_start = std::time::Instant::now();

    // Call mount in a timeout
    let mount_result = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            eprintln!("Calling mount from blocking task...");
            enclave_service.mount(&mount_path)
        }),
    )
    .await;

    match mount_result {
        Ok(Ok(Ok(_fuse_handle))) => {
            eprintln!("Mount succeeded after {:?}", mount_start.elapsed());
            eprintln!("=== Test passed ===");
        }
        Ok(Ok(Err(e))) => {
            eprintln!("Mount failed after {:?}: {:?}", mount_start.elapsed(), e);
            return Err(e.into());
        }
        Ok(Err(e)) => {
            eprintln!(
                "Mount task panicked after {:?}: {:?}",
                mount_start.elapsed(),
                e
            );
            return Err(anyhow::anyhow!("Mount task panicked: {:?}", e));
        }
        Err(_) => {
            eprintln!("Mount timed out after {:?}", mount_start.elapsed());
            eprintln!("=== MOUNT HANGING DETECTED ===");
            return Err(anyhow::anyhow!(
                "Mount operation timed out after 10 seconds"
            ));
        }
    }

    // Cleanup
    eprintln!("Stopping host service...");
    host_service.stop().await?;

    eprintln!("=== Minimal mount test finished ===");
    Ok(())
}
