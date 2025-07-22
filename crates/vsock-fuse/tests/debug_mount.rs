//! Debug mount test to isolate hanging issue

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
async fn test_debug_mount() -> anyhow::Result<()> {
    // Disable AWS metadata service to avoid timeouts
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }

    eprintln!("=== Starting debug mount test ===");

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
    tokio::time::sleep(Duration::from_secs(1)).await;

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

    // Mount filesystem
    eprintln!("Attempting to mount at {mount_path:?}");
    eprintln!("Mount path exists before mount: {}", mount_path.exists());

    let mount_start = std::time::Instant::now();
    let _fuse_handle = match enclave_service.mount(&mount_path) {
        Ok(handle) => {
            eprintln!("Mount call succeeded after {:?}", mount_start.elapsed());
            handle
        }
        Err(e) => {
            eprintln!(
                "Mount call failed after {:?}: {:?}",
                mount_start.elapsed(),
                e
            );
            return Err(e.into());
        }
    };
    eprintln!("Mount call returned");

    // Check if mount succeeded
    for i in 0..10 {
        eprintln!("Checking mount status, attempt {}", i + 1);

        // First check if mount path still exists
        if !mount_path.exists() {
            eprintln!("Mount path no longer exists!");
            break;
        }

        match std::fs::metadata(&mount_path) {
            Ok(metadata) => {
                eprintln!(
                    "Mount point metadata: is_dir={}, permissions={:?}",
                    metadata.is_dir(),
                    metadata.permissions()
                );

                // Try to list directory
                match std::fs::read_dir(&mount_path) {
                    Ok(entries) => {
                        let entry_count = entries.count();
                        eprintln!("Successfully read directory, {entry_count} entries");

                        // Try to create a test file
                        let test_file = mount_path.join("test.txt");
                        eprintln!("Attempting to create test file: {test_file:?}");
                        match std::fs::write(&test_file, b"Hello FUSE") {
                            Ok(_) => {
                                eprintln!("Successfully created test file");
                                match std::fs::read(&test_file) {
                                    Ok(content) => {
                                        eprintln!(
                                            "Read test file: {:?}",
                                            String::from_utf8_lossy(&content)
                                        );
                                        eprintln!("FUSE mount is fully functional!");
                                        return Ok(());
                                    }
                                    Err(e) => eprintln!("Failed to read test file: {e}"),
                                }
                            }
                            Err(e) => eprintln!("Failed to create test file: {e}"),
                        }
                        break;
                    }
                    Err(e) => {
                        eprintln!("Failed to read directory: {} (kind: {:?})", e, e.kind());
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to get metadata: {} (kind: {:?})", e, e.kind());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    eprintln!("Test complete, cleaning up...");

    // Cleanup
    drop(_fuse_handle);
    host_service.stop().await?;

    eprintln!("=== Debug mount test finished ===");
    Ok(())
}
