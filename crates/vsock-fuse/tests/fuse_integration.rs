//! FUSE integration test for vsock-fuse
//!
//! This test verifies end-to-end functionality through FUSE mount

#![allow(clippy::field_reassign_with_default)]

use proven_vsock_fuse::{
    config::{Config, HotTierConfig, RpcConfig},
    enclave::EnclaveServiceBuilder,
    encryption::MasterKey,
    host::HostServiceBuilder,
};
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::Path,
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::oneshot;

/// Test configuration
const MOUNT_CHECK_RETRIES: u32 = 20;
const MOUNT_CHECK_DELAY_MS: u64 = 100;

#[tokio::test]
#[ignore = "Requires FUSE to be installed and may need elevated permissions"]
async fn test_fuse_filesystem() -> anyhow::Result<()> {
    // Disable AWS metadata service to avoid timeouts
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }

    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .try_init();

    // Create temporary directories
    let storage_dir = TempDir::new()?;
    let mount_dir = TempDir::new()?;
    let mount_path = mount_dir.path().to_path_buf();

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

    // Start host service
    let mut host_service = HostServiceBuilder::new()
        .config(config.clone())
        .build()
        .await?;
    host_service.start().await?;

    // Give server time to start
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create master key for encryption
    let master_key = MasterKey::generate();

    // Create enclave service
    let enclave_service = EnclaveServiceBuilder::new()
        .config(config.clone())
        .master_key(master_key)
        .build()
        .await?;

    // Setup shutdown channel
    let (shutdown_tx, _shutdown_rx) = oneshot::channel();

    // Mount filesystem
    tracing::info!("Mounting FUSE filesystem at {:?}", mount_path);
    let _fuse_handle = match enclave_service.mount(&mount_path) {
        Ok(handle) => {
            tracing::info!("Mount call succeeded");
            handle
        }
        Err(e) => {
            tracing::error!("Mount call failed: {:?}", e);
            return Err(e.into());
        }
    };

    tracing::info!("Giving mount time to initialize...");
    std::thread::sleep(std::time::Duration::from_millis(1000));

    // Wait for mount to complete
    wait_for_mount(&mount_path).await?;

    // Run filesystem tests
    let result = run_filesystem_tests(&mount_path).await;

    // Shutdown services (FuseHandle will unmount on drop)
    let _ = shutdown_tx.send(());
    host_service.stop().await?;

    // Check test result
    result?;

    Ok(())
}

/// Wait for the filesystem to be mounted
async fn wait_for_mount(mount_path: &Path) -> anyhow::Result<()> {
    tracing::info!("Waiting for mount at {:?}", mount_path);
    for i in 0..MOUNT_CHECK_RETRIES {
        // Check if we can stat the mount point
        match tokio::fs::metadata(mount_path).await {
            Ok(metadata) if metadata.is_dir() => {
                tracing::debug!("Mount point exists and is directory at attempt {}", i + 1);
                // Try to list directory to ensure it's really mounted
                match tokio::fs::read_dir(mount_path).await {
                    Ok(_) => {
                        tracing::info!("Mount point is ready");
                        return Ok(());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // This might mean it's mounted but empty, which is OK
                        tracing::info!("Mount point is ready (empty)");
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::debug!("Mount check {}: {} (kind: {:?})", i + 1, e, e.kind());
                    }
                }
            }
            Ok(_) => {
                tracing::debug!("Mount point is ready (empty)");
                return Ok(());
            }
            Err(e) => {
                tracing::debug!(
                    "Mount not ready yet ({}): {} (kind: {:?})",
                    i + 1,
                    e,
                    e.kind()
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(MOUNT_CHECK_DELAY_MS)).await;
    }

    Err(anyhow::anyhow!(
        "Filesystem failed to mount after {} retries",
        MOUNT_CHECK_RETRIES
    ))
}

/// Run filesystem operations tests
async fn run_filesystem_tests(mount_path: &Path) -> anyhow::Result<()> {
    tracing::info!("Running filesystem tests");

    // Test 1: Create and write to a file
    let test_file = mount_path.join("test.txt");
    let test_data = b"Hello from FUSE!";

    tracing::info!("Test 1: Writing file");
    let mut file = File::create(&test_file)?;
    file.write_all(test_data)?;
    file.sync_all()?;
    drop(file);

    // Test 2: Read the file back
    tracing::info!("Test 2: Reading file");
    let mut file = File::open(&test_file)?;
    let mut read_data = Vec::new();
    file.read_to_end(&mut read_data)?;
    assert_eq!(read_data, test_data);
    tracing::info!("✓ Read data matches written data");

    // Test 3: Get file metadata
    tracing::info!("Test 3: Getting file metadata");
    let metadata = fs::metadata(&test_file)?;
    assert_eq!(metadata.len(), test_data.len() as u64);
    assert!(metadata.is_file());
    tracing::info!("✓ File metadata is correct");

    // Test 4: Create a directory
    tracing::info!("Test 4: Creating directory");
    let test_dir = mount_path.join("testdir");
    fs::create_dir(&test_dir)?;
    assert!(test_dir.exists());
    assert!(fs::metadata(&test_dir)?.is_dir());
    tracing::info!("✓ Directory created successfully");

    // Test 5: Create file in subdirectory
    tracing::info!("Test 5: Creating file in subdirectory");
    let nested_file = test_dir.join("nested.txt");
    fs::write(&nested_file, b"Nested file content")?;
    assert_eq!(fs::read(&nested_file)?, b"Nested file content");
    tracing::info!("✓ Nested file operations work");

    // Test 6: List directory contents
    tracing::info!("Test 6: Listing directory contents");
    let entries: Vec<_> = fs::read_dir(mount_path)?
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .collect();
    assert!(entries.iter().any(|name| name == "test.txt"));
    assert!(entries.iter().any(|name| name == "testdir"));
    tracing::info!("✓ Directory listing works");

    // Test 7: Rename file
    tracing::info!("Test 7: Renaming file");
    let renamed_file = mount_path.join("renamed.txt");
    fs::rename(&test_file, &renamed_file)?;
    assert!(!test_file.exists());
    assert!(renamed_file.exists());
    assert_eq!(fs::read(&renamed_file)?, test_data);
    tracing::info!("✓ File rename works");

    // Test 8: Delete file
    tracing::info!("Test 8: Deleting file");
    fs::remove_file(&renamed_file)?;
    assert!(!renamed_file.exists());
    tracing::info!("✓ File deletion works");

    // Test 9: Remove directory (should fail if not empty)
    tracing::info!("Test 9: Testing directory removal");
    match fs::remove_dir(&test_dir) {
        Err(e) => {
            tracing::info!("✓ Directory removal correctly failed (not empty): {:?}", e);
        }
        Ok(_) => panic!("Expected directory removal to fail when directory is not empty"),
    }

    // Test 10: Remove nested file and then directory
    tracing::info!("Test 10: Removing nested file and directory");
    fs::remove_file(&nested_file)?;
    fs::remove_dir(&test_dir)?;
    assert!(!test_dir.exists());
    tracing::info!("✓ Directory removal works");

    // Test 11: Large file operations
    tracing::info!("Test 11: Testing large file operations");
    let large_file = mount_path.join("large.bin");
    let large_data = vec![0xAB; 1024 * 1024]; // 1MB
    fs::write(&large_file, &large_data)?;
    let read_large = fs::read(&large_file)?;
    assert_eq!(read_large.len(), large_data.len());
    assert_eq!(read_large, large_data);
    fs::remove_file(&large_file)?;
    tracing::info!("✓ Large file operations work");

    // Test 12: Multiple concurrent operations
    tracing::info!("Test 12: Testing concurrent operations");
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let mount_path = mount_path.to_path_buf();
            tokio::spawn(async move {
                let file_path = mount_path.join(format!("concurrent_{i}.txt"));
                tokio::fs::write(&file_path, format!("Content {i}")).await?;
                let content = tokio::fs::read_to_string(&file_path).await?;
                tokio::fs::remove_file(&file_path).await?;
                Ok::<String, anyhow::Error>(content)
            })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        let content = handle.await??;
        assert_eq!(content, format!("Content {i}"));
    }
    tracing::info!("✓ Concurrent operations work");

    tracing::info!("All filesystem tests passed!");
    Ok(())
}

#[tokio::test]
#[ignore] // This test requires proper permissions
async fn test_fuse_permissions() -> anyhow::Result<()> {
    // This test would verify file permissions, ownership, etc.
    // Requires running with appropriate privileges
    Ok(())
}
