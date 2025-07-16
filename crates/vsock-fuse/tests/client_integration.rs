//! Client integration test for vsock-fuse
//!
//! This test verifies the client-side storage functionality

#![allow(clippy::field_reassign_with_default)]

use proven_vsock_fuse::{
    BlobId, TierHint,
    config::{Config, HotTierConfig, RpcConfig},
    enclave::client::EnclaveStorageClient,
    host::HostServiceBuilder,
    storage::BlobStorage,
};
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;

#[tokio::test]
async fn test_client_storage_operations() -> anyhow::Result<()> {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .try_init();

    // Create temporary directories
    let storage_dir = TempDir::new()?;

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
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create storage client
    let rpc_client = proven_vsock_rpc::RpcClient::builder()
        .vsock_addr(
            #[cfg(target_os = "linux")]
            tokio_vsock::VsockAddr::new(config.rpc.host_cid, config.rpc.port),
            #[cfg(not(target_os = "linux"))]
            std::net::SocketAddr::from(([127, 0, 0, 1], config.rpc.port as u16)),
        )
        .pool_size(1)
        .default_timeout(Duration::from_secs(5))
        .build()?;

    let storage_client = Arc::new(EnclaveStorageClient::from_client(Arc::new(rpc_client)));

    // Test 1: Store a data blob
    tracing::info!("Test 1: Storing data blob");
    let blob_id = BlobId([42; 32]);
    let test_data = b"Hello from client test!".to_vec();
    storage_client
        .store_blob(blob_id, test_data.clone(), TierHint::PreferHot)
        .await?;
    tracing::info!("✓ Data blob stored");

    // Test 2: Retrieve data blob
    tracing::info!("Test 2: Retrieving data blob");
    let retrieved_blob = storage_client.get_blob(blob_id).await?;
    assert_eq!(retrieved_blob.data, test_data);
    tracing::info!("✓ Data blob retrieved correctly");

    // Test 3: Store multiple blobs
    tracing::info!("Test 3: Storing multiple blobs");
    let mut blob_ids = Vec::new();
    for i in 0..5 {
        let blob_id = BlobId([i; 32]);
        let data = format!("Test blob {i}").into_bytes();
        storage_client
            .store_blob(blob_id, data, TierHint::PreferHot)
            .await?;
        blob_ids.push(blob_id);
    }
    tracing::info!("✓ Multiple blobs stored");

    // Test 4: List blobs
    tracing::info!("Test 4: Listing blobs");
    let all_blobs = storage_client.list_blobs(None).await?;
    assert!(all_blobs.len() >= 6); // At least our test blobs
    tracing::info!("✓ Found {} blobs", all_blobs.len());

    // Test 5: List blobs with prefix
    tracing::info!("Test 5: Listing blobs with prefix");
    let prefix_blobs = storage_client.list_blobs(Some(&[0])).await?;
    assert!(prefix_blobs.iter().any(|info| info.blob_id == blob_ids[0]));
    tracing::info!("✓ Prefix filtering works");

    // Test 6: Get blob info
    tracing::info!("Test 6: Checking blob info");
    let blob_info = all_blobs
        .iter()
        .find(|info| info.blob_id == blob_id)
        .expect("Should find our test blob");
    assert_eq!(blob_info.size, test_data.len() as u64);
    tracing::info!("✓ Blob info is correct");

    // Test 7: Storage stats
    tracing::info!("Test 7: Getting storage stats");
    let stats = storage_client.get_stats().await?;
    assert!(stats.hot_tier.used_bytes > 0);
    assert!(stats.hot_tier.file_count >= 6);
    tracing::info!(
        "✓ Storage stats: {} bytes used, {} files",
        stats.hot_tier.used_bytes,
        stats.hot_tier.file_count
    );

    // Test 8: Delete blob
    tracing::info!("Test 8: Deleting blob");
    storage_client.delete_blob(blob_id).await?;

    // Verify deletion
    match storage_client.get_blob(blob_id).await {
        Err(_) => tracing::info!("✓ Blob successfully deleted"),
        Ok(_) => panic!("Blob should have been deleted"),
    }

    // Test 9: Large blob operations
    tracing::info!("Test 9: Testing large blob operations");
    let large_blob_id = BlobId([99; 32]);
    let large_data = vec![0xAB; 1024 * 1024]; // 1MB
    storage_client
        .store_blob(large_blob_id, large_data.clone(), TierHint::PreferHot)
        .await?;

    let retrieved_large = storage_client.get_blob(large_blob_id).await?;
    assert_eq!(retrieved_large.data.len(), large_data.len());
    storage_client.delete_blob(large_blob_id).await?;
    tracing::info!("✓ Large blob operations work");

    // Test 10: Concurrent operations
    tracing::info!("Test 10: Testing concurrent operations");
    let client = storage_client.clone();
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move {
                let blob_id = BlobId([100 + i; 32]);
                let data = format!("Concurrent blob {i}").into_bytes();
                client
                    .store_blob(blob_id, data.clone(), TierHint::PreferHot)
                    .await?;
                let retrieved = client.get_blob(blob_id).await?;
                client.delete_blob(blob_id).await?;
                Ok::<Vec<u8>, anyhow::Error>(retrieved.data)
            })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        let data = handle.await??;
        assert_eq!(data, format!("Concurrent blob {i}").into_bytes());
    }
    tracing::info!("✓ Concurrent operations work");

    // Cleanup
    tracing::info!("Cleaning up remaining test blobs");
    for blob_id in blob_ids {
        let _ = storage_client.delete_blob(blob_id).await;
    }

    // Shutdown
    host_service.stop().await?;

    tracing::info!("\n✓ Client storage integration test completed successfully!");
    Ok(())
}
