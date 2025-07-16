//! Tests for host storage implementation

use std::sync::Arc;
use tempfile::TempDir;

use proven_vsock_fuse::{
    BlobId, BlobType, FileId, StorageTier, TierHint,
    config::{
        CacheConfig, ColdTierConfig, Config, EncryptionConfig, FilesystemConfig, HotTierConfig,
        KeyDerivationConfig, RpcConfig, SecurityConfig, SizeBasedTieringConfig, StorageConfig,
        TieringConfig, TimeBasedTieringConfig,
    },
    host::TieredStorage,
    storage::BlobStorage,
};
use std::time::Duration;

fn create_test_config(hot_dir: &std::path::Path) -> Config {
    Config {
        filesystem: FilesystemConfig {
            max_file_size: 100 * 1024 * 1024,      // 100MB
            max_total_storage: 1024 * 1024 * 1024, // 1GB
            block_size: 4096,
            database_direct_io: false,
            database_patterns: vec![],
        },
        encryption: EncryptionConfig {
            algorithm: "AES-256-GCM".to_string(),
            kdf: KeyDerivationConfig {
                algorithm: "Argon2id".to_string(),
                memory_cost: 65536,
                time_cost: 3,
                parallelism: 4,
            },
            encrypt_metadata: true,
            encrypt_filenames: true,
        },
        storage: StorageConfig {
            hot_tier: HotTierConfig {
                base_path: hot_dir.to_path_buf(),
                max_size: 10 * 1024 * 1024, // 10MB
                target_free_space_percent: 20,
                emergency_threshold_percent: 10,
            },
            cold_tier: ColdTierConfig {
                bucket: "test-bucket".to_string(),
                prefix: "test-prefix".to_string(),
                storage_class: "STANDARD".to_string(),
                compression_enabled: false,
                compression_algorithm: "zstd".to_string(),
                compression_level: 3,
                chunk_size: 5 * 1024 * 1024,
            },
            tiering: TieringConfig {
                time_based: TimeBasedTieringConfig {
                    cold_after: Duration::from_secs(86400),            // 1 day
                    hot_if_accessed_within: Duration::from_secs(3600), // 1 hour
                },
                size_based: SizeBasedTieringConfig {
                    large_file_threshold: 50 * 1024 * 1024,           // 50MB
                    large_file_cold_after: Duration::from_secs(3600), // 1 hour
                },
                batch_size: 10,
                max_concurrent_migrations: 2,
                bandwidth_limit_mbps: None,
            },
        },
        rpc: RpcConfig {
            host_cid: 2,
            port: 8000,
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_request_size: 10 * 1024 * 1024,
            max_response_size: 10 * 1024 * 1024,
        },
        cache: CacheConfig {
            metadata_cache_size: 10 * 1024 * 1024, // 10MB
            data_cache_size: 100 * 1024 * 1024,    // 100MB
            attr_timeout: Duration::from_secs(1),
            entry_timeout: Duration::from_secs(1),
            read_ahead_enabled: false,
            read_ahead_blocks: 4,
        },
        security: SecurityConfig {
            require_attestation: false,
            attestation_max_age: Duration::from_secs(3600),
            sandbox_enabled: false,
            sandbox_root: hot_dir.join("sandbox"),
            audit_logging: false,
            rate_limit_rps: None,
        },
    }
}

#[tokio::test]
async fn test_tiered_storage_basic_operations() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(temp_dir.path());

    let storage = TieredStorage::new(
        config.storage.hot_tier.base_path.clone(),
        config.storage.cold_tier.bucket.clone(),
        config.storage.cold_tier.prefix.clone(),
    )
    .await
    .unwrap();

    // Test storing a blob
    let blob_id = BlobId::new();
    let data = b"Hello, tiered storage!".to_vec();

    storage
        .store_blob(blob_id, data.clone(), TierHint::PreferHot)
        .await
        .unwrap();

    // Test retrieving the blob
    let retrieved = storage.get_blob(blob_id).await.unwrap();
    assert_eq!(retrieved.data, data);
    assert_eq!(retrieved.tier, StorageTier::Hot);

    // Test listing blobs
    let blobs = storage.list_blobs(None).await.unwrap();
    assert_eq!(blobs.len(), 1);
    assert_eq!(blobs[0].blob_id, blob_id);
    assert_eq!(blobs[0].tier, StorageTier::Hot);

    // Test deleting the blob
    storage.delete_blob(blob_id).await.unwrap();

    // Verify it's gone
    let result = storage.get_blob(blob_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_tiered_storage_large_blobs() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(temp_dir.path());

    let storage = TieredStorage::new(
        config.storage.hot_tier.base_path.clone(),
        config.storage.cold_tier.bucket.clone(),
        config.storage.cold_tier.prefix.clone(),
    )
    .await
    .unwrap();

    // Create a large blob (1MB)
    let blob_id = BlobId::new();
    let data = vec![0xAB; 1024 * 1024];

    storage
        .store_blob(blob_id, data.clone(), TierHint::PreferHot)
        .await
        .unwrap();

    // Retrieve and verify
    let retrieved = storage.get_blob(blob_id).await.unwrap();
    assert_eq!(retrieved.data.len(), 1024 * 1024);
    assert_eq!(retrieved.data[0], 0xAB);
    assert_eq!(retrieved.data[1024 * 1024 - 1], 0xAB);
}

#[tokio::test]
async fn test_tiered_storage_concurrent_access() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(temp_dir.path());

    let storage = Arc::new(
        TieredStorage::new(
            config.storage.hot_tier.base_path.clone(),
            config.storage.cold_tier.bucket.clone(),
            config.storage.cold_tier.prefix.clone(),
        )
        .await
        .unwrap(),
    );

    // Spawn multiple tasks to store blobs concurrently
    let mut handles = vec![];

    for i in 0..10 {
        let storage = storage.clone();
        let handle = tokio::spawn(async move {
            let blob_id = BlobId::new();
            let data = format!("Concurrent blob {i}").into_bytes();
            storage
                .store_blob(blob_id, data.clone(), TierHint::PreferHot)
                .await
                .unwrap();
            (blob_id, data)
        });
        handles.push(handle);
    }

    // Collect all blob IDs and data
    let mut blobs = vec![];
    for handle in handles {
        let (blob_id, data) = handle.await.unwrap();
        blobs.push((blob_id, data));
    }

    // Verify all blobs can be retrieved
    for (blob_id, expected_data) in blobs {
        let retrieved = storage.get_blob(blob_id).await.unwrap();
        assert_eq!(retrieved.data, expected_data);
    }
}

#[tokio::test]
async fn test_storage_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(temp_dir.path());

    let storage = TieredStorage::new(
        config.storage.hot_tier.base_path.clone(),
        config.storage.cold_tier.bucket.clone(),
        config.storage.cold_tier.prefix.clone(),
    )
    .await
    .unwrap();

    // Get initial stats
    let initial_stats = storage.get_stats().await.unwrap();
    assert_eq!(initial_stats.hot_tier.used_bytes, 0);
    assert_eq!(initial_stats.hot_tier.file_count, 0);

    // Store some blobs
    let mut total_size = 0u64;
    for i in 0..5 {
        let blob_id = BlobId::new();
        let data = vec![i as u8; 1024 * (i + 1)]; // Varying sizes
        total_size += data.len() as u64;
        storage
            .store_blob(blob_id, data, TierHint::PreferHot)
            .await
            .unwrap();
    }

    // Check updated stats
    let updated_stats = storage.get_stats().await.unwrap();
    assert_eq!(updated_stats.hot_tier.file_count, 5);
    // Used bytes should be at least the total size (might be more due to filesystem overhead)
    assert!(updated_stats.hot_tier.used_bytes >= total_size);
}

#[tokio::test]
async fn test_blob_listing_with_prefix() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(temp_dir.path());

    let storage = TieredStorage::new(
        config.storage.hot_tier.base_path.clone(),
        config.storage.cold_tier.bucket.clone(),
        config.storage.cold_tier.prefix.clone(),
    )
    .await
    .unwrap();

    // Store blobs with different prefixes
    let prefix1 = vec![0x01, 0x02];
    let prefix2 = vec![0x03, 0x04];

    // Store blobs with prefix1
    for i in 0..3 {
        let mut blob_bytes = [0u8; 32];
        blob_bytes[0..2].copy_from_slice(&prefix1);
        blob_bytes[2..].fill(i);
        let file_id = FileId::from_bytes(blob_bytes);
        let blob_id = BlobId::from_file_id(&file_id, BlobType::Data);
        let data = format!("Blob with prefix1: {i}").into_bytes();
        storage
            .store_blob(blob_id, data, TierHint::PreferHot)
            .await
            .unwrap();
    }

    // Store blobs with prefix2
    for i in 0..2 {
        let mut blob_bytes = [0u8; 32];
        blob_bytes[0..2].copy_from_slice(&prefix2);
        blob_bytes[2..].fill(i);
        let file_id = FileId::from_bytes(blob_bytes);
        let blob_id = BlobId::from_file_id(&file_id, BlobType::Data);
        let data = format!("Blob with prefix2: {i}").into_bytes();
        storage
            .store_blob(blob_id, data, TierHint::PreferHot)
            .await
            .unwrap();
    }

    // List all blobs
    let all_blobs = storage.list_blobs(None).await.unwrap();
    assert_eq!(all_blobs.len(), 5);

    // List blobs with prefix1
    let prefix1_blobs = storage.list_blobs(Some(&prefix1)).await.unwrap();
    assert_eq!(prefix1_blobs.len(), 3);

    // List blobs with prefix2
    let prefix2_blobs = storage.list_blobs(Some(&prefix2)).await.unwrap();
    assert_eq!(prefix2_blobs.len(), 2);
}

#[tokio::test]
async fn test_storage_error_handling() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(temp_dir.path());

    let storage = TieredStorage::new(
        config.storage.hot_tier.base_path.clone(),
        config.storage.cold_tier.bucket.clone(),
        config.storage.cold_tier.prefix.clone(),
    )
    .await
    .unwrap();

    // Test getting non-existent blob
    let blob_id = BlobId::new();
    let result = storage.get_blob(blob_id).await;
    assert!(result.is_err());

    // Test deleting non-existent blob (should succeed)
    storage.delete_blob(blob_id).await.unwrap();

    // Test storing blob, then trying to overwrite
    let data1 = b"Original data".to_vec();
    let data2 = b"New data".to_vec();

    storage
        .store_blob(blob_id, data1.clone(), TierHint::PreferHot)
        .await
        .unwrap();
    storage
        .store_blob(blob_id, data2.clone(), TierHint::PreferHot)
        .await
        .unwrap();

    // Should have the new data
    let retrieved = storage.get_blob(blob_id).await.unwrap();
    assert_eq!(retrieved.data, data2);
}

#[tokio::test]
async fn test_tier_hints() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(temp_dir.path());

    let storage = TieredStorage::new(
        config.storage.hot_tier.base_path.clone(),
        config.storage.cold_tier.bucket.clone(),
        config.storage.cold_tier.prefix.clone(),
    )
    .await
    .unwrap();

    // Store blob with hot tier hint
    let hot_blob_id = BlobId::new();
    let hot_data = b"Hot data".to_vec();
    storage
        .store_blob(hot_blob_id, hot_data.clone(), TierHint::PreferHot)
        .await
        .unwrap();

    // Store blob with cold tier hint (should still go to hot since cold isn't configured)
    let cold_blob_id = BlobId::new();
    let cold_data = b"Cold data".to_vec();
    storage
        .store_blob(cold_blob_id, cold_data.clone(), TierHint::PreferCold)
        .await
        .unwrap();

    // Both should be in hot tier
    let hot_retrieved = storage.get_blob(hot_blob_id).await.unwrap();
    assert_eq!(hot_retrieved.tier, StorageTier::Hot);

    let cold_retrieved = storage.get_blob(cold_blob_id).await.unwrap();
    assert_eq!(cold_retrieved.tier, StorageTier::Hot);
}
