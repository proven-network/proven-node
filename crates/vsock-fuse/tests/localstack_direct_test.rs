//! Direct test of S3 functionality with LocalStack
//! This test bypasses TieredStorage and tests ColdTier directly

#![allow(clippy::field_reassign_with_default)]

use std::time::Duration;

use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, retry::RetryConfig, timeout::TimeoutConfig,
};
use proven_vsock_fuse::{BlobId, host::storage::ColdTier};

/// Create S3 config for LocalStack
async fn create_s3_config(endpoint: &str) -> aws_sdk_s3::Config {
    use aws_config::SdkConfig;

    let creds = Credentials::new("test", "test", None, None, "test");
    let creds_provider = aws_sdk_s3::config::SharedCredentialsProvider::new(creds);

    // Create a base SDK config with our endpoint
    let sdk_config = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(creds_provider)
        .retry_config(RetryConfig::standard().with_max_attempts(3))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(5))
                .operation_timeout(Duration::from_secs(10))
                .build(),
        )
        .build();

    // Build S3 config from SDK config
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
    s3_config_builder.set_force_path_style(Some(true));
    s3_config_builder.build()
}

#[tokio::test]
#[ignore = "Requires LocalStack to be running"]
async fn test_direct_s3_connection() -> anyhow::Result<()> {
    // This test directly tests S3 connectivity
    println!("Testing direct S3 connection to LocalStack...");

    let endpoint = "http://localhost:4566";
    let s3_config = create_s3_config(endpoint).await;
    let client = aws_sdk_s3::Client::from_conf(s3_config);

    // Try to list buckets
    match client.list_buckets().send().await {
        Ok(resp) => {
            println!("Successfully connected to LocalStack!");
            let buckets = resp.buckets();
            println!("Found {} buckets", buckets.len());
        }
        Err(e) => {
            println!("Failed to connect to LocalStack: {e:?}");
            return Err(e.into());
        }
    }

    // Create test bucket
    let bucket = "test-bucket";
    match client.create_bucket().bucket(bucket).send().await {
        Ok(_) => println!("Created bucket: {bucket}"),
        Err(e) if e.to_string().contains("BucketAlreadyExists") => {
            println!("Bucket already exists: {bucket}");
        }
        Err(e) => return Err(e.into()),
    }

    // Test with ColdTier
    let cold_tier = ColdTier::with_client(
        bucket.to_string(),
        "test-prefix".to_string(),
        client.clone(),
    )
    .await?;

    // Store a blob
    let blob_id = BlobId::new();
    let test_data = vec![0x42; 1024]; // 1KB of data

    println!("Storing blob {blob_id:?}...");
    cold_tier.store(blob_id, test_data.clone()).await?;

    // Retrieve the blob
    println!("Retrieving blob {blob_id:?}...");
    let retrieved = cold_tier.get(&blob_id).await?;
    assert_eq!(retrieved, test_data);

    println!("Direct S3 test passed!");
    Ok(())
}
