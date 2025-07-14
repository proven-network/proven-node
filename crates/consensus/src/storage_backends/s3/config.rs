//! Configuration for S3 storage adaptor

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Main configuration for S3 storage adaptor
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3StorageConfig {
    /// S3 configuration
    pub s3: S3Config,

    /// WAL configuration
    pub wal: WalConfig,

    /// Cache configuration
    pub cache: CacheConfig,

    /// Batching configuration
    pub batch: BatchConfig,

    /// Performance tuning
    pub parallel_uploads: usize,

    /// Multipart upload threshold in bytes
    pub multipart_threshold: usize,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            s3: S3Config::default(),
            wal: WalConfig::default(),
            cache: CacheConfig::default(),
            batch: BatchConfig::default(),
            parallel_uploads: 4,
            multipart_threshold: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// S3-specific configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,

    /// AWS region
    pub region: String,

    /// Key prefix for all objects
    pub prefix: String,

    /// Use S3 One Zone storage class
    pub use_one_zone: bool,

    /// Request timeout
    pub request_timeout: Duration,

    /// Maximum retries
    pub max_retries: u32,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: "us-east-1".to_string(),
            prefix: "consensus".to_string(),
            use_one_zone: true,
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
        }
    }
}

/// WAL configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalConfig {
    /// VSOCK port for WAL server
    pub vsock_port: u32,

    /// Maximum pending data in MB
    pub max_pending_mb: usize,

    /// WAL sync interval
    pub sync_interval: Duration,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Request timeout
    pub request_timeout: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            vsock_port: 5000,
            max_pending_mb: 100,
            sync_interval: Duration::from_millis(100),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// Cache configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,

    /// Cache TTL in seconds
    pub ttl_seconds: u64,

    /// Enable bloom filters for existence checks
    pub use_bloom_filters: bool,

    /// Bloom filter expected items
    pub bloom_filter_items: usize,

    /// Bloom filter false positive rate
    pub bloom_filter_fp_rate: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 50 * 1024 * 1024, // 50MB
            ttl_seconds: 300,                 // 5 minutes
            use_bloom_filters: true,
            bloom_filter_items: 100_000,
            bloom_filter_fp_rate: 0.01,
        }
    }
}

/// Batching configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum batch size in bytes
    pub max_size_bytes: usize,

    /// Maximum batch time in milliseconds
    pub max_time_ms: u64,

    /// Maximum entries per batch
    pub max_entries: usize,

    /// Number of upload workers
    pub upload_workers: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 5 * 1024 * 1024, // 5MB
            max_time_ms: 100,
            max_entries: 1000,
            upload_workers: 2,
        }
    }
}
