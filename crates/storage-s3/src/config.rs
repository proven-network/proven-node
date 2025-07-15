//! Configuration for S3 storage backend

use std::time::Duration;

/// Complete S3 storage configuration
#[derive(Clone, Debug)]
pub struct S3StorageConfig {
    /// S3-specific configuration
    pub s3: S3Config,

    /// WAL configuration for durability
    pub wal: WalConfig,

    /// Cache configuration for performance
    pub cache: CacheConfig,

    /// Batching configuration
    pub batch: BatchConfig,

    /// Number of parallel upload workers
    pub parallel_uploads: usize,

    /// Threshold for using multipart upload (in bytes)
    pub multipart_threshold: usize,

    /// Enable compression for entries larger than this size
    pub compression_threshold: usize,

    /// Encryption configuration
    pub encryption: Option<EncryptionConfig>,
}

/// S3-specific configuration
#[derive(Clone, Debug)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,

    /// AWS region (if not using default)
    pub region: Option<String>,

    /// Optional prefix for all keys
    pub prefix: Option<String>,

    /// Always use S3 One Zone storage class for cost optimization
    pub use_one_zone: bool,

    /// Request timeout
    pub request_timeout: Duration,

    /// Maximum retry attempts
    pub max_retries: u32,
}

/// WAL configuration for durability
#[derive(Clone, Debug)]
pub struct WalConfig {
    /// VSOCK port for WAL communication
    pub vsock_port: u32,

    /// Maximum pending data in MB before blocking
    pub max_pending_mb: usize,

    /// Sync interval for WAL flushes
    pub sync_interval: Duration,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Request timeout
    pub request_timeout: Duration,
}

/// Cache configuration
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,

    /// TTL for cached entries in seconds
    pub ttl_seconds: u64,

    /// Enable bloom filters for existence checks
    pub enable_bloom_filters: bool,

    /// Expected number of entries for bloom filter sizing
    pub bloom_filter_capacity: usize,

    /// False positive rate for bloom filters
    pub bloom_filter_fpr: f64,
}

/// Batching configuration
#[derive(Clone, Debug)]
pub struct BatchConfig {
    /// Maximum batch size in bytes
    pub max_size_bytes: usize,

    /// Maximum time to wait before flushing batch (ms)
    pub max_time_ms: u64,

    /// Maximum number of entries per batch
    pub max_entries: usize,

    /// Number of upload workers
    pub upload_workers: usize,
}

/// Encryption configuration
#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    /// Encryption key for S3 data (base64 encoded)
    pub s3_key: String,

    /// Encryption key for WAL data (base64 encoded)
    pub wal_key: String,

    /// Enable key rotation
    pub enable_rotation: bool,

    /// Key rotation interval
    pub rotation_interval: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            vsock_port: 5000,
            max_pending_mb: 100,
            sync_interval: Duration::from_millis(100),
            connection_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            s3: S3Config {
                bucket: String::new(),
                region: None,
                prefix: None,
                use_one_zone: true,
                request_timeout: Duration::from_secs(30),
                max_retries: 3,
            },
            wal: WalConfig::default(),
            cache: CacheConfig {
                max_size_bytes: 50 * 1024 * 1024, // 50MB
                ttl_seconds: 300,                 // 5 minutes
                enable_bloom_filters: true,
                bloom_filter_capacity: 1_000_000,
                bloom_filter_fpr: 0.01,
            },
            batch: BatchConfig {
                max_size_bytes: 5 * 1024 * 1024, // 5MB
                max_time_ms: 100,
                max_entries: 1000,
                upload_workers: 2,
            },
            parallel_uploads: 4,
            multipart_threshold: 10 * 1024 * 1024, // 10MB
            compression_threshold: 1024,           // 1KB
            encryption: None,
        }
    }
}

/// Builder for S3StorageConfig
pub struct S3StorageConfigBuilder {
    config: S3StorageConfig,
}

impl S3StorageConfigBuilder {
    /// Create a new builder with the given bucket
    pub fn new(bucket: impl Into<String>) -> Self {
        let mut config = S3StorageConfig::default();
        config.s3.bucket = bucket.into();
        Self { config }
    }

    /// Set the S3 prefix
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.s3.prefix = Some(prefix.into());
        self
    }

    /// Enable batching with custom settings
    pub fn batching(mut self, max_size_mb: usize, max_time_ms: u64, max_entries: usize) -> Self {
        self.config.batch.max_size_bytes = max_size_mb * 1024 * 1024;
        self.config.batch.max_time_ms = max_time_ms;
        self.config.batch.max_entries = max_entries;
        self
    }

    /// Enable caching with custom settings
    pub fn caching(mut self, max_size_mb: usize, ttl_seconds: u64) -> Self {
        self.config.cache.max_size_bytes = max_size_mb * 1024 * 1024;
        self.config.cache.ttl_seconds = ttl_seconds;
        self
    }

    /// Enable encryption
    pub fn encryption(mut self, s3_key: impl Into<String>, wal_key: impl Into<String>) -> Self {
        self.config.encryption = Some(EncryptionConfig {
            s3_key: s3_key.into(),
            wal_key: wal_key.into(),
            enable_rotation: false,
            rotation_interval: Duration::from_secs(86400 * 30), // 30 days
        });
        self
    }

    /// Set WAL configuration
    pub fn wal(mut self, vsock_port: u32, max_pending_mb: usize) -> Self {
        self.config.wal.vsock_port = vsock_port;
        self.config.wal.max_pending_mb = max_pending_mb;
        self
    }

    /// Build the configuration
    pub fn build(self) -> S3StorageConfig {
        self.config
    }
}
