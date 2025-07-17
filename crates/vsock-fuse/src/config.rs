//! Configuration structures for VSOCK-FUSE

use crate::error::{Result, VsockFuseError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration for VSOCK-FUSE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Filesystem configuration
    pub filesystem: FilesystemConfig,

    /// Encryption configuration
    pub encryption: EncryptionConfig,

    /// Storage configuration
    pub storage: StorageConfig,

    /// RPC configuration
    pub rpc: RpcConfig,

    /// Cache configuration
    pub cache: CacheConfig,

    /// Security configuration
    pub security: SecurityConfig,
}

/// Filesystem-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    /// Maximum file size allowed
    pub max_file_size: u64,

    /// Maximum total storage allowed
    pub max_total_storage: u64,

    /// Block size for encryption (should match page size)
    pub block_size: usize,

    /// Enable direct I/O for database files
    pub database_direct_io: bool,

    /// File patterns to treat as database files
    pub database_patterns: Vec<String>,
}

/// Encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Encryption algorithm (currently only AES-256-GCM)
    pub algorithm: String,

    /// Key derivation function
    pub kdf: KeyDerivationConfig,

    /// Enable metadata encryption
    pub encrypt_metadata: bool,

    /// Enable filename encryption
    pub encrypt_filenames: bool,
}

/// Key derivation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDerivationConfig {
    /// Algorithm (SHA256 for performance)
    pub algorithm: String,

    /// Memory cost in KB (legacy, not used with SHA256)
    pub memory_cost: u32,

    /// Time cost (legacy, not used with SHA256)
    pub time_cost: u32,

    /// Parallelism degree (legacy, not used with SHA256)
    pub parallelism: u32,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Hot tier configuration
    pub hot_tier: HotTierConfig,

    /// Cold tier configuration
    pub cold_tier: ColdTierConfig,

    /// Tiering policy configuration
    pub tiering: TieringConfig,
}

/// Hot tier (local NVMe) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotTierConfig {
    /// Base path for hot tier storage
    pub base_path: PathBuf,

    /// Maximum size for hot tier
    pub max_size: u64,

    /// Target free space percentage
    pub target_free_space_percent: u8,

    /// Emergency free space threshold
    pub emergency_threshold_percent: u8,
}

/// Cold tier (S3) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdTierConfig {
    /// S3 bucket name
    pub bucket: String,

    /// S3 key prefix
    pub prefix: String,

    /// Storage class (e.g., "ONEZONE_IA")
    pub storage_class: String,

    /// Enable compression for cold tier
    pub compression_enabled: bool,

    /// Compression algorithm
    pub compression_algorithm: String,

    /// Compression level (1-9)
    pub compression_level: u32,

    /// Multipart upload chunk size
    pub chunk_size: usize,
}

/// Tiering policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    /// Time-based tiering rules
    pub time_based: TimeBasedTieringConfig,

    /// Size-based tiering rules
    pub size_based: SizeBasedTieringConfig,

    /// Migration batch size
    pub batch_size: usize,

    /// Maximum concurrent migrations
    pub max_concurrent_migrations: usize,

    /// Bandwidth limit in MB/s
    pub bandwidth_limit_mbps: Option<u32>,
}

/// Time-based tiering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBasedTieringConfig {
    /// Move to cold tier after this duration
    #[serde(with = "humantime_serde")]
    pub cold_after: Duration,

    /// Promote to hot tier if accessed within this duration
    #[serde(with = "humantime_serde")]
    pub hot_if_accessed_within: Duration,
}

/// Size-based tiering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeBasedTieringConfig {
    /// Files larger than this are candidates for cold tier
    pub large_file_threshold: u64,

    /// Move large files to cold tier after this duration
    #[serde(with = "humantime_serde")]
    pub large_file_cold_after: Duration,
}

/// RPC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcConfig {
    /// VSOCK CID for the host
    pub host_cid: u32,

    /// VSOCK port to connect to
    pub port: u32,

    /// Connection timeout
    #[serde(with = "humantime_serde")]
    pub connection_timeout: Duration,

    /// Request timeout
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,

    /// Maximum request size
    pub max_request_size: usize,

    /// Maximum response size
    pub max_response_size: usize,
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Metadata cache size in bytes
    pub metadata_cache_size: usize,

    /// Data cache size in bytes
    pub data_cache_size: usize,

    /// Attribute cache timeout
    #[serde(with = "humantime_serde")]
    pub attr_timeout: Duration,

    /// Directory entry cache timeout
    #[serde(with = "humantime_serde")]
    pub entry_timeout: Duration,

    /// Enable read-ahead prefetching
    pub read_ahead_enabled: bool,

    /// Read-ahead size in blocks
    pub read_ahead_blocks: u32,
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Require attestation for connections
    pub require_attestation: bool,

    /// Maximum age for attestation documents
    #[serde(with = "humantime_serde")]
    pub attestation_max_age: Duration,

    /// Enable sandbox mode
    pub sandbox_enabled: bool,

    /// Sandbox root directory
    pub sandbox_root: PathBuf,

    /// Enable audit logging
    pub audit_logging: bool,

    /// Rate limit in requests per second
    pub rate_limit_rps: Option<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            filesystem: FilesystemConfig {
                max_file_size: 10 * 1024 * 1024 * 1024,       // 10GB
                max_total_storage: 1024 * 1024 * 1024 * 1024, // 1TB
                block_size: 4096,                             // 4KB
                database_direct_io: true,
                database_patterns: vec![
                    "*.db".to_string(),
                    "*.sqlite".to_string(),
                    "*.sqlite3".to_string(),
                ],
            },
            encryption: EncryptionConfig {
                algorithm: "AES-256-GCM".to_string(),
                kdf: KeyDerivationConfig {
                    algorithm: "SHA256".to_string(),
                    memory_cost: 65536, // 64MB
                    time_cost: 3,
                    parallelism: 4,
                },
                encrypt_metadata: true,
                encrypt_filenames: true,
            },
            storage: StorageConfig {
                hot_tier: HotTierConfig {
                    base_path: PathBuf::from("/mnt/instance-storage/vsock-fuse"),
                    max_size: 100 * 1024 * 1024 * 1024, // 100GB
                    target_free_space_percent: 20,
                    emergency_threshold_percent: 5,
                },
                cold_tier: ColdTierConfig {
                    bucket: "vsock-fuse-cold-tier".to_string(),
                    prefix: "enclave-data/".to_string(),
                    storage_class: "ONEZONE_IA".to_string(),
                    compression_enabled: true,
                    compression_algorithm: "zstd".to_string(),
                    compression_level: 3,
                    chunk_size: 32 * 1024 * 1024, // 32MB
                },
                tiering: TieringConfig {
                    time_based: TimeBasedTieringConfig {
                        cold_after: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
                        hot_if_accessed_within: Duration::from_secs(60 * 60), // 1 hour
                    },
                    size_based: SizeBasedTieringConfig {
                        large_file_threshold: 100 * 1024 * 1024, // 100MB
                        large_file_cold_after: Duration::from_secs(3 * 24 * 60 * 60), // 3 days
                    },
                    batch_size: 10,
                    max_concurrent_migrations: 4,
                    bandwidth_limit_mbps: Some(100),
                },
            },
            rpc: RpcConfig {
                host_cid: 3,
                port: 9999,
                connection_timeout: Duration::from_secs(30),
                request_timeout: Duration::from_secs(120),
                max_request_size: 10 * 1024 * 1024,  // 10MB
                max_response_size: 10 * 1024 * 1024, // 10MB
            },
            cache: CacheConfig {
                metadata_cache_size: 100 * 1024 * 1024, // 100MB
                data_cache_size: 500 * 1024 * 1024,     // 500MB
                attr_timeout: Duration::from_secs(1),
                entry_timeout: Duration::from_secs(5),
                read_ahead_enabled: true,
                read_ahead_blocks: 16,
            },
            security: SecurityConfig {
                require_attestation: true,
                attestation_max_age: Duration::from_secs(300), // 5 minutes
                sandbox_enabled: true,
                sandbox_root: PathBuf::from("/enclave-storage"),
                audit_logging: true,
                rate_limit_rps: Some(1000),
            },
        }
    }
}

impl Config {
    /// Load configuration from a file
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let contents =
            std::fs::read_to_string(path).map_err(|e| VsockFuseError::Configuration {
                message: format!("Failed to read config file: {e}"),
            })?;

        toml::from_str(&contents).map_err(|e| VsockFuseError::Configuration {
            message: format!("Failed to parse config: {e}"),
        })
    }

    /// Save configuration to a file
    pub fn to_file(&self, path: &PathBuf) -> Result<()> {
        let contents = toml::to_string_pretty(self).map_err(|e| VsockFuseError::Configuration {
            message: format!("Failed to serialize config: {e}"),
        })?;

        std::fs::write(path, contents).map_err(|e| VsockFuseError::Configuration {
            message: format!("Failed to write config file: {e}"),
        })?;

        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Validate filesystem config
        if self.filesystem.block_size == 0 || !self.filesystem.block_size.is_multiple_of(512) {
            return Err(VsockFuseError::Configuration {
                message: "Block size must be a multiple of 512".to_string(),
            });
        }

        // Validate storage config
        if self.storage.hot_tier.target_free_space_percent > 100 {
            return Err(VsockFuseError::Configuration {
                message: "Target free space percentage must be <= 100".to_string(),
            });
        }

        // Validate encryption config
        if self.encryption.algorithm != "AES-256-GCM" {
            return Err(VsockFuseError::Configuration {
                message: format!(
                    "Unsupported encryption algorithm: {}",
                    self.encryption.algorithm
                ),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.filesystem.block_size, 4096);
        assert_eq!(config.encryption.algorithm, "AES-256-GCM");
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        config.filesystem.block_size = 0;
        assert!(config.validate().is_err());

        config.filesystem.block_size = 4096;
        config.storage.hot_tier.target_free_space_percent = 101;
        assert!(config.validate().is_err());
    }
}
