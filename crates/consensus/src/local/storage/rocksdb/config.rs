//! RocksDB configuration for local consensus storage

use std::path::PathBuf;

use crate::local::storage::traits::{CompressionType, WalConfig};

/// RocksDB configuration for local consensus groups
#[derive(Debug, Clone)]
pub struct RocksDBConfig {
    // Database paths
    /// Base directory for all group databases
    pub base_path: PathBuf,

    // Per-group settings
    /// Maximum number of open files (-1 for unlimited)
    pub max_open_files: i32,
    /// Number of background jobs for compaction
    pub max_background_jobs: i32,
    /// Enable statistics collection
    pub enable_statistics: bool,
    /// Keep log files for debugging
    pub keep_log_file_num: usize,
    /// Maximum total WAL size
    pub max_total_wal_size: u64,

    // Per-stream column family settings
    /// Write buffer size per stream (default: 64MB)
    pub stream_write_buffer_size: usize,
    /// Number of write buffers per stream
    pub stream_max_write_buffer_number: i32,
    /// Compression type for streams
    pub stream_compression: CompressionType,
    /// Bloom filter bits per key (0 to disable)
    pub stream_bloom_filter_bits: i32,
    /// Block size for streams
    pub stream_block_size: usize,
    /// Block cache size per stream
    pub stream_block_cache_size: usize,

    // Compaction settings
    /// Level 0 file number compaction trigger
    pub level0_file_num_compaction_trigger: i32,
    /// Level 0 slowdown writes trigger
    pub level0_slowdown_writes_trigger: i32,
    /// Level 0 stop writes trigger
    pub level0_stop_writes_trigger: i32,
    /// Target file size base
    pub target_file_size_base: u64,
    /// Target file size multiplier
    pub target_file_size_multiplier: i32,

    // Migration settings
    /// Directory for checkpoints during migration
    pub checkpoint_dir: PathBuf,
    /// Enable SST file manager for space management
    pub enable_sst_file_manager: bool,
    /// Maximum SST file size
    pub max_sst_file_size: u64,

    // WAL settings
    /// Write-ahead log configuration
    pub wal_config: WalConfig,

    // Performance settings
    /// Use direct I/O for reads
    pub use_direct_io_for_flush_and_compaction: bool,
    /// Enable pipelined writes
    pub enable_pipelined_write: bool,
    /// Number of threads for flush and compaction
    pub increase_parallelism: i32,
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        Self {
            // Use current directory by default (should be overridden)
            base_path: PathBuf::from("./rocksdb_data"),

            // Per-group settings
            max_open_files: -1, // Unlimited
            max_background_jobs: 4,
            enable_statistics: false,
            keep_log_file_num: 1000,
            max_total_wal_size: 1024 * 1024 * 1024, // 1GB

            // Per-stream settings
            stream_write_buffer_size: 64 * 1024 * 1024, // 64MB
            stream_max_write_buffer_number: 3,
            stream_compression: CompressionType::Lz4,
            stream_bloom_filter_bits: 10,
            stream_block_size: 16 * 1024,               // 16KB
            stream_block_cache_size: 512 * 1024 * 1024, // 512MB

            // Compaction settings
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            target_file_size_base: 64 * 1024 * 1024, // 64MB
            target_file_size_multiplier: 2,

            // Migration settings
            checkpoint_dir: PathBuf::from("./checkpoints"),
            enable_sst_file_manager: true,
            max_sst_file_size: 256 * 1024 * 1024, // 256MB

            // WAL settings
            wal_config: WalConfig::default(),

            // Performance settings
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            increase_parallelism: 4,
        }
    }
}

impl RocksDBConfig {
    /// Create a development configuration with lower resource usage
    pub fn development() -> Self {
        Self {
            max_background_jobs: 2,
            stream_block_cache_size: 128 * 1024 * 1024, // 128MB
            increase_parallelism: 2,
            enable_statistics: true,
            ..Default::default()
        }
    }

    /// Create a production configuration optimized for performance
    pub fn production() -> Self {
        Self {
            max_background_jobs: 8,
            stream_write_buffer_size: 128 * 1024 * 1024, // 128MB
            stream_block_cache_size: 1024 * 1024 * 1024, // 1GB
            increase_parallelism: 8,
            use_direct_io_for_flush_and_compaction: true,
            enable_statistics: false, // Disable for performance
            ..Default::default()
        }
    }

    /// Create a configuration optimized for migrations
    pub fn migration_optimized() -> Self {
        Self {
            // Larger write buffers for bulk operations
            stream_write_buffer_size: 256 * 1024 * 1024, // 256MB
            stream_max_write_buffer_number: 4,

            // More aggressive compaction for faster SST generation
            level0_file_num_compaction_trigger: 2,
            target_file_size_base: 128 * 1024 * 1024, // 128MB

            // Optimize for bulk writes
            enable_pipelined_write: true,
            max_background_jobs: 8,

            ..Default::default()
        }
    }
}

/// Convert our compression type to RocksDB compression type
pub fn to_rocksdb_compression(compression: CompressionType) -> rocksdb::DBCompressionType {
    match compression {
        CompressionType::None => rocksdb::DBCompressionType::None,
        CompressionType::Lz4 => rocksdb::DBCompressionType::Lz4,
        CompressionType::Zstd => rocksdb::DBCompressionType::Zstd,
        CompressionType::Snappy => rocksdb::DBCompressionType::Snappy,
    }
}
