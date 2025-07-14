//! RocksDB configuration for the storage adaptor layer

use crate::storage_backends::types::CompressionType;
use std::path::PathBuf;

/// RocksDB configuration for the storage adaptor
#[derive(Debug, Clone)]
pub struct RocksDBAdaptorConfig {
    /// Database path
    pub path: PathBuf,

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

    /// Write buffer size (default: 64MB)
    pub write_buffer_size: usize,

    /// Number of write buffers
    pub max_write_buffer_number: i32,

    /// Compression type
    pub compression: CompressionType,

    /// Bloom filter bits per key (0 to disable)
    pub bloom_filter_bits: i32,

    /// Block size
    pub block_size: usize,

    /// Block cache size
    pub block_cache_size: usize,

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

    /// Enable SST file manager for space management
    pub enable_sst_file_manager: bool,

    /// Maximum SST file size
    pub max_sst_file_size: u64,

    /// Use direct I/O for reads
    pub use_direct_io_for_flush_and_compaction: bool,

    /// Enable pipelined writes
    pub enable_pipelined_write: bool,

    /// Number of threads for flush and compaction
    pub increase_parallelism: i32,

    /// Sync writes to disk
    pub sync_writes: bool,

    /// Disable WAL
    pub disable_wal: bool,
}

impl Default for RocksDBAdaptorConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./rocksdb_data"),
            max_open_files: -1, // Unlimited
            max_background_jobs: 4,
            enable_statistics: false,
            keep_log_file_num: 1000,
            max_total_wal_size: 1024 * 1024 * 1024, // 1GB
            write_buffer_size: 64 * 1024 * 1024,    // 64MB
            max_write_buffer_number: 3,
            compression: CompressionType::Lz4,
            bloom_filter_bits: 10,
            block_size: 16 * 1024,               // 16KB
            block_cache_size: 512 * 1024 * 1024, // 512MB
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            target_file_size_base: 64 * 1024 * 1024, // 64MB
            target_file_size_multiplier: 2,
            enable_sst_file_manager: true,
            max_sst_file_size: 256 * 1024 * 1024, // 256MB
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            increase_parallelism: 4,
            sync_writes: false,
            disable_wal: false,
        }
    }
}

impl RocksDBAdaptorConfig {
    /// Create a development configuration with lower resource usage
    pub fn development<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            max_background_jobs: 2,
            block_cache_size: 128 * 1024 * 1024, // 128MB
            increase_parallelism: 2,
            enable_statistics: true,
            ..Default::default()
        }
    }

    /// Create a production configuration optimized for performance
    pub fn production<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            max_background_jobs: 8,
            write_buffer_size: 128 * 1024 * 1024, // 128MB
            block_cache_size: 1024 * 1024 * 1024, // 1GB
            increase_parallelism: 8,
            use_direct_io_for_flush_and_compaction: true,
            enable_statistics: false, // Disable for performance
            ..Default::default()
        }
    }

    /// Create a configuration optimized for bulk operations
    pub fn bulk_operations<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            // Larger write buffers for bulk operations
            write_buffer_size: 256 * 1024 * 1024, // 256MB
            max_write_buffer_number: 4,

            // More aggressive compaction for faster SST generation
            level0_file_num_compaction_trigger: 2,
            target_file_size_base: 128 * 1024 * 1024, // 128MB

            // Optimize for bulk writes
            enable_pipelined_write: true,
            max_background_jobs: 8,

            // Disable WAL for bulk operations
            disable_wal: true,

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
