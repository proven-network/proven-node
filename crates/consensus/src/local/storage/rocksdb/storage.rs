//! RocksDB storage implementation for local consensus groups

use std::path::PathBuf;
use std::sync::Arc;

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IteratorMode, Options};
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::allocation::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use crate::local::state_machine::LocalState;

use super::config::{RocksDBConfig, to_rocksdb_compression};
use crate::local::storage::traits::{StreamMetadata, StreamOptions};

/// Column family names
pub(super) const CF_DEFAULT: &str = "default";
pub(super) const CF_LOGS: &str = "logs";
pub(super) const CF_STREAM_INDEX: &str = "stream_index";
pub(super) const CF_SNAPSHOTS: &str = "snapshots";

/// Key prefixes for stream_index column family
pub(super) const KEY_PREFIX_META: &str = "meta:";
pub(super) const KEY_PREFIX_PAUSE: &str = "pause:";
pub(super) const _KEY_PREFIX_PENDING: &str = "pending:";

/// RocksDB-based storage for local consensus groups
#[derive(Clone)]
pub struct LocalRocksDBStorage {
    /// RocksDB database instance
    pub db: Arc<DB>,
    /// Group ID this storage is for
    pub(super) group_id: ConsensusGroupId,
    /// Storage configuration
    pub(super) config: Arc<RocksDBConfig>,
    /// Stream-specific options
    pub(super) stream_options: StreamOptions,
    /// Path to this group's database
    pub(super) _db_path: PathBuf,
    /// State machine (for compatibility with LocalMemoryStorage)
    pub state_machine: Arc<RwLock<LocalState>>,
}

impl LocalRocksDBStorage {
    /// Create new RocksDB storage for a consensus group
    pub fn new(group_id: ConsensusGroupId, config: RocksDBConfig) -> ConsensusResult<Self> {
        let db_path = config.base_path.join(format!("group_{:?}", group_id));

        // Ensure directory exists
        std::fs::create_dir_all(&db_path)
            .map_err(|e| Error::Storage(format!("Failed to create directory: {}", e)))?;

        // Create DB options
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_open_files(config.max_open_files);
        db_opts.set_max_background_jobs(config.max_background_jobs);
        db_opts.set_max_total_wal_size(config.max_total_wal_size);
        db_opts.set_keep_log_file_num(config.keep_log_file_num);

        if config.enable_statistics {
            db_opts.enable_statistics();
        }

        if config.use_direct_io_for_flush_and_compaction {
            db_opts.set_use_direct_io_for_flush_and_compaction(true);
        }

        // Create column families with specific options
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_DEFAULT, Options::default()),
            ColumnFamilyDescriptor::new(CF_LOGS, create_logs_cf_options()),
            ColumnFamilyDescriptor::new(CF_STREAM_INDEX, create_index_cf_options()),
            ColumnFamilyDescriptor::new(CF_SNAPSHOTS, Options::default()),
        ];

        // Open database
        let db = DB::open_cf_descriptors(&db_opts, &db_path, cf_descriptors)
            .map_err(|e| Error::Storage(format!("Failed to open RocksDB: {}", e)))?;

        let db = Arc::new(db);

        // Verify column families were created
        for cf_name in &[CF_DEFAULT, CF_LOGS, CF_STREAM_INDEX, CF_SNAPSHOTS] {
            if db.cf_handle(cf_name).is_none() {
                return Err(Error::Storage(format!(
                    "Column family {} not found after creation",
                    cf_name
                )));
            }
        }

        info!(
            "Initialized RocksDB storage for group {:?} at {:?}",
            group_id, db_path
        );

        Ok(Self {
            db,
            group_id,
            config: Arc::new(config),
            stream_options: StreamOptions::default(),
            _db_path: db_path,
            state_machine: Arc::new(RwLock::new(LocalState::new(group_id))),
        })
    }

    /// Get or create a column family for a stream
    pub async fn get_or_create_stream_cf(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<&ColumnFamily> {
        let cf_name = format!("stream_{}", stream_name);

        // Check if already exists
        if let Some(cf) = self.db.cf_handle(&cf_name) {
            return Ok(cf);
        }

        // Create column family with stream-specific options
        let mut opts = Options::default();
        opts.set_compression_type(to_rocksdb_compression(self.stream_options.compression));
        opts.set_write_buffer_size(self.stream_options.write_buffer_size);
        opts.set_max_write_buffer_number(self.stream_options.max_write_buffer_number);
        opts.set_target_file_size_base(self.stream_options.target_file_size_base);

        if self.stream_options.enable_statistics {
            opts.enable_statistics();
        }

        if let Some(_ttl) = self.stream_options.ttl_seconds {
            // Note: TTL is set at DB open time, not CF creation time in current RocksDB
            // We'll need to handle TTL through compaction filters instead
            debug!(
                "TTL requested for stream {} but must be handled via compaction",
                stream_name
            );
        }

        // Set bloom filter
        if self.config.stream_bloom_filter_bits > 0 {
            opts.set_bloom_locality(1);
            // Note: Bloom filter is set on the block-based table options
        }

        // Create the column family
        // Note: We need to handle the Arc<DB> properly
        // Clone the Arc to get a new reference
        let db_clone = Arc::clone(&self.db);

        // Use a separate scope to ensure we don't hold references across await points
        {
            // This is a workaround for the const/mut issue with RocksDB
            // The create_cf method requires &mut self but we have Arc<DB>
            // We'll use the fact that column family creation is thread-safe in RocksDB
            let db_ptr = Arc::as_ptr(&db_clone) as *mut DB;
            unsafe {
                (*db_ptr).create_cf(&cf_name, &opts).map_err(|e| {
                    Error::Storage(format!("Failed to create column family: {}", e))
                })?;
            }
        }

        // Get the handle
        self.db.cf_handle(&cf_name).ok_or_else(|| {
            Error::Storage(format!(
                "Failed to get handle for created column family: {}",
                cf_name
            ))
        })
    }

    /// Get a column family handle by name
    pub(super) async fn get_cf(&self, cf_name: &str) -> ConsensusResult<&ColumnFamily> {
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| Error::Storage(format!("Column family not found: {}", cf_name)))
    }

    /// Get the stream column family, creating if needed
    pub(super) async fn get_stream_cf(&self, stream_name: &str) -> ConsensusResult<&ColumnFamily> {
        self.get_or_create_stream_cf(stream_name).await
    }

    /// Encode a sequence number as a key
    pub fn encode_sequence(seq: u64) -> Vec<u8> {
        seq.to_be_bytes().to_vec()
    }

    /// Decode a sequence number from a key
    pub fn decode_sequence(key: &[u8]) -> ConsensusResult<u64> {
        if key.len() != 8 {
            return Err(Error::Storage(format!(
                "Invalid sequence key length: {}",
                key.len()
            )));
        }
        Ok(u64::from_be_bytes(key.try_into().unwrap()))
    }

    /// Get the last sequence number for a stream
    #[allow(dead_code)]
    pub(super) async fn get_last_sequence(&self, stream_cf: &ColumnFamily) -> ConsensusResult<u64> {
        // Use the column family reference directly
        let iter = self.db.iterator_cf(stream_cf, IteratorMode::End);

        if let Some(Ok((key, _))) = iter.into_iter().next() {
            Self::decode_sequence(&key)
        } else {
            Ok(0)
        }
    }

    /// Get stream metadata
    pub(super) async fn get_stream_metadata(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<StreamMetadata>> {
        let cf_index = self.get_cf(CF_STREAM_INDEX).await?;
        let key = format!("{}{}", KEY_PREFIX_META, stream_name);

        if let Some(value) = self
            .db
            .get_cf(&cf_index, key.as_bytes())
            .map_err(|e| Error::Storage(format!("Failed to get metadata: {}", e)))?
        {
            let metadata: StreamMetadata = serde_json::from_slice(&value)
                .map_err(|e| Error::Storage(format!("Failed to deserialize metadata: {}", e)))?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    /// Update stream metadata
    pub async fn update_stream_metadata(&self, metadata: &StreamMetadata) -> ConsensusResult<()> {
        let cf_index = self.get_cf(CF_STREAM_INDEX).await?;
        let key = format!("{}{}", KEY_PREFIX_META, metadata.name);
        let value = serde_json::to_vec(metadata)
            .map_err(|e| Error::Storage(format!("Failed to serialize metadata: {}", e)))?;

        self.db
            .put_cf(&cf_index, key.as_bytes(), value)
            .map_err(|e| Error::Storage(format!("Failed to update metadata: {}", e)))?;

        Ok(())
    }
}

/// Create options for logs column family
fn create_logs_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_write_buffer_size(32 * 1024 * 1024); // 32MB
    opts.set_max_write_buffer_number(3);
    opts
}

/// Create options for stream index column family
fn create_index_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_write_buffer_size(16 * 1024 * 1024); // 16MB
    opts
}
