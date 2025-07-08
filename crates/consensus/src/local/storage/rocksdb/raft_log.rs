//! RaftLogStorage implementation for RocksDB

use std::fmt::Debug;
use std::ops::RangeBounds;

use openraft::{
    Entry, LogId, RaftLogReader, StorageError, Vote,
    entry::RaftEntry,
    storage::{IOFlushed, LogState, RaftLogStorage},
};
use rocksdb::{Direction, IteratorMode, WriteBatch};
use tracing::debug;

use crate::error::{ConsensusResult, Error};
use crate::local::LocalTypeConfig;

use super::storage::{CF_DEFAULT, CF_LOGS, LocalRocksDBStorage};

/// Key prefixes for default column family
const KEY_PREFIX_VOTE: &str = "vote";
const KEY_PREFIX_LAST_PURGED: &str = "last_purged";

impl LocalRocksDBStorage {
    /// Encode a log entry key
    fn encode_log_key(index: u64) -> Vec<u8> {
        index.to_be_bytes().to_vec()
    }

    /// Decode a log entry key
    fn decode_log_key(key: &[u8]) -> ConsensusResult<u64> {
        if key.len() != 8 {
            return Err(Error::Storage(format!(
                "Invalid log key length: {}",
                key.len()
            )));
        }
        Ok(u64::from_be_bytes(key.try_into().unwrap()))
    }

    /// Get vote from storage
    async fn get_vote(&self) -> ConsensusResult<Option<Vote<LocalTypeConfig>>> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_VOTE.as_bytes();

        if let Some(value) = self
            .db
            .get_cf(&cf_default, key)
            .map_err(|e| Error::Storage(format!("Failed to get vote: {}", e)))?
        {
            let vote: Vote<LocalTypeConfig> = serde_json::from_slice(&value)
                .map_err(|e| Error::Storage(format!("Failed to deserialize vote: {}", e)))?;
            Ok(Some(vote))
        } else {
            Ok(None)
        }
    }

    /// Save vote to storage
    async fn save_vote_internal(&self, vote: &Vote<LocalTypeConfig>) -> ConsensusResult<()> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_VOTE.as_bytes();
        let value = serde_json::to_vec(vote)
            .map_err(|e| Error::Storage(format!("Failed to serialize vote: {}", e)))?;

        self.db
            .put_cf(&cf_default, key, value)
            .map_err(|e| Error::Storage(format!("Failed to save vote: {}", e)))?;

        // Ensure vote is persisted
        self.db
            .flush_wal(true)
            .map_err(|e| Error::Storage(format!("Failed to flush WAL: {}", e)))?;

        Ok(())
    }

    /// Get last purged log ID
    async fn get_last_purged(&self) -> ConsensusResult<Option<LogId<LocalTypeConfig>>> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_LAST_PURGED.as_bytes();

        if let Some(value) = self
            .db
            .get_cf(&cf_default, key)
            .map_err(|e| Error::Storage(format!("Failed to get last purged: {}", e)))?
        {
            let log_id: LogId<LocalTypeConfig> = serde_json::from_slice(&value)
                .map_err(|e| Error::Storage(format!("Failed to deserialize last purged: {}", e)))?;
            Ok(Some(log_id))
        } else {
            Ok(None)
        }
    }

    /// Save last purged log ID
    async fn save_last_purged(&self, log_id: &LogId<LocalTypeConfig>) -> ConsensusResult<()> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_LAST_PURGED.as_bytes();
        let value = serde_json::to_vec(log_id)
            .map_err(|e| Error::Storage(format!("Failed to serialize last purged: {}", e)))?;

        self.db
            .put_cf(&cf_default, key, value)
            .map_err(|e| Error::Storage(format!("Failed to save last purged: {}", e)))?;

        Ok(())
    }
}

impl RaftLogReader<LocalTypeConfig> for LocalRocksDBStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        let cf_logs = self
            .get_cf(CF_LOGS)
            .await
            .map_err(|e| StorageError::read(&e))?;

        // Convert range bounds to start key
        let start_key = match range.start_bound() {
            std::ops::Bound::Included(&n) => Self::encode_log_key(n),
            std::ops::Bound::Excluded(&n) => Self::encode_log_key(n + 1),
            std::ops::Bound::Unbounded => Self::encode_log_key(0),
        };

        let mut entries = Vec::new();
        let iter = self
            .db
            .iterator_cf(&cf_logs, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::read(&e))?;
            let index = Self::decode_log_key(&key).map_err(|e| StorageError::read(&e))?;

            // Check if index is within range
            if !range.contains(&index) {
                break;
            }

            let entry: Entry<LocalTypeConfig> =
                serde_json::from_slice(&value).map_err(|e| StorageError::read(&e))?;

            // Verify index matches
            if entry.log_id().index != index {
                return Err(StorageError::read(&Error::Storage(format!(
                    "Log entry index mismatch: expected {}, got {}",
                    index,
                    entry.log_id().index
                ))));
            }

            entries.push(entry);
        }

        debug!("Read {} log entries in range {:?}", entries.len(), range);
        Ok(entries)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        self.get_vote()
            .await
            .map_err(|e| StorageError::read_vote(&e))
    }
}

impl RaftLogStorage<LocalTypeConfig> for LocalRocksDBStorage {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        debug!("Saving vote: {:?}", vote);
        self.save_vote_internal(vote)
            .await
            .map_err(|e| StorageError::write_vote(&e))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<LocalTypeConfig>> + Send,
    {
        let cf_logs = self
            .get_cf(CF_LOGS)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        let mut batch = WriteBatch::default();
        let mut count = 0;

        for entry in entries {
            let index = entry.log_id().index;
            let key = Self::encode_log_key(index);
            let value = serde_json::to_vec(&entry).map_err(|e| StorageError::write_logs(&e))?;

            batch.put_cf(&cf_logs, key, value);
            count += 1;
        }

        if count > 0 {
            self.db
                .write(batch)
                .map_err(|e| StorageError::write_logs(&e))?;

            // Optionally flush WAL based on configuration
            if self.config.wal_config.sync_on_commit {
                self.db
                    .flush_wal(true)
                    .map_err(|e| StorageError::write_logs(&e))?;
            }

            debug!("Appended {} log entries", count);
        }

        // Notify completion
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        debug!("Truncating logs from index {}", log_id.index);

        let cf_logs = self
            .get_cf(CF_LOGS)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        // Find all entries to delete
        let start_key = Self::encode_log_key(log_id.index);
        let mut batch = WriteBatch::default();
        let mut count = 0;

        let iter = self
            .db
            .iterator_cf(&cf_logs, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::write_logs(&e))?;
            batch.delete_cf(&cf_logs, key);
            count += 1;
        }

        if count > 0 {
            self.db
                .write(batch)
                .map_err(|e| StorageError::write_logs(&e))?;

            debug!("Truncated {} log entries", count);
        }

        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        debug!("Purging logs up to index {}", log_id.index);

        // Save last purged ID first
        self.save_last_purged(&log_id)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        let cf_logs = self
            .get_cf(CF_LOGS)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        // Delete all entries up to and including log_id
        let end_key = Self::encode_log_key(log_id.index + 1);
        let mut batch = WriteBatch::default();
        let mut count = 0;

        let iter = self.db.iterator_cf(&cf_logs, IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::write_logs(&e))?;

            // Stop if we've reached entries we should keep
            if key.as_ref() >= end_key.as_slice() {
                break;
            }

            batch.delete_cf(&cf_logs, key);
            count += 1;
        }

        if count > 0 {
            self.db
                .write(batch)
                .map_err(|e| StorageError::write_logs(&e))?;

            debug!("Purged {} log entries", count);
        }

        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<LocalTypeConfig>, StorageError<LocalTypeConfig>> {
        let last_purged_log_id = self
            .get_last_purged()
            .await
            .map_err(|e| StorageError::read(&e))?;

        let cf_logs = self
            .get_cf(CF_LOGS)
            .await
            .map_err(|e| StorageError::read(&e))?;

        // Get last log entry
        let last_log_id = if let Some(Ok((_key, value))) =
            self.db.iterator_cf(&cf_logs, IteratorMode::End).next()
        {
            let entry: Entry<LocalTypeConfig> =
                serde_json::from_slice(&value).map_err(|e| StorageError::read(&e))?;
            Some(entry.log_id().clone())
        } else {
            last_purged_log_id.clone()
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
}
