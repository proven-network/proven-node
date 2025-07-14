//! Log storage implementation for global consensus
//!
//! This module provides the log storage component that implements RaftLogStorage
//! for global consensus, handling only log persistence responsibilities.

use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, RaftLogReader, StorageError, Vote};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::global::GlobalConsensusTypeConfig;
use crate::storage_backends::{
    StorageEngine, StorageKey, StorageNamespace, StorageValue, log::LogStorage,
};

use super::log_types::{GlobalEntryType, GlobalLogMetadata};

/// Keys for metadata storage
const VOTE_KEY: &[u8] = b"vote";
const LAST_PURGED_KEY: &[u8] = b"last_purged";

/// Namespaces for storage
const META_NAMESPACE: &str = "meta";
const LOGS_NAMESPACE: &str = "logs";

/// Global log storage that only handles log persistence
#[derive(Clone)]
pub struct GlobalLogStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata>,
{
    /// The underlying storage engine
    storage: Arc<S>,
    /// Log entries cache for performance
    log_cache: Arc<RwLock<BTreeMap<u64, Entry<GlobalConsensusTypeConfig>>>>,
}

impl<S> GlobalLogStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    /// Create a new global log storage instance
    pub async fn new(storage: Arc<S>) -> Result<Self, StorageError<GlobalConsensusTypeConfig>> {
        // Create required namespaces
        storage
            .create_namespace(&StorageNamespace::new(META_NAMESPACE))
            .await
            .map_err(|e| StorageError::write(&e))?;

        storage
            .create_namespace(&StorageNamespace::new(LOGS_NAMESPACE))
            .await
            .map_err(|e| StorageError::write(&e))?;

        Ok(Self {
            storage,
            log_cache: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    /// Get the underlying storage engine
    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    /// Load all log entries from storage into cache
    async fn load_logs(&self) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        // For now, we'll leave the cache empty and load entries on demand
        // In a production implementation, we would scan the storage namespace
        // to find all log entries
        let mut cache = self.log_cache.write().await;
        cache.clear();

        Ok(())
    }

    /// Get a log entry by index
    async fn get_log_entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry<GlobalConsensusTypeConfig>>, StorageError<GlobalConsensusTypeConfig>>
    {
        // Check cache first
        {
            let cache = self.log_cache.read().await;
            if let Some(entry) = cache.get(&index) {
                return Ok(Some(entry.clone()));
            }
        }

        // Load from storage if not in cache
        let key = StorageKey::from(index.to_be_bytes().as_ref());
        let value = self
            .storage
            .get(&StorageNamespace::new(LOGS_NAMESPACE), &key)
            .await
            .map_err(|e| StorageError::read_logs(&e))?;

        match value {
            Some(v) => {
                let entry: Entry<GlobalConsensusTypeConfig> =
                    ciborium::from_reader(v.as_bytes()).map_err(|e| StorageError::read_logs(&e))?;

                // Update cache
                self.log_cache.write().await.insert(index, entry.clone());

                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Get the last log id
    async fn get_last_log_id(
        &self,
    ) -> Result<Option<LogId<GlobalConsensusTypeConfig>>, StorageError<GlobalConsensusTypeConfig>>
    {
        let cache = self.log_cache.read().await;
        if let Some((_, entry)) = cache.last_key_value() {
            return Ok(Some(entry.log_id.clone()));
        }

        // If cache is empty, try loading from storage
        drop(cache);
        self.load_logs().await?;

        let cache = self.log_cache.read().await;
        Ok(cache
            .last_key_value()
            .map(|(_, entry)| entry.log_id.clone()))
    }
}

impl<S> RaftLogReader<GlobalConsensusTypeConfig> for GlobalLogStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GlobalConsensusTypeConfig>>, StorageError<GlobalConsensusTypeConfig>>
    {
        // Ensure cache is loaded
        if self.log_cache.read().await.is_empty() {
            self.load_logs().await?;
        }

        let cache = self.log_cache.read().await;
        let entries: Vec<_> = cache.range(range).map(|(_, entry)| entry.clone()).collect();

        Ok(entries)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<GlobalConsensusTypeConfig>>, StorageError<GlobalConsensusTypeConfig>>
    {
        let key = StorageKey::from(VOTE_KEY);
        let value = self
            .storage
            .get(&StorageNamespace::new(META_NAMESPACE), &key)
            .await
            .map_err(|e| StorageError::read_vote(&e))?;

        match value {
            Some(v) => {
                let vote =
                    ciborium::from_reader(v.as_bytes()).map_err(|e| StorageError::read_vote(&e))?;
                Ok(Some(vote))
            }
            None => Ok(None),
        }
    }
}

impl<S> RaftLogStorage<GlobalConsensusTypeConfig> for GlobalLogStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<GlobalConsensusTypeConfig>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(vote, &mut buffer).map_err(|e| StorageError::write_vote(&e))?;

        self.storage
            .put(
                &StorageNamespace::new(META_NAMESPACE),
                StorageKey::from(VOTE_KEY),
                StorageValue::new(buffer),
            )
            .await
            .map_err(|e| StorageError::write_vote(&e))?;

        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<GlobalConsensusTypeConfig>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut cache = self.log_cache.write().await;
        let entries_vec: Vec<_> = entries.into_iter().collect();

        // Store each entry
        for entry in &entries_vec {
            let index = entry.log_id.index;

            // Create metadata
            let metadata = GlobalLogMetadata {
                term: entry.log_id.leader_id.term,
                leader_node_id: entry.log_id.leader_id.node_id.clone(),
                entry_type: match &entry.payload {
                    openraft::EntryPayload::Blank => GlobalEntryType::Blank,
                    openraft::EntryPayload::Normal(_) => GlobalEntryType::Normal,
                    openraft::EntryPayload::Membership(_) => GlobalEntryType::Membership,
                },
            };

            // Store in engine
            let key = StorageKey::from(index.to_be_bytes().as_ref());
            let mut buffer = Vec::new();
            ciborium::into_writer(&entry, &mut buffer).map_err(|e| StorageError::write_logs(&e))?;

            // Use the LogStorage trait to append the entry
            let log_entry = crate::storage_backends::log::LogEntry {
                index,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
                data: bytes::Bytes::from(buffer),
                metadata,
            };

            self.storage
                .append_entry(&StorageNamespace::new(LOGS_NAMESPACE), log_entry)
                .await
                .map_err(|e| StorageError::write_logs(&e))?;

            // Update cache
            cache.insert(index, entry.clone());
        }

        // Notify completion
        callback.io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        let mut cache = self.log_cache.write().await;

        // Remove from cache
        cache.retain(|&index, _| index < log_id.index);

        // Truncate in storage using LogStorage trait
        self.storage
            .truncate(&StorageNamespace::new(LOGS_NAMESPACE), log_id.index)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        let mut cache = self.log_cache.write().await;

        // Remove from cache
        cache.retain(|&index, _| index > log_id.index);

        // Store last purged
        let mut buffer = Vec::new();
        ciborium::into_writer(&log_id, &mut buffer).map_err(|e| StorageError::write(&e))?;

        self.storage
            .put(
                &StorageNamespace::new(META_NAMESPACE),
                StorageKey::from(LAST_PURGED_KEY),
                StorageValue::new(buffer),
            )
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Purge from storage using LogStorage trait
        self.storage
            .purge(&StorageNamespace::new(LOGS_NAMESPACE), log_id.index)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GlobalConsensusTypeConfig>, StorageError<GlobalConsensusTypeConfig>> {
        // Get last purged
        let last_purged_key = StorageKey::from(LAST_PURGED_KEY);
        let last_purged_value = self
            .storage
            .get(&StorageNamespace::new(META_NAMESPACE), &last_purged_key)
            .await
            .map_err(|e| StorageError::read(&e))?;

        let last_purged_log_id = match last_purged_value {
            Some(v) => {
                let log_id: LogId<GlobalConsensusTypeConfig> =
                    ciborium::from_reader(v.as_bytes()).map_err(|e| StorageError::read(&e))?;
                Some(log_id)
            }
            None => None,
        };

        // Get last log id
        let last_log_id = self.get_last_log_id().await?;

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
}
