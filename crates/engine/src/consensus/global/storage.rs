//! Raft log storage implementation for global consensus
//!
//! This module provides a pure storage implementation that only handles
//! log persistence without any business logic.

use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::{
    Entry, LogId, StorageError,
    storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage},
};
use tokio::sync::RwLock;

use bytes::Bytes;
use proven_storage::{LogStorage, StorageNamespace};

use super::raft::GlobalTypeConfig;

/// Raft log storage - only handles log persistence
#[derive(Clone)]
pub struct GlobalRaftLogStorage<L: LogStorage> {
    /// Log storage backend
    log_storage: Arc<L>,
    /// Namespace for logs
    namespace: StorageNamespace,
    /// Namespace for state
    state_namespace: StorageNamespace,
    /// Current log state
    log_state: Arc<RwLock<LogState<GlobalTypeConfig>>>,
    /// Persisted vote
    vote: Arc<RwLock<Option<openraft::Vote<GlobalTypeConfig>>>>,
    /// Persisted committed index
    committed: Arc<RwLock<Option<LogId<GlobalTypeConfig>>>>,
}

impl<L: LogStorage> GlobalRaftLogStorage<L> {
    /// Create new log storage
    pub fn new(log_storage: Arc<L>) -> Self {
        let namespace = StorageNamespace::new("global_logs");
        let state_namespace = StorageNamespace::new("global_state");

        Self {
            log_storage,
            namespace,
            state_namespace,
            log_state: Arc::new(RwLock::new(LogState::default())),
            vote: Arc::new(RwLock::new(None)),
            committed: Arc::new(RwLock::new(None)),
        }
    }
}

// Implement RaftLogReader trait
impl<L: LogStorage> RaftLogReader<GlobalTypeConfig> for Arc<GlobalRaftLogStorage<L>> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        use std::ops::Bound;

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => u64::MAX,
        };

        // Read from storage
        let entries = self
            .log_storage
            .read_range(&self.namespace, start, end)
            .await
            .map_err(|e| StorageError::read(&e))?;

        // Deserialize entries
        let mut result = Vec::new();
        for (_index, data) in entries {
            let entry: Entry<GlobalTypeConfig> =
                ciborium::from_reader(data.as_ref()).map_err(|e| StorageError::read(&e))?;
            result.push(entry);
        }

        Ok(result)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        // Return the cached vote
        // In production, this should also verify against persistent storage
        Ok(self.vote.read().await.clone())
    }
}

impl<L: LogStorage> RaftLogStorage<GlobalTypeConfig> for Arc<GlobalRaftLogStorage<L>> {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        // Save vote to memory
        *self.vote.write().await = Some(vote.clone());

        // Persist to storage backend
        let mut buffer = Vec::new();
        ciborium::into_writer(vote, &mut buffer).map_err(|e| StorageError::write(&e))?;

        // Use a special key for vote in state namespace
        let vote_key = 0; // Using index 0 for vote
        self.log_storage
            .append(&self.state_namespace, vec![(vote_key, Bytes::from(buffer))])
            .await
            .map_err(|e| StorageError::write(&e))?;

        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        // Serialize and prepare entries for storage
        let mut storage_entries = Vec::new();
        let mut last_log_id = None;

        for entry in &entries {
            let mut buffer = Vec::new();
            ciborium::into_writer(entry, &mut buffer).map_err(|e| StorageError::write(&e))?;
            storage_entries.push((entry.log_id.index, Bytes::from(buffer)));
            last_log_id = Some(entry.log_id.clone());
        }

        // Append to storage
        self.log_storage
            .append(&self.namespace, storage_entries)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Update log state
        if let Some(last_id) = last_log_id {
            let mut log_state = self.log_state.write().await;
            log_state.last_log_id = Some(last_id);
        }

        // Notify Raft that IO is complete
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        // Truncate all entries after the given log_id
        self.log_storage
            .truncate_after(&self.namespace, log_id.index)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Update log state
        let mut log_state = self.log_state.write().await;
        log_state.last_log_id = Some(log_id);

        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        // Compact all entries before the given log_id
        self.log_storage
            .compact_before(&self.namespace, log_id.index)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Update log state
        let mut log_state = self.log_state.write().await;
        log_state.last_purged_log_id = Some(log_id);

        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<GlobalTypeConfig>>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        // Save committed index to memory
        *self.committed.write().await = committed.clone();

        // Persist to storage
        let mut buffer = Vec::new();
        ciborium::into_writer(&committed, &mut buffer).map_err(|e| StorageError::write(&e))?;

        // Use a special key for committed in state namespace
        let committed_key = 1; // Using index 1 for committed
        self.log_storage
            .append(
                &self.state_namespace,
                vec![(committed_key, Bytes::from(buffer))],
            )
            .await
            .map_err(|e| StorageError::write(&e))?;

        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        // Return the cached committed index
        // In production, this should also verify against persistent storage
        Ok(self.committed.read().await.clone())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GlobalTypeConfig>, StorageError<GlobalTypeConfig>> {
        // Get current bounds from storage
        let bounds = self
            .log_storage
            .bounds(&self.namespace)
            .await
            .map_err(|e| StorageError::read(&e))?;

        let mut log_state = self.log_state.write().await;

        if let Some((_first, last)) = bounds {
            // Read the last entry to get its full LogId
            let entries = self
                .log_storage
                .read_range(&self.namespace, last, last + 1)
                .await
                .map_err(|e| StorageError::read(&e))?;

            if let Some((_, data)) = entries.first() {
                let entry: Entry<GlobalTypeConfig> =
                    ciborium::from_reader(data.as_ref()).map_err(|e| StorageError::read(&e))?;
                log_state.last_log_id = Some(entry.log_id);
            }
        }

        Ok(log_state.clone())
    }
}
