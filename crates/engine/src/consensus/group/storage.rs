//! Raft log storage implementation for group consensus
//!
//! This module provides a pure storage implementation that only handles
//! log persistence without any business logic.

use std::sync::Arc;
use std::{num::NonZero, ops::RangeBounds};

use openraft::{
    Entry, LogId, StorageError,
    storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage},
};
use tokio::sync::RwLock;

use bytes::Bytes;
use proven_storage::{LogStorage, StorageNamespace};

use super::raft::GroupTypeConfig;
use crate::foundation::types::ConsensusGroupId;

/// Raft log storage - only handles log persistence
///
/// This implementation bridges between OpenRaft's 0-based indexing and our storage's
/// 1-based indexing. All conversions happen at the storage boundary:
/// - When storing: OpenRaft index + 1 = Storage index
/// - When reading: Storage index - 1 = OpenRaft index (handled by stored LogId)
#[derive(Clone)]
pub struct GroupRaftLogStorage<L: LogStorage> {
    /// Log storage backend
    log_storage: Arc<L>,
    /// Group ID for this storage instance
    group_id: ConsensusGroupId,
    /// Namespace for logs
    namespace: StorageNamespace,
}

impl<L: LogStorage> GroupRaftLogStorage<L> {
    /// Create new log storage with group ID
    pub fn new(log_storage: Arc<L>, group_id: ConsensusGroupId) -> Self {
        let namespace = StorageNamespace::new(format!("group_{}_logs", group_id.value()));

        Self {
            log_storage,
            group_id,
            namespace,
        }
    }
}

// Implement RaftLogReader trait
impl<L: LogStorage> RaftLogReader<GroupTypeConfig> for Arc<GroupRaftLogStorage<L>> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GroupTypeConfig>>, StorageError<GroupTypeConfig>> {
        use std::ops::Bound;

        // Convert from OpenRaft's 0-based indexing to our storage's 1-based indexing
        let start = match range.start_bound() {
            Bound::Included(&n) => NonZero::new(n + 1).expect("n + 1 should never be 0"),
            Bound::Excluded(&n) => NonZero::new(n + 2).expect("n + 2 should never be 0"),
            Bound::Unbounded => NonZero::new(1).unwrap(),
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => NonZero::new(n + 2).expect("n + 2 should never be 0"),
            Bound::Excluded(&n) => {
                if n == 0 {
                    // OpenRaft might query with end=Excluded(0), which means "no entries"
                    // In our 1-based system, this would be before index 1
                    NonZero::new(1).unwrap()
                } else {
                    NonZero::new(n + 1).expect("n + 1 should never be 0")
                }
            }
            Bound::Unbounded => NonZero::new(u64::MAX).unwrap(),
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
            let entry: Entry<GroupTypeConfig> =
                ciborium::from_reader(data.as_ref()).map_err(|e| StorageError::read(&e))?;
            result.push(entry);
        }

        Ok(result)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<GroupTypeConfig>>, StorageError<GroupTypeConfig>> {
        // Read directly from storage
        if let Some(vote_data) = self
            .log_storage
            .get_metadata(&self.namespace, "vote")
            .await
            .map_err(|e| StorageError::read(&e))?
        {
            let vote: openraft::Vote<GroupTypeConfig> =
                ciborium::from_reader(vote_data.as_ref()).map_err(|e| StorageError::read(&e))?;
            Ok(Some(vote))
        } else {
            Ok(None)
        }
    }
}

impl<L: LogStorage> RaftLogStorage<GroupTypeConfig> for Arc<GroupRaftLogStorage<L>> {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<GroupTypeConfig>,
    ) -> Result<(), StorageError<GroupTypeConfig>> {
        // Serialize vote
        let mut buffer = Vec::new();
        ciborium::into_writer(vote, &mut buffer).map_err(|e| StorageError::write(&e))?;

        // Save directly to storage
        self.log_storage
            .set_metadata(&self.namespace, "vote", Bytes::from(buffer))
            .await
            .map_err(|e| StorageError::write(&e))?;

        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<GroupTypeConfig>,
    ) -> Result<(), StorageError<GroupTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GroupTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        // Serialize and prepare entries for storage
        let mut storage_entries = Vec::new();

        for entry in &entries {
            let mut buffer = Vec::new();
            ciborium::into_writer(entry, &mut buffer).map_err(|e| StorageError::write(&e))?;
            // Convert from OpenRaft's 0-based indexing to our storage's 1-based indexing
            let storage_index = NonZero::new(entry.log_id.index + 1)
                .expect("entry.log_id.index + 1 should never be 0");
            storage_entries.push((storage_index, Arc::new(Bytes::from(buffer))));
        }

        // Put entries at specific indices using RandomAccessLogStorage
        self.log_storage
            .put_at(&self.namespace, storage_entries)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Note: log state is computed on demand in get_log_state()

        // Notify Raft that IO is complete
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<GroupTypeConfig>,
    ) -> Result<(), StorageError<GroupTypeConfig>> {
        // Truncate all entries after the given log_id
        // Convert from OpenRaft's 0-based indexing to our storage's 1-based indexing
        let storage_index =
            NonZero::new(log_id.index + 1).expect("log_id.index + 1 should never be 0");
        self.log_storage
            .truncate_after(&self.namespace, storage_index)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Note: log state is computed on demand in get_log_state()

        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<GroupTypeConfig>,
    ) -> Result<(), StorageError<GroupTypeConfig>> {
        // Save the last purged log ID to metadata before compacting
        let mut buffer = Vec::new();
        ciborium::into_writer(&log_id, &mut buffer).map_err(|e| StorageError::write(&e))?;

        self.log_storage
            .set_metadata(&self.namespace, "last_purged", Bytes::from(buffer))
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Compact all entries before the given log_id
        // Convert from OpenRaft's 0-based indexing to our storage's 1-based indexing
        let storage_index =
            NonZero::new(log_id.index + 1).expect("log_id.index + 1 should never be 0");
        self.log_storage
            .compact_before(&self.namespace, storage_index)
            .await
            .map_err(|e| StorageError::write(&e))?;

        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<GroupTypeConfig>>,
    ) -> Result<(), StorageError<GroupTypeConfig>> {
        // Serialize committed
        let mut buffer = Vec::new();
        ciborium::into_writer(&committed, &mut buffer).map_err(|e| StorageError::write(&e))?;

        // Save directly to storage
        self.log_storage
            .set_metadata(&self.namespace, "committed", Bytes::from(buffer))
            .await
            .map_err(|e| StorageError::write(&e))?;

        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<GroupTypeConfig>>, StorageError<GroupTypeConfig>> {
        // Read directly from storage
        if let Some(committed_data) = self
            .log_storage
            .get_metadata(&self.namespace, "committed")
            .await
            .map_err(|e| StorageError::read(&e))?
        {
            let committed: Option<LogId<GroupTypeConfig>> =
                ciborium::from_reader(committed_data.as_ref())
                    .map_err(|e| StorageError::read(&e))?;
            Ok(committed)
        } else {
            Ok(None)
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GroupTypeConfig>, StorageError<GroupTypeConfig>> {
        // Get current bounds from storage
        let bounds = self
            .log_storage
            .bounds(&self.namespace)
            .await
            .map_err(|e| StorageError::read(&e))?;

        let mut log_state = LogState::default();

        if let Some((_first, last)) = bounds {
            // Read the last entry to get its full LogId
            let entries = self
                .log_storage
                .read_range(&self.namespace, last, last.saturating_add(1))
                .await
                .map_err(|e| StorageError::read(&e))?;

            if let Some((_, data)) = entries.first() {
                let entry: Entry<GroupTypeConfig> =
                    ciborium::from_reader(data.as_ref()).map_err(|e| StorageError::read(&e))?;
                log_state.last_log_id = Some(entry.log_id.clone());
            }

            // Check if we have purged logs by reading metadata
            if let Some(purged_data) = self
                .log_storage
                .get_metadata(&self.namespace, "last_purged")
                .await
                .map_err(|e| StorageError::read(&e))?
            {
                let last_purged: LogId<GroupTypeConfig> =
                    ciborium::from_reader(purged_data.as_ref())
                        .map_err(|e| StorageError::read(&e))?;
                log_state.last_purged_log_id = Some(last_purged);
            }
        }

        Ok(log_state)
    }
}
