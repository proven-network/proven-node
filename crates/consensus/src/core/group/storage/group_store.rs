//! Unified local consensus storage implementation
//!
//! This module provides a unified storage implementation that combines
//! Raft log storage and state machine functionality, similar to GlobalStorage.

use super::log_types::{LocalEntryType, LocalLogMetadata, StreamOperationType};
use crate::{
    ConsensusGroupId,
    core::group::GroupConsensusTypeConfig,
    core::state_machine::{LocalStateMachine as StorageBackedLocalState, group::GroupStateMachine},
    core::stream::StreamMetadata,
    error::{ConsensusResult, Error},
    storage_backends::{
        StorageEngine, StorageValue,
        log::LogStorage,
        traits::{Priority, StorageHints, StorageMetrics},
    },
};
use openraft::{
    Entry, LogId, SnapshotMeta, StorageError, StoredMembership, Vote,
    storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage, Snapshot},
};
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, io::Cursor, ops::RangeBounds, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;

/// Namespaces for Raft storage
mod namespaces {
    use crate::storage_backends::StorageNamespace;

    pub fn logs() -> StorageNamespace {
        StorageNamespace::new("raft_logs")
    }

    pub fn state() -> StorageNamespace {
        StorageNamespace::new("raft_state")
    }

    pub fn snapshots() -> StorageNamespace {
        StorageNamespace::new("raft_snapshots")
    }

    pub fn stream_data(stream_id: &str) -> StorageNamespace {
        StorageNamespace::new(format!("stream_data_{stream_id}"))
    }

    pub fn stream_metadata() -> StorageNamespace {
        StorageNamespace::new("stream_metadata")
    }

    pub fn migrations() -> StorageNamespace {
        StorageNamespace::new("migrations")
    }
}

/// Keys for Raft storage
mod keys {
    use crate::storage_backends::StorageKey;

    pub fn vote() -> StorageKey {
        StorageKey::from("vote")
    }

    pub fn membership() -> StorageKey {
        StorageKey::from("membership")
    }

    pub fn last_applied() -> StorageKey {
        StorageKey::from("last_applied")
    }

    #[allow(dead_code)]
    pub fn log_entry(index: u64) -> StorageKey {
        StorageKey::from(format!("log:{index:016x}").as_str())
    }

    pub fn snapshot_meta() -> StorageKey {
        StorageKey::from("snapshot_meta")
    }

    pub fn snapshot_data() -> StorageKey {
        StorageKey::from("snapshot_data")
    }

    pub fn stream_entry(stream_id: &str, index: u64) -> StorageKey {
        StorageKey::from(format!("stream_{stream_id}_{index:016x}").as_str())
    }

    pub fn stream_metadata(stream_id: &str) -> StorageKey {
        StorageKey::from(format!("metadata_{stream_id}").as_str())
    }

    pub fn migration_lock(stream_id: &str) -> StorageKey {
        StorageKey::from(format!("migration_lock_{stream_id}").as_str())
    }
}

/// Unified local storage that combines Raft storage with state machine
#[derive(Clone)]
pub struct GroupStorage<S: StorageEngine + LogStorage<LocalLogMetadata>> {
    /// The underlying storage engine
    storage: Arc<S>,
    /// Group ID for this storage instance
    group_id: ConsensusGroupId,
    /// State machine implementation
    state_machine: Arc<GroupStateMachine>,
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata>> GroupStorage<S> {
    /// Create a new unified local storage instance with direct state machine
    pub async fn new(
        storage: Arc<S>,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self> {
        // Create namespaces
        storage
            .create_namespace(&namespaces::logs())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;
        storage
            .create_namespace(&namespaces::state())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;
        storage
            .create_namespace(&namespaces::snapshots())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;

        // Initialize the storage engine
        storage
            .initialize()
            .await
            .map_err(|e| Error::storage(format!("Failed to initialize storage: {e}")))?;

        // Log initialization
        debug!(
            group_id = ?group_id,
            "Local group storage initialized successfully"
        );

        // Create state machine from the local state
        let storage_state_arc = {
            let state = local_state.read().await;
            Arc::new(state.clone())
        };
        let state_machine = Arc::new(GroupStateMachine::new(storage_state_arc, group_id));

        Ok(Self {
            storage,
            group_id,
            state_machine,
        })
    }

    /// Create a new unified local storage instance with handler-based state machine
    pub async fn new_with_state_machine(
        storage: Arc<S>,
        group_id: ConsensusGroupId,
        state_machine: Arc<GroupStateMachine>,
    ) -> ConsensusResult<Self> {
        // Create namespaces
        storage
            .create_namespace(&namespaces::logs())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;
        storage
            .create_namespace(&namespaces::state())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;
        storage
            .create_namespace(&namespaces::snapshots())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;

        // Initialize the storage engine
        storage
            .initialize()
            .await
            .map_err(|e| Error::storage(format!("Failed to initialize storage: {e}")))?;

        // Log initialization
        debug!(
            group_id = ?group_id,
            "Local group storage initialized with handler-based state machine"
        );

        Ok(Self {
            storage,
            group_id,
            state_machine,
        })
    }

    /// Shutdown the storage gracefully
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        debug!(
            group_id = ?self.group_id,
            "Shutting down local group storage"
        );

        // Flush any pending operations
        self.storage
            .flush()
            .await
            .map_err(|e| Error::storage(format!("Failed to flush storage: {e}")))?;

        // Shutdown the storage engine
        self.storage
            .shutdown()
            .await
            .map_err(|e| Error::storage(format!("Failed to shutdown storage: {e}")))?;

        Ok(())
    }

    /// Perform maintenance operations
    pub async fn maintenance(&self) -> ConsensusResult<MaintenanceReport> {
        let result = self
            .storage
            .maintenance()
            .await
            .map_err(|e| Error::storage(format!("Maintenance failed: {e}")))?;

        Ok(MaintenanceReport {
            group_id: self.group_id,
            bytes_reclaimed: result.bytes_reclaimed,
            entries_compacted: result.entries_compacted,
            duration_ms: result.duration_ms,
            details: result.details,
        })
    }

    /// Check storage capabilities and optimize for local consensus
    pub fn optimize_for_local_consensus(&self) -> LocalOptimizationStrategy {
        let caps = self.storage.capabilities();

        LocalOptimizationStrategy {
            // Local consensus benefits from atomic batches
            use_batch_commits: caps.atomic_batches,
            // Stream operations need efficient scanning
            use_range_optimization: caps.efficient_range_scan,
            // Local groups may have large stream data
            use_streaming: caps.streaming,
            // Cache hot streams
            cache_enabled: caps.caching,
            // For S3-backed storage, different strategy
            is_eventually_consistent: caps.eventual_consistency,
        }
    }

    /// Get recommended batch size based on capabilities
    pub fn recommended_batch_size(&self) -> usize {
        let caps = self.storage.capabilities();

        if caps.eventual_consistency {
            // Larger batches for eventually consistent storage
            1000
        } else if caps.atomic_batches {
            // Medium batches for atomic storage
            100
        } else {
            // Small batches for basic storage
            10
        }
    }
}

// Batch operations for stream processing
impl<S: StorageEngine + LogStorage<LocalLogMetadata>> GroupStorage<S> {
    /// Batch read multiple stream entries
    pub async fn read_stream_batch(
        &self,
        stream_id: &str,
        indices: &[u64],
    ) -> ConsensusResult<Vec<Option<StreamEntry>>> {
        let namespace = namespaces::stream_data(stream_id);
        let keys: Vec<crate::storage_backends::StorageKey> = indices
            .iter()
            .map(|idx| keys::stream_entry(stream_id, *idx))
            .collect();

        let values = self
            .storage
            .get_batch(&namespace, &keys)
            .await
            .map_err(|e| Error::storage(format!("Batch read failed: {e}")))?;

        // Convert to stream entries
        values
            .into_iter()
            .map(|opt_val| {
                opt_val
                    .map(|val| {
                        ciborium::from_reader(val.as_bytes())
                            .map_err(|e| Error::storage(format!("Deserialize failed: {e}")))
                    })
                    .transpose()
            })
            .collect()
    }

    /// Batch append stream entries with proper hints
    pub async fn append_stream_batch(
        &self,
        stream_id: &str,
        entries: Vec<StreamEntry>,
    ) -> ConsensusResult<()> {
        let namespace = namespaces::stream_data(stream_id);

        // Stream data is written sequentially and rarely updated
        let _hints = StorageHints {
            is_batch_write: true,
            access_pattern: crate::storage_backends::traits::AccessPattern::Sequential,
            allow_eventual_consistency: false, // Consensus requires strong consistency
            priority: Priority::High,
        };

        // Use batch operations
        let mut batch = self.storage.create_write_batch();
        for entry in entries {
            let key = keys::stream_entry(stream_id, entry.index);
            let value = crate::storage_backends::StorageValue::new(serialize(&entry)?);

            // Would need to extend WriteBatch API to support hints
            batch.put(namespace.clone(), key, value);
        }

        self.storage
            .write_batch(batch)
            .await
            .map_err(|e| Error::storage(format!("Batch write failed: {e}")))?;

        Ok(())
    }
}

// Storage metrics for group monitoring
impl<S> GroupStorage<S>
where
    S: StorageEngine + LogStorage<LocalLogMetadata> + StorageMetrics + Clone + 'static,
{
    /// Get storage statistics for this group
    pub async fn get_group_stats(&self) -> GroupStorageStats {
        let stats = self.storage.get_stats().await;

        GroupStorageStats {
            group_id: self.group_id,
            total_reads: stats.reads,
            total_writes: stats.writes,
            cache_hit_rate: calculate_hit_rate(stats.cache_hits, stats.cache_misses),
            avg_read_latency_ms: stats.avg_read_latency_ms,
            avg_write_latency_ms: stats.avg_write_latency_ms,
            error_count: stats.errors,
            bytes_read: stats.bytes_read,
            bytes_written: stats.bytes_written,
        }
    }

    /// Monitor hot streams within the group
    pub async fn identify_hot_streams(&self, _threshold_ops_per_sec: f64) -> Vec<String> {
        // Track per-stream operation rates
        // This would require enhanced metrics with labels/tags
        vec![]
    }

    /// Get health status for the group
    pub async fn check_health(&self) -> GroupHealthStatus {
        let stats = self.get_group_stats().await;

        if stats.error_count > 0 && stats.total_reads > 0 {
            let error_rate = stats.error_count as f64 / stats.total_reads as f64;
            if error_rate > 0.01 {
                return GroupHealthStatus::Unhealthy(format!(
                    "High error rate: {:.2}%",
                    error_rate * 100.0
                ));
            }
        }

        if let Some(latency) = stats.avg_read_latency_ms
            && latency > 50.0
        {
            // 50ms threshold for local consensus
            return GroupHealthStatus::Degraded(format!("High read latency: {latency:.2}ms"));
        }

        GroupHealthStatus::Healthy
    }
}

/// Group storage statistics
#[derive(Debug, Clone)]
pub struct GroupStorageStats {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Total reads
    pub total_reads: u64,
    /// Total writes
    pub total_writes: u64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Average read latency
    pub avg_read_latency_ms: Option<f64>,
    /// Average write latency
    pub avg_write_latency_ms: Option<f64>,
    /// Error count
    pub error_count: u64,
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
}

/// Group health status
#[derive(Debug, Clone)]
pub enum GroupHealthStatus {
    /// Healthy
    Healthy,
    /// Degraded performance
    Degraded(String),
    /// Unhealthy
    Unhealthy(String),
}

/// Calculate cache hit rate
fn calculate_hit_rate(hits: Option<u64>, misses: Option<u64>) -> f64 {
    match (hits, misses) {
        (Some(h), Some(m)) if h + m > 0 => h as f64 / (h + m) as f64,
        _ => 0.0,
    }
}

// Conditional operations for concurrent stream access
impl<S: StorageEngine + LogStorage<LocalLogMetadata>> GroupStorage<S> {
    /// Atomically update stream metadata only if unchanged
    pub async fn update_stream_metadata_if_unchanged(
        &self,
        stream_id: &str,
        expected: &StreamMetadata,
        new: StreamMetadata,
    ) -> ConsensusResult<bool> {
        let namespace = namespaces::stream_metadata();
        let key = keys::stream_metadata(stream_id);

        let expected_val = StorageValue::new(serialize(expected)?);
        let new_val = StorageValue::new(serialize(&new)?);

        let updated = self
            .storage
            .compare_and_swap(&namespace, &key, Some(&expected_val), new_val)
            .await
            .map_err(|e| Error::storage(format!("CAS failed: {e}")))?;

        if updated {
            debug!(
                stream_id = stream_id,
                "Successfully updated stream metadata"
            );
        }

        Ok(updated)
    }

    /// Atomically claim a stream for migration
    pub async fn claim_stream_for_migration(
        &self,
        stream_id: &str,
        migration_id: &str,
    ) -> ConsensusResult<bool> {
        let namespace = namespaces::migrations();
        let key = keys::migration_lock(stream_id);
        let value = StorageValue::new(migration_id.as_bytes().to_vec());

        // Only succeed if no other migration is in progress
        let claimed = self
            .storage
            .put_if_absent(&namespace, key, value)
            .await
            .map_err(|e| Error::storage(format!("Failed to claim stream: {e}")))?;

        Ok(claimed)
    }
}

/// Simplified stream metadata for local storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStorageStreamMetadata {
    /// Stream name
    pub name: String,
    /// Last sequence number
    pub last_sequence: u64,
    /// Message count
    pub message_count: u64,
}

/// Stream entry structure (placeholder)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    /// Entry index
    pub index: u64,
    /// Entry data
    pub data: Vec<u8>,
}

/// Serialize helper
fn serialize<T: Serialize>(value: &T) -> ConsensusResult<Vec<u8>> {
    let mut buf = Vec::new();
    ciborium::into_writer(value, &mut buf)
        .map_err(|e| Error::storage(format!("Serialize failed: {e}")))?;
    Ok(buf)
}

/// Maintenance report for a local group
#[derive(Debug, Clone)]
pub struct MaintenanceReport {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Bytes reclaimed
    pub bytes_reclaimed: u64,
    /// Entries compacted
    pub entries_compacted: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Additional details
    pub details: String,
}

/// Local optimization strategy based on storage capabilities
#[derive(Debug, Clone)]
pub struct LocalOptimizationStrategy {
    /// Use batch commits for better performance
    pub use_batch_commits: bool,
    /// Use range optimization for stream queries
    pub use_range_optimization: bool,
    /// Use streaming for large data
    pub use_streaming: bool,
    /// Cache enabled
    pub cache_enabled: bool,
    /// Storage is eventually consistent
    pub is_eventually_consistent: bool,
}

/// Stored vote structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredVote {
    term: u64,
    node_id: NodeId,
    committed: bool,
}

impl From<&Vote<GroupConsensusTypeConfig>> for StoredVote {
    fn from(vote: &Vote<GroupConsensusTypeConfig>) -> Self {
        Self {
            term: vote.leader_id.term,
            node_id: vote.leader_id.node_id.clone(),
            committed: vote.committed,
        }
    }
}

impl StoredVote {
    fn to_vote(&self) -> Vote<GroupConsensusTypeConfig> {
        Vote {
            leader_id: openraft::vote::leader_id_adv::LeaderId {
                term: self.term,
                node_id: self.node_id.clone(),
            },
            committed: self.committed,
        }
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone> Debug for GroupStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupStorage")
            .field("storage", &"<storage_engine>")
            .field("group_id", &self.group_id)
            .finish()
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone>
    RaftLogReader<GroupConsensusTypeConfig> for GroupStorage<S>
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GroupConsensusTypeConfig>>, StorageError<GroupConsensusTypeConfig>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        // Use direct storage access to read entries
        let mut entries = Vec::new();

        for index in start..end {
            match self.storage.get_entry(&namespaces::logs(), index).await {
                Ok(Some(log_entry)) => {
                    // Extract the OpenRaft Entry from our LogEntry wrapper
                    let entry: Entry<GroupConsensusTypeConfig> =
                        ciborium::from_reader(log_entry.data.as_ref())
                            .map_err(|e| StorageError::read_logs(&e))?;
                    entries.push(entry);
                }
                Ok(None) => break, // No more entries
                Err(e) => return Err(StorageError::read_logs(&e)),
            }
        }

        Ok(entries)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<GroupConsensusTypeConfig>>, StorageError<GroupConsensusTypeConfig>>
    {
        match self.storage.get(&namespaces::state(), &keys::vote()).await {
            Ok(Some(value)) => {
                let stored: StoredVote = ciborium::from_reader(value.as_bytes())
                    .map_err(|e| StorageError::read_vote(&e))?;
                Ok(Some(stored.to_vote()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::read_vote(&e)),
        }
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone>
    RaftLogStorage<GroupConsensusTypeConfig> for GroupStorage<S>
{
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>> {
        let stored = StoredVote::from(vote);
        let mut buffer = Vec::new();
        ciborium::into_writer(&stored, &mut buffer).map_err(|e| StorageError::write_vote(&e))?;

        // Vote is critical for consensus correctness
        let hints = StorageHints {
            is_batch_write: false,
            access_pattern: crate::storage_backends::traits::AccessPattern::Random,
            allow_eventual_consistency: false,
            priority: Priority::Critical, // Highest priority
        };

        self.storage
            .put_with_hints(
                &namespaces::state(),
                keys::vote(),
                StorageValue::new(buffer),
                hints,
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
        callback: IOFlushed<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GroupConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Create a batch for efficient writing
        let mut batch = self.storage.create_batch().await;

        for entry in entries {
            // Create metadata for the log entry
            let metadata = LocalLogMetadata {
                term: entry.log_id.leader_id.term,
                leader_node_id: entry.log_id.leader_id.node_id.clone(),
                entry_type: match &entry.payload {
                    openraft::EntryPayload::Normal(_) => LocalEntryType::StreamOperation {
                        stream_id: String::new(), // TODO: Extract from request
                        operation: StreamOperationType::Publish,
                    },
                    openraft::EntryPayload::Membership(_) => LocalEntryType::MembershipChange,
                    openraft::EntryPayload::Blank => LocalEntryType::Empty,
                },
                group_id: self.group_id,
            };

            // Serialize the entry for storage
            let mut buffer = Vec::new();
            if let Err(e) = ciborium::into_writer(&entry, &mut buffer) {
                callback.io_completed(Err(std::io::Error::other(format!(
                    "Failed to serialize entry: {e}"
                ))));
                return Err(StorageError::write_logs(&e));
            }

            // Create log entry for LogStorage
            let log_entry = crate::storage_backends::log::LogEntry {
                index: entry.log_id.index,
                timestamp: chrono::Utc::now().timestamp() as u64,
                data: bytes::Bytes::from(buffer),
                metadata,
            };

            batch.append(namespaces::logs(), log_entry);
        }

        // Apply the batch
        if let Err(e) = self.storage.apply_batch(batch).await {
            callback.io_completed(Err(std::io::Error::other(format!("Storage error: {e}"))));
            return Err(StorageError::write_logs(&e));
        }

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>> {
        // Use LogStorage's truncate method
        self.storage
            .truncate(&namespaces::logs(), log_id.index)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>> {
        // Use LogStorage's purge method
        self.storage
            .purge(&namespaces::logs(), log_id.index)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GroupConsensusTypeConfig>, StorageError<GroupConsensusTypeConfig>> {
        // Use LogStorage's get_log_state method
        let state = self
            .storage
            .get_log_state(&namespaces::logs())
            .await
            .map_err(|e| StorageError::read_logs(&e))?;

        // Convert to OpenRaft LogState
        let last_log_id = if let Some(_state) = state {
            // Get the last entry to extract the full LogId
            if let Some(last_entry) = self
                .storage
                .get_last_entry(&namespaces::logs())
                .await
                .map_err(|e| StorageError::read_logs(&e))?
            {
                // Deserialize to get the OpenRaft Entry
                let entry: Entry<GroupConsensusTypeConfig> =
                    ciborium::from_reader(last_entry.data.as_ref())
                        .map_err(|e| StorageError::read_logs(&e))?;
                Some(entry.log_id)
            } else {
                None
            }
        } else {
            None
        };

        Ok(LogState {
            last_purged_log_id: None, // TODO: Track purged log ID
            last_log_id,
        })
    }
}

/// Snapshot builder for local storage
pub struct LocalSnapshotBuilder<S: StorageEngine + LogStorage<LocalLogMetadata>> {
    #[allow(dead_code)]
    storage: Arc<S>,
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata>> LocalSnapshotBuilder<S> {
    /// Create a new snapshot builder
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone>
    openraft::RaftSnapshotBuilder<GroupConsensusTypeConfig> for LocalSnapshotBuilder<S>
{
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GroupConsensusTypeConfig>, StorageError<GroupConsensusTypeConfig>> {
        // For now, return an empty snapshot
        // In a real implementation, this would serialize the state machine state
        let data = Vec::new();
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "empty".to_string(),
            },
            snapshot: Cursor::new(data),
        };

        Ok(snapshot)
    }
}

// RaftStateMachine implementation has been moved to GroupStateMachine in core/state_machine/group.rs
// This storage now only handles log persistence
// TODO: Remove this entire implementation block after updating the factory and manager
/*
impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone> RaftStateMachine<GroupTypeConfig>
    for GroupStorage<S>
{
    type SnapshotBuilder = LocalSnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GroupTypeConfig>>,
            StoredMembership<GroupTypeConfig>,
        ),
        StorageError<GroupTypeConfig>,
    > {
        // Get last applied log ID
        let last_applied = match self
            .storage
            .get(&namespaces::state(), &keys::last_applied())
            .await
        {
            Ok(Some(value)) => {
                let log_id: LogId<GroupTypeConfig> = ciborium::from_reader(value.as_bytes())
                    .map_err(|e| StorageError::read_state_machine(&e))?;
                Some(log_id)
            }
            Ok(None) => None,
            Err(e) => return Err(StorageError::read_state_machine(&e)),
        };

        // Get membership
        let membership = match self
            .storage
            .get(&namespaces::state(), &keys::membership())
            .await
        {
            Ok(Some(value)) => ciborium::from_reader(value.as_bytes())
                .map_err(|e| StorageError::read_state_machine(&e))?,
            Ok(None) => StoredMembership::default(),
            Err(e) => return Err(StorageError::read_state_machine(&e)),
        };

        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GroupStreamOperationResponse>, StorageError<GroupTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GroupTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut last_applied = None;

        for entry in entries {
            // Process the entry payload
            match entry.payload {
                openraft::EntryPayload::Normal(ref request) => {
                    debug!(
                        "Processing local request for group {:?}: {:?}",
                        self.group_id,
                        request.operation_name()
                    );

                    // Delegate to state machine
                    let response = self.state_machine.apply_entry(&entry).await.map_err(|e| {
                        StorageError::write_state_machine(&std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e,
                        ))
                    })?;

                    responses.push(response);
                }
                openraft::EntryPayload::Membership(ref membership) => {
                    // Process membership changes
                    let mut buffer = Vec::new();
                    ciborium::into_writer(membership, &mut buffer)
                        .map_err(|e| StorageError::write_state_machine(&e))?;

                    self.storage
                        .put(
                            &namespaces::state(),
                            keys::membership(),
                            StorageValue::new(buffer),
                        )
                        .await
                        .map_err(|e| StorageError::write_state_machine(&e))?;

                    // Create a response for membership change
                    let response = GroupStreamOperationResponse::Maintenance(
                        crate::operations::handlers::MaintenanceOperationResponse::MembershipUpdated {
                            sequence: entry.log_id.index,
                            group_id: self.group_id,
                            membership_config: format!("{:?}", membership),
                        },
                    );
                    responses.push(response);
                }
                openraft::EntryPayload::Blank => {
                    // No operation - this is used for heartbeats
                    let response = GroupStreamOperationResponse::Maintenance(
                        crate::operations::handlers::MaintenanceOperationResponse::Heartbeat {
                            sequence: entry.log_id.index,
                            group_id: self.group_id,
                        },
                    );
                    responses.push(response);
                }
            }

            last_applied = Some(entry.log_id);
        }

        // Update last applied
        if let Some(log_id) = last_applied {
            let mut buffer = Vec::new();
            ciborium::into_writer(&log_id, &mut buffer)
                .map_err(|e| StorageError::write_state_machine(&e))?;

            self.storage
                .put(
                    &namespaces::state(),
                    keys::last_applied(),
                    StorageValue::new(buffer),
                )
                .await
                .map_err(|e| StorageError::write_state_machine(&e))?;
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LocalSnapshotBuilder::new(self.storage.clone())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Cursor<Vec<u8>>, StorageError<GroupTypeConfig>> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GroupTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), StorageError<GroupTypeConfig>> {
        // Store snapshot metadata
        let mut buffer = Vec::new();
        ciborium::into_writer(meta, &mut buffer)
            .map_err(|e| StorageError::write_state_machine(&e))?;

        self.storage
            .put(
                &namespaces::snapshots(),
                keys::snapshot_meta(),
                StorageValue::new(buffer),
            )
            .await
            .map_err(|e| StorageError::write_state_machine(&e))?;

        // Store snapshot data
        let data = snapshot.into_inner();
        self.storage
            .put(
                &namespaces::snapshots(),
                keys::snapshot_data(),
                StorageValue::new(data),
            )
            .await
            .map_err(|e| StorageError::write_state_machine(&e))?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GroupTypeConfig>>, StorageError<GroupTypeConfig>> {
        // Get snapshot metadata
        let meta = match self
            .storage
            .get(&namespaces::snapshots(), &keys::snapshot_meta())
            .await
        {
            Ok(Some(value)) => {
                let meta: SnapshotMeta<GroupTypeConfig> =
                    ciborium::from_reader(value.as_bytes())
                        .map_err(|e| StorageError::read_snapshot(None, &e))?;
                meta
            }
            Ok(None) => return Ok(None),
            Err(e) => return Err(StorageError::read_snapshot(None, &e)),
        };

        // Get snapshot data
        let data = match self
            .storage
            .get(&namespaces::snapshots(), &keys::snapshot_data())
            .await
        {
            Ok(Some(value)) => value.as_bytes().to_vec(),
            Ok(None) => return Ok(None),
            Err(e) => return Err(StorageError::read_snapshot(None, &e)),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}
*/
