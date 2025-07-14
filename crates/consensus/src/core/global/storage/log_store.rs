//! Unified global consensus storage implementation using storage adaptors
//!
//! This module provides a generic storage implementation that works with any
//! storage engine, eliminating the need for separate memory and RocksDB implementations.

use openraft::storage::{IOFlushed, LogState, RaftLogStorage, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::global::GlobalConsensusTypeConfig;
use crate::core::global::global_state::GlobalState;
use crate::storage_backends::{
    StorageEngine, StorageKey, StorageNamespace, StorageValue,
    log::{LogEntry as StorageLogEntry, LogStorage},
    traits::{MaintenanceResult, Priority, StorageHints, StorageMetrics, StorageStats},
};

use super::log_types::{GlobalEntryType, GlobalLogMetadata};
use super::snapshot::SnapshotData;

/// Keys for metadata storage
const VOTE_KEY: &[u8] = b"vote";
const LAST_PURGED_KEY: &[u8] = b"last_purged";
const STATE_MACHINE_KEY: &[u8] = b"state_machine";

/// Namespaces for storage
const META_NAMESPACE: &str = "meta";
const LOGS_NAMESPACE: &str = "logs";

/// Unified global consensus storage that works with any storage engine
/// This now only handles log storage responsibilities. State machine functionality
/// has been moved to GlobalStateMachine in core/state_machine/global.rs
#[derive(Clone)]
pub struct GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata>,
{
    /// The underlying storage engine
    storage: Arc<S>,
    /// Global state reference (kept for backward compatibility during transition)
    global_state: Arc<GlobalState>,
    /// State machine cache (kept for backward compatibility during transition)
    state_machine_cache: Arc<RwLock<Option<StateMachineData>>>,
}

/// State machine data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    /// Last applied log ID
    pub last_applied_log: Option<LogId<GlobalConsensusTypeConfig>>,
    /// State machine data
    pub data: BTreeMap<String, String>,
    /// Global state snapshot data
    pub global_state_snapshot: Option<SnapshotData>,
    /// Last membership configuration
    pub last_membership: StoredMembership<GlobalConsensusTypeConfig>,
}

impl<S> GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    /// Create a new unified global storage instance
    pub async fn new(
        storage: Arc<S>,
        global_state: Arc<GlobalState>,
    ) -> Result<Self, StorageError<GlobalConsensusTypeConfig>> {
        // Create necessary namespaces
        let meta_namespace = StorageNamespace::new(META_NAMESPACE);
        let logs_namespace = StorageNamespace::new(LOGS_NAMESPACE);

        // Create namespaces (idempotent operation)
        if let Err(e) = storage.create_namespace(&meta_namespace).await {
            tracing::warn!("Failed to create meta namespace (may already exist): {}", e);
        }
        if let Err(e) = storage.create_namespace(&logs_namespace).await {
            tracing::warn!("Failed to create logs namespace (may already exist): {}", e);
        }

        // Initialize the storage engine
        storage.initialize().await.map_err(|e| {
            StorageError::write(&std::io::Error::other(format!(
                "Failed to initialize storage: {e}"
            )))
        })?;

        let instance = Self {
            storage,
            global_state,
            state_machine_cache: Arc::new(RwLock::new(None)),
        };

        // Load initial state machine data
        instance.load_state_machine().await?;

        Ok(instance)
    }

    /// Load state machine from storage
    async fn load_state_machine(
        &self,
    ) -> Result<StateMachineData, StorageError<GlobalConsensusTypeConfig>> {
        // Check cache first
        if let Some(cached) = self.state_machine_cache.read().await.as_ref() {
            return Ok(cached.clone());
        }

        // Load from storage
        let namespace = StorageNamespace::new(META_NAMESPACE);
        let key = StorageKey::from(STATE_MACHINE_KEY);

        match self.storage.get(&namespace, &key).await {
            Ok(Some(value)) => {
                let state_machine: StateMachineData =
                    ciborium::from_reader(value.as_bytes()).map_err(|e| StorageError::read(&e))?;

                // Update cache
                *self.state_machine_cache.write().await = Some(state_machine.clone());

                Ok(state_machine)
            }
            Ok(None) => {
                // Initialize empty state machine
                let state_machine = StateMachineData {
                    last_applied_log: None,
                    data: BTreeMap::new(),
                    global_state_snapshot: None,
                    last_membership: StoredMembership::default(),
                };

                // Save and cache the new state machine
                self.save_state_machine(&state_machine).await?;
                *self.state_machine_cache.write().await = Some(state_machine.clone());

                Ok(state_machine)
            }
            Err(e) => Err(StorageError::read(&std::io::Error::other(format!(
                "Failed to load state machine: {e}"
            )))),
        }
    }

    /// Save state machine to storage
    async fn save_state_machine(
        &self,
        state_machine: &StateMachineData,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        let namespace = StorageNamespace::new(META_NAMESPACE);
        let key = StorageKey::from(STATE_MACHINE_KEY);

        let mut serialized = Vec::new();
        ciborium::into_writer(state_machine, &mut serialized)
            .map_err(|e| StorageError::write(&e))?;

        self.storage
            .put(&namespace, key, StorageValue::new(serialized))
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to save state machine: {e}"
                )))
            })?;

        Ok(())
    }

    /// Convert OpenRaft log entry to storage log entry
    fn to_storage_log_entry(
        entry: &Entry<GlobalConsensusTypeConfig>,
    ) -> StorageLogEntry<GlobalLogMetadata> {
        let metadata = GlobalLogMetadata {
            term: entry.log_id.leader_id.term,
            leader_node_id: entry.log_id.leader_id.node_id.clone(),
            entry_type: match &entry.payload {
                EntryPayload::Normal(_) => GlobalEntryType::Normal,
                EntryPayload::Membership(_) => GlobalEntryType::Membership,
                EntryPayload::Blank => GlobalEntryType::Blank,
            },
        };

        let data = {
            let mut serialized = Vec::new();
            // Serialize the entire entry for storage
            ciborium::into_writer(&entry, &mut serialized).unwrap_or_else(|e| {
                panic!("Failed to serialize log entry: {e}");
            });
            bytes::Bytes::from(serialized)
        };

        StorageLogEntry {
            index: entry.log_id.index,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            data,
            metadata,
        }
    }

    /// Convert storage log entry back to OpenRaft entry
    fn from_storage_log_entry(
        log_entry: StorageLogEntry<GlobalLogMetadata>,
    ) -> Result<Entry<GlobalConsensusTypeConfig>, StorageError<GlobalConsensusTypeConfig>> {
        // Deserialize the full entry from the data field
        ciborium::from_reader(log_entry.data.as_ref()).map_err(|e| StorageError::read(&e))
    }

    /// Shutdown the storage gracefully
    pub async fn shutdown(&self) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        // Flush any pending operations
        self.storage.flush().await.map_err(|e| {
            StorageError::write(&std::io::Error::other(format!(
                "Failed to flush storage: {e}"
            )))
        })?;

        // Shutdown the storage engine
        self.storage.shutdown().await.map_err(|e| {
            StorageError::write(&std::io::Error::other(format!(
                "Failed to shutdown storage: {e}"
            )))
        })?;

        Ok(())
    }

    /// Perform maintenance operations
    pub async fn maintenance(
        &self,
    ) -> Result<MaintenanceResult, StorageError<GlobalConsensusTypeConfig>> {
        self.storage.maintenance().await.map_err(|e| {
            StorageError::read(&std::io::Error::other(format!(
                "Failed to perform maintenance: {e}"
            )))
        })
    }

    /// Check if storage supports a specific capability
    pub fn supports(&self, capability: &str) -> bool {
        let caps = self.storage.capabilities();
        match capability {
            "atomic_batches" => caps.atomic_batches,
            "efficient_range_scan" => caps.efficient_range_scan,
            "snapshots" => caps.snapshots,
            "streaming" => caps.streaming,
            _ => false,
        }
    }
}

// Storage metrics integration
impl<S> GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + StorageMetrics + Clone + 'static,
{
    /// Get storage statistics
    pub async fn get_stats(&self) -> StorageStats {
        self.storage.get_stats().await
    }

    /// Monitor storage health
    pub async fn check_health(
        &self,
    ) -> Result<HealthStatus, StorageError<GlobalConsensusTypeConfig>> {
        let stats = self.storage.get_stats().await;

        // Check error rate
        if stats.errors > 0 && stats.reads > 0 {
            let error_rate = stats.errors as f64 / stats.reads as f64;
            if error_rate > 0.01 {
                // 1% error rate threshold
                return Ok(HealthStatus::Degraded(format!(
                    "High error rate: {:.2}%",
                    error_rate * 100.0
                )));
            }
        }

        // Check latency
        if let Some(read_latency) = stats.avg_read_latency_ms
            && read_latency > 100.0
        {
            // 100ms threshold
            return Ok(HealthStatus::Slow(format!(
                "High read latency: {read_latency:.2}ms"
            )));
        }

        Ok(HealthStatus::Healthy)
    }
}

// Conditional operations for consensus safety
impl<S> GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    /// Atomically update state machine only if it hasn't changed
    pub async fn update_state_machine_if_unchanged(
        &self,
        expected: &StateMachineData,
        new: StateMachineData,
    ) -> Result<bool, StorageError<GlobalConsensusTypeConfig>> {
        let namespace = StorageNamespace::new(META_NAMESPACE);
        let key = StorageKey::from(STATE_MACHINE_KEY);

        let mut expected_bytes = Vec::new();
        ciborium::into_writer(expected, &mut expected_bytes)
            .map_err(|e| StorageError::write(&e))?;

        let mut new_bytes = Vec::new();
        ciborium::into_writer(&new, &mut new_bytes).map_err(|e| StorageError::write(&e))?;

        let updated = self
            .storage
            .compare_and_swap(
                &namespace,
                &key,
                Some(&StorageValue::new(expected_bytes)),
                StorageValue::new(new_bytes),
            )
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to update state machine: {e}"
                )))
            })?;

        if updated {
            // Update cache
            *self.state_machine_cache.write().await = Some(new);
        }

        Ok(updated)
    }

    /// Atomically create a log entry only if it doesn't exist
    pub async fn create_log_entry_if_absent(
        &self,
        index: u64,
        entry: Entry<GlobalConsensusTypeConfig>,
    ) -> Result<bool, StorageError<GlobalConsensusTypeConfig>> {
        let namespace = StorageNamespace::new(LOGS_NAMESPACE);
        let storage_entry = Self::to_storage_log_entry(&entry);

        // Check if capabilities allow atomic operations
        if self.storage.capabilities().atomic_conditionals {
            // Use put_if_absent for atomic creation
            self.storage
                .put_if_absent(
                    &namespace,
                    StorageKey::from(format!("log_{index:020}").as_bytes()),
                    StorageValue::new(storage_entry.data.to_vec()),
                )
                .await
                .map_err(|e| {
                    StorageError::write(&std::io::Error::other(format!(
                        "Failed to create log entry: {e}"
                    )))
                })
        } else {
            // Fall back to check-then-set (not atomic)
            match self.storage.get_entry(&namespace, index).await {
                Ok(Some(_)) => Ok(false), // Entry already exists
                Ok(None) => {
                    // Entry doesn't exist, try to create it
                    self.storage
                        .append_entry(&namespace, storage_entry)
                        .await
                        .map_err(|e| {
                            StorageError::write(&std::io::Error::other(format!(
                                "Failed to append log entry: {e}"
                            )))
                        })?;
                    Ok(true)
                }
                Err(e) => Err(StorageError::read(&std::io::Error::other(format!(
                    "Failed to check log entry: {e}"
                )))),
            }
        }
    }
}

/// Health status for storage
#[derive(Debug, Clone)]
pub enum HealthStatus {
    /// Storage is healthy
    Healthy,
    /// Storage is degraded but operational
    Degraded(String),
    /// Storage is slow but operational
    Slow(String),
}

impl<S> Debug for GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedGlobalStorage")
            .field("storage", &"<storage_engine>")
            .field("global_state", &"<global_state>")
            .finish()
    }
}

impl<S> RaftLogReader<GlobalConsensusTypeConfig> for GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GlobalConsensusTypeConfig>>, StorageError<GlobalConsensusTypeConfig>>
    {
        let namespace = StorageNamespace::new(LOGS_NAMESPACE);
        let caps = self.storage.capabilities();

        let storage_entries = if caps.efficient_range_scan {
            // Use optimized range scan
            self.storage
                .read_range(&namespace, range)
                .await
                .map_err(|e| {
                    StorageError::read(&std::io::Error::other(format!(
                        "Failed to read log range: {e}"
                    )))
                })?
        } else {
            // For storage without efficient range scan, batch read individual keys
            use std::ops::Bound;
            let start = match range.start_bound() {
                Bound::Included(&n) => n,
                Bound::Excluded(&n) => n + 1,
                Bound::Unbounded => 0,
            };
            let end = match range.end_bound() {
                Bound::Included(&n) => Some(n + 1),
                Bound::Excluded(&n) => Some(n),
                Bound::Unbounded => None,
            };

            // Calculate keys to fetch
            let state = self.storage.get_log_state(&namespace).await.map_err(|e| {
                StorageError::read(&std::io::Error::other(format!(
                    "Failed to get log state: {e}"
                )))
            })?;

            if let Some(log_state) = state {
                let actual_end = end.unwrap_or(log_state.last_index + 1);
                let indices: Vec<u64> = (start..actual_end).collect();

                if indices.is_empty() {
                    vec![]
                } else {
                    // Use batch read if available
                    let keys: Vec<StorageKey> = indices
                        .iter()
                        .map(|&idx| StorageKey::from(format!("log_{idx:020}").as_bytes()))
                        .collect();

                    let values = self
                        .storage
                        .get_batch(&namespace, &keys)
                        .await
                        .map_err(|e| {
                            StorageError::read(&std::io::Error::other(format!(
                                "Failed to batch read logs: {e}"
                            )))
                        })?;

                    // Process batch results
                    let mut entries = Vec::new();
                    for (idx, value) in indices.iter().zip(values.iter()) {
                        if let Some(_val) = value
                            && let Some(entry) = self
                                .storage
                                .get_entry(&namespace, *idx)
                                .await
                                .map_err(|e| {
                                    StorageError::read(&std::io::Error::other(format!(
                                        "Failed to read log entry: {e}"
                                    )))
                                })?
                        {
                            entries.push(entry);
                        }
                    }
                    entries
                }
            } else {
                vec![]
            }
        };

        // Convert storage entries back to OpenRaft entries
        let mut entries = Vec::with_capacity(storage_entries.len());
        for storage_entry in storage_entries {
            let entry = Self::from_storage_log_entry(storage_entry)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<GlobalConsensusTypeConfig>>, StorageError<GlobalConsensusTypeConfig>>
    {
        let namespace = StorageNamespace::new(META_NAMESPACE);
        let key = StorageKey::from(VOTE_KEY);

        match self.storage.get(&namespace, &key).await {
            Ok(Some(value)) => {
                let vote: Vote<GlobalConsensusTypeConfig> =
                    ciborium::from_reader(value.as_bytes()).map_err(|e| StorageError::read(&e))?;
                Ok(Some(vote))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::read(&std::io::Error::other(format!(
                "Failed to read vote: {e}"
            )))),
        }
    }
}

impl<S> RaftLogStorage<GlobalConsensusTypeConfig> for GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<GlobalConsensusTypeConfig>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        let namespace = StorageNamespace::new(META_NAMESPACE);
        let key = StorageKey::from(VOTE_KEY);

        // Vote is critical metadata that's read frequently
        let hints = StorageHints {
            is_batch_write: false,
            access_pattern: crate::storage_backends::traits::AccessPattern::Random,
            allow_eventual_consistency: false,
            priority: Priority::Critical,
        };

        let mut serialized = Vec::new();
        ciborium::into_writer(vote, &mut serialized).map_err(|e| StorageError::write(&e))?;

        self.storage
            .put_with_hints(&namespace, key, StorageValue::new(serialized), hints)
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!("Failed to save vote: {e}")))
            })?;

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
        let namespace = StorageNamespace::new(LOGS_NAMESPACE);
        let caps = self.storage.capabilities();

        // Convert OpenRaft entries to storage log entries
        let storage_entries: Vec<StorageLogEntry<GlobalLogMetadata>> = entries
            .into_iter()
            .map(|entry| Self::to_storage_log_entry(&entry))
            .collect();

        if storage_entries.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        // Use enhanced batch operations if available
        let result = if caps.atomic_batches {
            // Use batch operations for atomicity
            self.storage
                .append_entries(&namespace, storage_entries)
                .await
                .map_err(|e| std::io::Error::other(format!("Failed to append log entries: {e}")))
        } else {
            // Fall back to individual operations
            let mut last_error = None;
            for entry in storage_entries {
                if let Err(e) = self.storage.append_entry(&namespace, entry).await {
                    last_error = Some(e);
                    break;
                }
            }
            if let Some(e) = last_error {
                Err(std::io::Error::other(format!(
                    "Failed to append log entry: {e}"
                )))
            } else {
                Ok(())
            }
        };

        match &result {
            Ok(_) => callback.io_completed(Ok(())),
            Err(e) => callback.io_completed(Err(std::io::Error::new(e.kind(), e.to_string()))),
        }
        result.map_err(|e| StorageError::write(&e))
    }

    async fn truncate(
        &mut self,
        log_id: LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        let namespace = StorageNamespace::new(LOGS_NAMESPACE);

        // Use LogStorage's truncate method for efficient truncation
        self.storage
            .truncate(&namespace, log_id.index)
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to truncate logs after index {}: {}",
                    log_id.index, e
                )))
            })
    }

    async fn purge(
        &mut self,
        log_id: LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        let namespace = StorageNamespace::new(LOGS_NAMESPACE);

        // Use LogStorage's purge method for efficient purging
        self.storage
            .purge(&namespace, log_id.index)
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to purge logs up to index {}: {}",
                    log_id.index, e
                )))
            })?;

        // Save the last purged log ID
        let meta_namespace = StorageNamespace::new(META_NAMESPACE);
        let key = StorageKey::from(LAST_PURGED_KEY);
        let mut serialized = Vec::new();
        ciborium::into_writer(&log_id, &mut serialized).map_err(|e| StorageError::write(&e))?;

        self.storage
            .put(&meta_namespace, key, StorageValue::new(serialized))
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to save last purged log ID: {e}"
                )))
            })?;

        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GlobalConsensusTypeConfig>, StorageError<GlobalConsensusTypeConfig>> {
        let namespace = StorageNamespace::new(LOGS_NAMESPACE);

        // Use LogStorage's get_log_state for O(1) performance
        let storage_state = self.storage.get_log_state(&namespace).await.map_err(|e| {
            StorageError::read(&std::io::Error::other(format!(
                "Failed to get log state: {e}"
            )))
        })?;

        let (last_log_id, last_purged_log_id) = if let Some(state) = storage_state {
            // Get the actual last log entry to extract the full LogId with leader info
            let last_log_id = if state.last_index > 0 {
                match self.storage.get_entry(&namespace, state.last_index).await {
                    Ok(Some(entry)) => {
                        let raft_entry = Self::from_storage_log_entry(entry)?;
                        Some(raft_entry.log_id)
                    }
                    _ => None,
                }
            } else {
                None
            };

            // Get last purged log ID from metadata
            let meta_namespace = StorageNamespace::new(META_NAMESPACE);
            let key = StorageKey::from(LAST_PURGED_KEY);
            let last_purged = match self.storage.get(&meta_namespace, &key).await {
                Ok(Some(value)) => {
                    let log_id: LogId<GlobalConsensusTypeConfig> =
                        ciborium::from_reader(value.as_bytes())
                            .map_err(|e| StorageError::read(&e))?;
                    Some(log_id)
                }
                _ => None,
            };

            (last_log_id, last_purged)
        } else {
            (None, None)
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
}

/// Placeholder snapshot builder for unified storage
pub struct UnifiedSnapshotBuilder<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata>,
{
    #[allow(dead_code)]
    storage: Arc<S>,
}

impl<S> RaftSnapshotBuilder<GlobalConsensusTypeConfig> for UnifiedSnapshotBuilder<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GlobalConsensusTypeConfig>, StorageError<GlobalConsensusTypeConfig>> {
        // For now, return an empty snapshot
        // In a real implementation, this would create a snapshot of the current state
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "empty".to_string(),
            },
            snapshot: Cursor::new(Vec::new()),
        })
    }
}

// RaftStateMachine implementation has been moved to GlobalStateMachine in core/state_machine/global.rs
// This storage now only handles log persistence

// TODO: Remove this entire implementation block after updating the factory and manager
/*
impl<S> RaftStateMachine<GlobalConsensusTypeConfig> for GlobalStorage<S>
where
    S: StorageEngine + LogStorage<GlobalLogMetadata> + Clone + 'static,
{
    type SnapshotBuilder = UnifiedSnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GlobalConsensusTypeConfig>>,
            StoredMembership<GlobalConsensusTypeConfig>,
        ),
        StorageError<GlobalConsensusTypeConfig>,
    > {
        let state_machine = self.load_state_machine().await?;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership,
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GlobalOperationResponse>, StorageError<GlobalConsensusTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut state_machine = self.load_state_machine().await?;

        for entry in entries {
            let log_id = entry.log_id;

            let response = match entry.payload {
                EntryPayload::Blank => GlobalOperationResponse::StreamManagement(
                    crate::operations::handlers::StreamManagementResponse::ConfigUpdated {
                        sequence: log_id.index,
                        stream_name: String::new(),
                        changes: vec!["Blank entry processed".to_string()],
                    },
                ),
                EntryPayload::Normal(request) => {
                    state_machine.last_applied_log = Some(log_id.clone());

                    // For now, just apply the operation directly to state
                    // In production, this would use the handler registry
                    GlobalOperationResponse::StreamManagement(
                        crate::operations::handlers::StreamManagementResponse::ConfigUpdated {
                            sequence: log_id.index,
                            stream_name: String::new(),
                            changes: vec![format!("Applied operation: {:?}", request)],
                        },
                    )
                }
                EntryPayload::Membership(membership) => {
                    state_machine.last_applied_log = Some(log_id.clone());
                    state_machine.last_membership =
                        StoredMembership::new(Some(log_id.clone()), membership);

                    GlobalOperationResponse::Node(
                        crate::operations::handlers::NodeOperationResponse::AssignedToGroup {
                            sequence: log_id.index,
                            node_id: NodeId::from_seed(0), // Placeholder
                            group_id: crate::ConsensusGroupId::initial(),
                            total_groups: 1,
                        },
                    )
                }
            };

            responses.push(response);
        }

        // Save the updated state machine
        self.save_state_machine(&state_machine).await?;
        *self.state_machine_cache.write().await = Some(state_machine);

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        UnifiedSnapshotBuilder {
            storage: self.storage.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<std::io::Cursor<Vec<u8>>, StorageError<GlobalConsensusTypeConfig>> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GlobalConsensusTypeConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        // Deserialize and restore the snapshot data
        let snapshot_bytes = snapshot.into_inner();
        let snapshot_data = SnapshotData::from_bytes(&snapshot_bytes).map_err(|e| {
            StorageError::read(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize snapshot: {}", e),
            ))
        })?;

        // Restore the snapshot to global state
        snapshot_data
            .restore_to_global_state(&self.global_state)
            .await;

        // Update the state machine with the snapshot metadata
        let mut state_machine = self.load_state_machine().await?;
        state_machine.last_applied_log = meta.last_log_id.clone();
        state_machine.last_membership = meta.last_membership.clone();

        // Also store the snapshot data in the state machine for future reference
        state_machine.global_state_snapshot = Some(snapshot_data);

        self.save_state_machine(&state_machine).await?;
        *self.state_machine_cache.write().await = Some(state_machine);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GlobalConsensusTypeConfig>>, StorageError<GlobalConsensusTypeConfig>>
    {
        // Create a snapshot of the current state
        let snapshot_data = SnapshotData::from_global_state(&self.global_state).await;

        let snapshot_bytes = snapshot_data.to_bytes().map_err(|e| {
            StorageError::read(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize snapshot: {}", e),
            ))
        })?;

        let state_machine = self.load_state_machine().await?;

        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_log_id: state_machine.last_applied_log,
                last_membership: state_machine.last_membership,
                snapshot_id: format!("snapshot-{}", chrono::Utc::now().timestamp()),
            },
            snapshot: Cursor::new(snapshot_bytes),
        }))
    }
}
*/
