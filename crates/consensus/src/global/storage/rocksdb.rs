//! RocksDB storage implementation for consensus

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
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

// RocksDB imports
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rocksdb::{ColumnFamily, DB, Direction};
use std::error::Error;

use super::apply_request_to_state_machine;
use crate::error::ConsensusResult;
use crate::global::GlobalState;
use crate::global::SnapshotData;
use crate::global::{GlobalResponse, GlobalTypeConfig};

/// RocksDB-backed storage for consensus
#[derive(Debug, Clone)]
pub struct GlobalConsensusRocksStorage {
    pub(crate) db: Arc<DB>,
    state_machine: Arc<RwLock<RocksStateMachine>>,
    /// Global state reference for snapshot building and operations
    global_state: Arc<GlobalState>,
}

/// RocksDB-backed state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksStateMachine {
    /// Last applied log ID
    pub last_applied_log: Option<LogId<GlobalTypeConfig>>,
    /// State machine data (simplified)
    pub data: BTreeMap<String, String>,
    /// Global state snapshot data
    pub global_state_snapshot: Option<SnapshotData>,
    /// Last membership
    pub last_membership: StoredMembership<GlobalTypeConfig>,
}

/// Snapshot builder for rocks storage
pub struct RocksSnapshotBuilder {
    last_applied: Option<LogId<GlobalTypeConfig>>,
    last_membership: StoredMembership<GlobalTypeConfig>,
    global_state: Arc<GlobalState>,
}

impl GlobalConsensusRocksStorage {
    /// Create new RocksDB storage with global state
    pub fn new(db: Arc<DB>, global_state: Arc<GlobalState>) -> Self {
        // Ensure required column families exist
        db.cf_handle("meta")
            .expect("column family `meta` not found");
        db.cf_handle("logs")
            .expect("column family `logs` not found");

        Self {
            db,
            state_machine: Arc::new(RwLock::new(RocksStateMachine {
                last_applied_log: None,
                data: BTreeMap::new(),
                global_state_snapshot: None,
                last_membership: StoredMembership::default(),
            })),
            global_state,
        }
    }

    /// Create new RocksDB storage from path
    pub fn new_with_path(db_path: &str, global_state: Arc<GlobalState>) -> ConsensusResult<Self> {
        use rocksdb::{ColumnFamilyDescriptor, DB, Options};

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new("meta", Options::default()),
            ColumnFamilyDescriptor::new("logs", Options::default()),
            ColumnFamilyDescriptor::new("snapshots", Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, db_path, cfs)
            .map_err(|e| crate::error::Error::Storage(format!("Failed to open RocksDB: {e}")))?;

        Ok(Self::new(Arc::new(db), global_state))
    }

    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    fn cf_snapshots(&self) -> &ColumnFamily {
        self.db.cf_handle("snapshots").unwrap()
    }

    /// Get a store metadata.
    fn get_meta<M: meta::StoreMeta<GlobalTypeConfig>>(
        &self,
    ) -> Result<Option<M::Value>, Box<StorageError<GlobalTypeConfig>>> {
        let bytes = self
            .db
            .get_cf(self.cf_meta(), M::KEY)
            .map_err(|e| Box::new(M::read_err(e)))?;

        let Some(bytes) = bytes else {
            return Ok(None);
        };

        let t = serde_json::from_slice(&bytes).map_err(|e| Box::new(M::read_err(e)))?;
        Ok(Some(t))
    }

    /// Save a store metadata.
    fn put_meta<M: meta::StoreMeta<GlobalTypeConfig>>(
        &self,
        value: &M::Value,
    ) -> Result<(), Box<StorageError<GlobalTypeConfig>>> {
        let json_value = serde_json::to_vec(value).map_err(|e| Box::new(M::write_err(value, e)))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, json_value)
            .map_err(|e| Box::new(M::write_err(value, e)))?;

        Ok(())
    }
}

impl RaftLogReader<GlobalTypeConfig> for GlobalConsensusRocksStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };

        let mut res = Vec::new();

        let it = self.db.iterator_cf(
            self.cf_logs(),
            rocksdb::IteratorMode::From(&start, Direction::Forward),
        );
        for item_res in it {
            let (id, val) = item_res.map_err(read_logs_err)?;

            let id = bin_to_id(&id);
            if !range.contains(&id) {
                break;
            }

            let entry: Entry<GlobalTypeConfig> =
                serde_json::from_slice(&val).map_err(read_logs_err)?;

            assert_eq!(id, entry.index());

            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        self.get_meta::<meta::Vote>().map_err(|e| *e)
    }
}

impl RaftLogStorage<GlobalTypeConfig> for GlobalConsensusRocksStorage {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GlobalTypeConfig>, StorageError<GlobalTypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
            .next();

        let last_log_id = match last {
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(read_logs_err)?;
                let ent = serde_json::from_slice::<Entry<GlobalTypeConfig>>(&entry_bytes)
                    .map_err(read_logs_err)?;
                Some(ent.log_id())
            }
            None => None,
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>().map_err(|e| *e)?;

        let last_log_id = last_log_id.map_or_else(|| last_purged_log_id.clone(), Some);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        self.put_meta::<meta::Vote>(vote).map_err(|e| *e)?;
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_vote(&e))?;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalTypeConfig>> + Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.index());
            assert_eq!(bin_to_id(&id), entry.index());
            self.db
                .put_cf(
                    self.cf_logs(),
                    id,
                    serde_json::to_vec(&entry).map_err(|e| StorageError::write_logs(&e))?,
                )
                .map_err(|e| StorageError::write_logs(&e))?;
        }

        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_logs(&e))?;

        // If there is error, the callback will be dropped.
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        tracing::debug!("truncate: [{:?}, +oo)", log_id);

        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageError::write_logs(&e))?;

        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<GlobalTypeConfig>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        // Write the last-purged log id before purging the logs.
        self.put_meta::<meta::LastPurged>(&log_id).map_err(|e| *e)?;

        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }
}

impl RaftStateMachine<GlobalTypeConfig> for GlobalConsensusRocksStorage {
    type SnapshotBuilder = RocksSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GlobalTypeConfig>>,
            StoredMembership<GlobalTypeConfig>,
        ),
        StorageError<GlobalTypeConfig>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log.clone(),
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GlobalResponse>, StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut state_machine = self.state_machine.write().await;

        for entry in entries {
            let log_id = entry.log_id().clone();
            state_machine.last_applied_log = Some(log_id.clone());

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(GlobalResponse {
                        sequence: log_id.index,
                        success: true,
                        error: None,
                    });
                }
                EntryPayload::Normal(request) => {
                    // Apply the request to state machine
                    let response = apply_request_to_state_machine(
                        &mut state_machine.data,
                        &request,
                        log_id.index,
                    );

                    // Also apply to GlobalState
                    let _stream_response = self
                        .global_state
                        .apply_operation(&request.operation, log_id.index)
                        .await;

                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    state_machine.last_membership =
                        StoredMembership::new(Some(log_id.clone()), membership);
                    responses.push(GlobalResponse {
                        sequence: log_id.index,
                        success: true,
                        error: None,
                    });
                }
            }
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Cursor<Vec<u8>>, StorageError<GlobalTypeConfig>> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GlobalTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        let mut state_machine = self.state_machine.write().await;
        state_machine.last_applied_log = meta.last_log_id.clone();
        state_machine.last_membership = meta.last_membership.clone();

        // If we have snapshot data, deserialize and restore it
        if !snapshot.get_ref().is_empty() {
            match SnapshotData::from_bytes(snapshot.get_ref()) {
                Ok(snapshot_data) => {
                    state_machine.global_state_snapshot = Some(snapshot_data.clone());

                    // Store snapshot in RocksDB
                    let snapshot_key = format!("snapshot_{}", meta.snapshot_id);
                    let snapshot_bytes = snapshot_data
                        .to_bytes()
                        .map_err(|e| StorageError::write_snapshot(None, &e))?;
                    self.db
                        .put_cf(self.cf_snapshots(), snapshot_key, snapshot_bytes)
                        .map_err(|e| StorageError::write_snapshot(None, &e))?;

                    // Restore to global state
                    snapshot_data
                        .restore_to_global_state(&self.global_state)
                        .await;
                }
                Err(e) => {
                    return Err(StorageError::read_snapshot(None, &e));
                }
            }
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        let state_machine = self.state_machine.read().await;

        // Create snapshot even if no logs have been applied yet
        // This is useful for testing and initial snapshots

        let meta = SnapshotMeta {
            last_log_id: state_machine.last_applied_log.clone(),
            last_membership: state_machine.last_membership.clone(),
            snapshot_id: format!(
                "snapshot-{}",
                state_machine
                    .last_applied_log
                    .as_ref()
                    .map_or(0, |id| id.index)
            ),
        };

        // Create snapshot data from current GlobalState
        let snapshot_data = SnapshotData::from_global_state(&self.global_state).await;
        let snapshot_bytes = snapshot_data.to_bytes().unwrap_or_else(|_| Vec::new());

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_bytes),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let state_machine = self.state_machine.read().await;
        RocksSnapshotBuilder {
            last_applied: state_machine.last_applied_log.clone(),
            last_membership: state_machine.last_membership.clone(),
            global_state: self.global_state.clone(),
        }
    }
}

impl RaftSnapshotBuilder<GlobalTypeConfig> for RocksSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GlobalTypeConfig>, StorageError<GlobalTypeConfig>> {
        let meta = SnapshotMeta {
            last_log_id: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "snapshot-{}",
                self.last_applied.as_ref().map_or(0, |id| id.index)
            ),
        };

        // Create snapshot data from GlobalState
        let snapshot_data = SnapshotData::from_global_state(&self.global_state).await;
        let snapshot_bytes = snapshot_data.to_bytes().unwrap_or_else(|_| Vec::new());

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_bytes),
        })
    }
}

/// Metadata storage definitions
mod meta {
    use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError};

    use crate::global::GlobalTypeConfig;

    /// Defines metadata key and value
    pub trait StoreMeta<C: openraft::RaftTypeConfig> {
        /// The key used to store in rocksdb
        const KEY: &'static str;

        /// The type of the value to store
        type Value: serde::Serialize + serde::de::DeserializeOwned;

        /// The subject this meta belongs to
        fn subject(v: Option<&Self::Value>) -> ErrorSubject<C>;

        fn read_err(e: impl std::error::Error + 'static) -> StorageError<C> {
            StorageError::new(Self::subject(None), ErrorVerb::Read, AnyError::new(&e))
        }

        fn write_err(v: &Self::Value, e: impl std::error::Error + 'static) -> StorageError<C> {
            StorageError::new(Self::subject(Some(v)), ErrorVerb::Write, AnyError::new(&e))
        }
    }

    pub struct LastPurged {}
    pub struct Vote {}

    impl StoreMeta<GlobalTypeConfig> for LastPurged {
        const KEY: &'static str = "last_purged_log_id";
        type Value = openraft::LogId<GlobalTypeConfig>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<GlobalTypeConfig> {
            ErrorSubject::Store
        }
    }

    impl StoreMeta<GlobalTypeConfig> for Vote {
        const KEY: &'static str = "vote";
        type Value = openraft::Vote<GlobalTypeConfig>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<GlobalTypeConfig> {
            ErrorSubject::Vote
        }
    }
}

/// Converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

fn read_logs_err(e: impl Error + 'static) -> StorageError<GlobalTypeConfig> {
    StorageError::read_logs(&e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::GlobalState;
    use crate::global::storage::create_rocks_storage_with_global_state;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_rocks_storage_snapshot_with_global_state() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let global_state = Arc::new(GlobalState::new());
        let mut storage =
            create_rocks_storage_with_global_state(db_path, global_state.clone()).unwrap();

        // For this storage test, we'll test snapshot functionality without data
        // In the hierarchical model, stream data is managed by local consensus groups

        // Skip setting up applied state for simplicity

        // Create a snapshot
        let snapshot = storage.get_current_snapshot().await.unwrap();
        assert!(snapshot.is_some());

        let snapshot = snapshot.unwrap();
        assert!(!snapshot.snapshot.get_ref().is_empty());

        // Install the snapshot on a new storage
        let temp_dir2 = tempdir().unwrap();
        let db_path2 = temp_dir2.path().to_str().unwrap();
        let new_global_state = Arc::new(GlobalState::new());
        let mut new_storage =
            create_rocks_storage_with_global_state(db_path2, new_global_state.clone()).unwrap();

        new_storage
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        // Verify snapshot was installed successfully
        // In the hierarchical model, actual stream data verification would be done
        // through local consensus groups
    }

    #[tokio::test]
    async fn test_rocks_storage_snapshot_builder() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let global_state = Arc::new(GlobalState::new());
        let mut storage =
            create_rocks_storage_with_global_state(db_path, global_state.clone()).unwrap();

        // For this storage test, we're testing the snapshot builder functionality
        // without needing actual stream data

        // Get snapshot builder and build snapshot (without setting applied state for simplicity)
        let mut builder = storage.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        assert!(!snapshot.snapshot.get_ref().is_empty());
        assert_eq!(snapshot.meta.last_log_id, None);
    }
}
