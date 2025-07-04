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
use crate::types::{MessagingResponse, TypeConfig};

/// RocksDB-backed storage for consensus
#[derive(Debug, Clone)]
pub struct RocksConsensusStorage {
    db: Arc<DB>,
    state_machine: Arc<RwLock<RocksStateMachine>>,
}

/// RocksDB-backed state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksStateMachine {
    /// Last applied log ID
    pub last_applied_log: Option<LogId<TypeConfig>>,
    /// State machine data (simplified)
    pub data: BTreeMap<String, String>,
    /// Last membership
    pub last_membership: StoredMembership<TypeConfig>,
}

/// Snapshot builder for rocks storage
pub struct RocksSnapshotBuilder {
    last_applied: Option<LogId<TypeConfig>>,
    last_membership: StoredMembership<TypeConfig>,
}

impl RocksConsensusStorage {
    /// Create new RocksDB storage
    pub fn new(db: Arc<DB>) -> Self {
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
                last_membership: StoredMembership::default(),
            })),
        }
    }

    /// Create new RocksDB storage from path
    pub fn new_with_path(db_path: &str) -> ConsensusResult<Self> {
        use rocksdb::{ColumnFamilyDescriptor, DB, Options};

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new("meta", Options::default()),
            ColumnFamilyDescriptor::new("logs", Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, db_path, cfs).map_err(|e| {
            crate::error::ConsensusError::Storage(format!("Failed to open RocksDB: {e}"))
        })?;

        Ok(Self::new(Arc::new(db)))
    }

    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    /// Get a store metadata.
    fn get_meta<M: meta::StoreMeta<TypeConfig>>(
        &self,
    ) -> Result<Option<M::Value>, Box<StorageError<TypeConfig>>> {
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
    fn put_meta<M: meta::StoreMeta<TypeConfig>>(
        &self,
        value: &M::Value,
    ) -> Result<(), Box<StorageError<TypeConfig>>> {
        let json_value = serde_json::to_vec(value).map_err(|e| Box::new(M::write_err(value, e)))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, json_value)
            .map_err(|e| Box::new(M::write_err(value, e)))?;

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for RocksConsensusStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>> {
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

            let entry: Entry<TypeConfig> = serde_json::from_slice(&val).map_err(read_logs_err)?;

            assert_eq!(id, entry.index());

            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<TypeConfig>>, StorageError<TypeConfig>> {
        self.get_meta::<meta::Vote>().map_err(|e| *e)
    }
}

impl RaftLogStorage<TypeConfig> for RocksConsensusStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<TypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
            .next();

        let last_log_id = match last {
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(read_logs_err)?;
                let ent = serde_json::from_slice::<Entry<TypeConfig>>(&entry_bytes)
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

    async fn save_vote(&mut self, vote: &Vote<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
        self.put_meta::<meta::Vote>(vote).map_err(|e| *e)?;
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::write_vote(&e))?;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
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
        log_id: LogId<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>> {
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

    async fn purge(&mut self, log_id: LogId<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
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

impl RaftStateMachine<TypeConfig> for RocksConsensusStorage {
    type SnapshotBuilder = RocksSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log.clone(),
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<MessagingResponse>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut state_machine = self.state_machine.write().await;

        for entry in entries {
            let log_id = entry.log_id().clone();
            state_machine.last_applied_log = Some(log_id.clone());

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(MessagingResponse {
                        sequence: log_id.index,
                        success: true,
                        error: None,
                    });
                }
                EntryPayload::Normal(request) => {
                    // Apply the request to state machine
                    responses.push(apply_request_to_state_machine(
                        &mut state_machine.data,
                        &request,
                        log_id.index,
                    ));
                }
                EntryPayload::Membership(membership) => {
                    state_machine.last_membership =
                        StoredMembership::new(Some(log_id.clone()), membership);
                    responses.push(MessagingResponse {
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
    ) -> Result<Cursor<Vec<u8>>, StorageError<TypeConfig>> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        _snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let mut state_machine = self.state_machine.write().await;
        state_machine.last_applied_log = meta.last_log_id.clone();
        state_machine.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        let state_machine = self.state_machine.read().await;

        if state_machine.last_applied_log.is_none() {
            return Ok(None);
        }

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

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(Vec::new()),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let state_machine = self.state_machine.read().await;
        RocksSnapshotBuilder {
            last_applied: state_machine.last_applied_log.clone(),
            last_membership: state_machine.last_membership.clone(),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let meta = SnapshotMeta {
            last_log_id: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "snapshot-{}",
                self.last_applied.as_ref().map_or(0, |id| id.index)
            ),
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(Vec::new()),
        })
    }
}

/// Metadata storage definitions
mod meta {
    use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError};

    use crate::types::TypeConfig;

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

    impl StoreMeta<TypeConfig> for LastPurged {
        const KEY: &'static str = "last_purged_log_id";
        type Value = openraft::LogId<TypeConfig>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
            ErrorSubject::Store
        }
    }

    impl StoreMeta<TypeConfig> for Vote {
        const KEY: &'static str = "vote";
        type Value = openraft::Vote<TypeConfig>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
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

fn read_logs_err(e: impl Error + 'static) -> StorageError<TypeConfig> {
    StorageError::read_logs(&e)
}
