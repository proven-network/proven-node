#![allow(clippy::significant_drop_in_scrutinee)]

mod error;

use crate::request::Request;
use crate::response::Response;
use crate::{DeserializeError, SerializeError};
pub use error::Error;

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_libsql::Database;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::service_responder::ServiceResponder;
use proven_messaging::stream::InitializedStream;
use proven_store::Store;
use tempfile::NamedTempFile;
use tokio::sync::{oneshot, Mutex, MutexGuard};

static SNAPSHOT_INTERVAL: u64 = 1000;
static SNAPSHOT_MIN_INTERVAL_SECS: u64 = 30;

/// A stream handler that executes SQL queries and migrations.
#[derive(Clone, Debug)]
pub struct SqlServiceHandler<S, SS>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    applied_migrations: Arc<Mutex<Vec<String>>>,
    caught_up_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    database: Arc<Mutex<Option<Database>>>,
    last_snapshot_attempt: Arc<Mutex<Instant>>,
    mutations_since_last_snapshot: Arc<AtomicU64>,
    snapshot_store: SS,
    stream: S,
}

impl<S, SS> SqlServiceHandler<S, SS>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    pub(crate) fn new(
        applied_migrations: Arc<Mutex<Vec<String>>>,
        caught_up_tx: oneshot::Sender<()>,
        snapshot_store: SS,
        stream: S,
    ) -> Self {
        Self {
            applied_migrations,
            caught_up_tx: Arc::new(Mutex::new(Some(caught_up_tx))),
            database: Arc::new(Mutex::new(None)),
            last_snapshot_attempt: Arc::new(Mutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(SNAPSHOT_MIN_INTERVAL_SECS))
                    .unwrap(),
            )),
            mutations_since_last_snapshot: Arc::new(AtomicU64::new(0)),
            snapshot_store,
            stream,
        }
    }

    async fn get_database(&self) -> Result<MutexGuard<'_, Option<Database>>, Error<S, SS>> {
        let mut db_guard = self.database.lock().await;
        if db_guard.is_none() {
            let db_path = NamedTempFile::new()
                .map_err(|e| Error::TempFile(e))?
                .into_temp_path()
                .to_path_buf();

            let database = Database::connect(&db_path)
                .await
                .map_err(Error::<S, SS>::Libsql)
                .unwrap();
            *db_guard = Some(database);
        }

        Ok(db_guard)
    }

    async fn is_caught_up(&self) -> bool {
        self.caught_up_tx.lock().await.is_none()
    }

    async fn save_snapshot(&self, current_seq: u64) -> Result<(), Error<S, SS>> {
        // Limit the frequency of snapshot attempts to prevent thundering herd.
        // (Rolling up a stream will fail if new requests coming in during backup.)
        let mut last_snapshot_time = self.last_snapshot_attempt.lock().await;
        if last_snapshot_time.elapsed() < Duration::from_secs(SNAPSHOT_MIN_INTERVAL_SECS) {
            return Ok(());
        }
        *last_snapshot_time = Instant::now();
        drop(last_snapshot_time);

        let snapshot_store = self.snapshot_store.clone();
        let stream = self.stream.clone();

        let mut db_guard = self.database.lock().await;

        if db_guard.is_none() {
            return Ok(());
        }

        // Take database temporarily to allow backup.
        let mut database = db_guard.take().unwrap();

        let backup_result = database
            .backup(move |path: PathBuf| async move {
                let db_bytes = tokio::fs::read(path)
                    .await
                    .map_err(Error::<S, SS>::TempFile)?;
                let store_key = current_seq.to_string();

                snapshot_store
                    .put(store_key.clone(), Bytes::from(db_bytes))
                    .await
                    .map_err(Error::<S, SS>::SnapshotStore)?;

                stream
                    .rollup(Request::Snapshot(store_key), current_seq)
                    .await
                    .map_err(Error::<S, SS>::Stream)?;

                Ok::<(), Error<S, SS>>(())
            })
            .await?;

        // Always put database back (as long as backup reconnects) - even if rollup fails.
        *db_guard = Some(database);
        drop(db_guard);

        // Reset mutations since last snapshot if backup was successful.
        if backup_result.is_ok() {
            self.mutations_since_last_snapshot
                .store(0, Ordering::SeqCst);
        }

        backup_result
    }
}

#[async_trait]
impl<S, SS>
    ServiceHandler<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    > for SqlServiceHandler<S, SS>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    type Error = Error<S, SS>;
    type ResponseType = Response;
    type ResponseDeserializationError = ciborium::de::Error<std::io::Error>;
    type ResponseSerializationError = ciborium::ser::Error<std::io::Error>;

    #[allow(clippy::too_many_lines)]
    async fn handle<R>(
        &self,
        request: Request,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
            Request,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
            Self::ResponseType,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    {
        Ok(match request {
            Request::Execute(sql, params) => {
                match self
                    .get_database()
                    .await?
                    .as_ref()
                    .unwrap()
                    .execute(&sql, params)
                    .await
                {
                    Ok(affected_rows) => {
                        if self.is_caught_up().await {
                            self.mutations_since_last_snapshot
                                .fetch_add(1, Ordering::SeqCst);

                            if self.mutations_since_last_snapshot.load(Ordering::SeqCst)
                                >= SNAPSHOT_INTERVAL
                            {
                                let current_seq = responder.stream_sequence();
                                let self_clone = self.clone();

                                tokio::spawn(async move {
                                    let _ = self_clone.save_snapshot(current_seq).await;
                                });
                            }
                        }

                        responder.reply(Response::Execute(affected_rows)).await
                    }
                    Err(e) => {
                        responder
                            .reply_and_delete_request(Response::Failed(e))
                            .await
                    }
                }
            }
            Request::ExecuteBatch(sql, params) => {
                match self
                    .get_database()
                    .await?
                    .as_ref()
                    .unwrap()
                    .execute_batch(&sql, params)
                    .await
                {
                    Ok(affected_rows) => {
                        if self.is_caught_up().await {
                            self.mutations_since_last_snapshot
                                .fetch_add(1, Ordering::SeqCst);

                            if self.mutations_since_last_snapshot.load(Ordering::SeqCst)
                                >= SNAPSHOT_INTERVAL
                            {
                                let current_seq = responder.stream_sequence();
                                let self_clone = self.clone();

                                tokio::spawn(async move {
                                    let _ = self_clone.save_snapshot(current_seq).await;
                                });
                            }
                        }

                        responder.reply(Response::ExecuteBatch(affected_rows)).await
                    }
                    Err(e) => {
                        responder
                            .reply_and_delete_request(Response::Failed(e))
                            .await
                    }
                }
            }
            Request::Migrate(sql) => match self
                .get_database()
                .await?
                .as_ref()
                .unwrap()
                .migrate(&sql)
                .await
            {
                Ok(needed_to_run) => {
                    if needed_to_run {
                        self.applied_migrations.lock().await.push(sql);
                        responder.reply(Response::Migrate(needed_to_run)).await
                    } else {
                        responder
                            .reply_and_delete_request(Response::Migrate(needed_to_run))
                            .await
                    }
                }
                Err(e) => {
                    responder
                        .reply_and_delete_request(Response::Failed(e))
                        .await
                }
            },
            Request::Query(sql, params) => {
                match self
                    .get_database()
                    .await?
                    .as_ref()
                    .unwrap()
                    .query(&sql, params)
                    .await
                {
                    Ok(rows) => {
                        tokio::pin!(rows);

                        responder
                            .stream_and_delete_request(rows.map(Response::Row))
                            .await
                    }
                    Err(e) => {
                        responder
                            .reply_and_delete_request(Response::Failed(e))
                            .await
                    }
                }
            }
            Request::Snapshot(snapshot_key) => {
                let db_bytes = self
                    .snapshot_store
                    .get(&snapshot_key)
                    .await
                    .map_err(Error::SnapshotStore)?
                    .ok_or(Error::SnapshotNotFound)?;

                let db_path = NamedTempFile::new()
                    .map_err(|e| Error::TempFile(e))?
                    .into_temp_path()
                    .to_path_buf();

                tokio::fs::write(&db_path, &db_bytes)
                    .await
                    .map_err(Error::TempFile)?;

                let database = Database::connect(&db_path)
                    .await
                    .map_err(Error::<S, SS>::Libsql)
                    .unwrap();

                self.database.lock().await.replace(database);

                responder.no_reply().await
            }
        })
    }

    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        self.caught_up_tx.lock().await.take().map_or(Ok(()), |tx| {
            tx.send(()).map_err(|()| Error::CaughtUpChannelClosed)
        })
    }
}
