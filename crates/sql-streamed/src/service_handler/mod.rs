#![allow(clippy::significant_drop_in_scrutinee)]

mod error;

use crate::request::Request;
use crate::response::Response;
use crate::{DeserializeError, SerializeError};
pub use error::Error;

use std::convert::Infallible;
use std::sync::Arc;

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
    snapshot_store: SS,
    _stream: S,
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
            snapshot_store,
            _stream: stream,
        }
    }

    async fn get_database(&self) -> Result<MutexGuard<'_, Option<Database>>, Error<SS>> {
        let mut db_guard = self.database.lock().await;
        if db_guard.is_none() {
            let db_path = NamedTempFile::new()
                .map_err(|e| Error::TempFile(e))?
                .into_temp_path()
                .to_path_buf();

            let database = Database::connect(&db_path)
                .await
                .map_err(Error::<SS>::Libsql)
                .unwrap();
            *db_guard = Some(database);
        }

        Ok(db_guard)
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
    type Error = Error<SS>;
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
                    Ok(affected_rows) => responder.reply(Response::Execute(affected_rows)).await,
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
                    .map_err(Error::<SS>::Libsql)
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
