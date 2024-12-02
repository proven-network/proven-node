mod connection;
mod error;
mod request;
mod response;

use std::collections::HashMap;
use std::sync::Arc;

pub use connection::Connection;
pub use error::{Error, HandlerError, HandlerResult, Result};
pub use request::Request;
pub use response::Response;

use async_trait::async_trait;
use bytes::Bytes;
use proven_libsql::Database;
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use proven_store::{Store, Store1, Store2, Store3};
use proven_stream::{HandlerResponse, StreamHandler};
use proven_stream::{Stream, Stream1, Stream2, Stream3};
use tokio::sync::{oneshot, Mutex};

#[derive(Clone, Debug)]
pub struct SqlStreamHandler {
    applied_migrations: Arc<Mutex<Vec<String>>>,
    caught_up_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    database: Database,
}

#[async_trait]
impl StreamHandler for SqlStreamHandler {
    type HandlerError = HandlerError;

    async fn handle(&self, bytes: Bytes) -> HandlerResult<HandlerResponse> {
        let request: Request = bytes.try_into()?;
        println!("Request: {:?}", request);

        let mut headers = HashMap::with_capacity(1);

        let response = match request {
            Request::Execute(sql, params) => {
                let affected_rows = self.database.execute(&sql, params).await?;
                Response::Execute(affected_rows)
            }
            Request::ExecuteBatch(sql, params) => {
                let affected_rows = self.database.execute_batch(&sql, params).await?;
                Response::ExecuteBatch(affected_rows)
            }
            Request::Migrate(sql) => {
                let needed_to_run = self.database.migrate(&sql).await?;
                if needed_to_run {
                    self.applied_migrations.lock().await.push(sql);
                }
                Response::Migrate(needed_to_run)
            }
            Request::Query(sql, params) => {
                // Used in relevent stream implemenetations to remove queries from append-only logs
                headers.insert(
                    "Request-Message-Should-Persist".to_string(),
                    "false".to_string(),
                );

                let rows = self.database.query(&sql, params).await?;
                Response::Query(rows)
            }
        };

        let data = response
            .try_into()
            .map_err(|e| HandlerError::CborSerialize(Arc::new(e)))?;

        Ok(HandlerResponse { headers, data })
    }

    async fn on_caught_up(&self) -> HandlerResult<()> {
        if let Some(tx) = self.caught_up_tx.lock().await.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct StreamedSqlStoreOptions<S: Stream<SqlStreamHandler>, LS: Store> {
    pub leader_store: LS,
    pub local_name: String,
    pub stream: S,
}

#[derive(Clone, Debug)]
pub struct StreamedSqlStore<S: Stream<SqlStreamHandler>, LS: Store> {
    leader_store: LS,
    local_name: String,
    stream: S,
}

impl<S: Stream<SqlStreamHandler>, LS: Store> StreamedSqlStore<S, LS> {
    pub fn new(
        StreamedSqlStoreOptions {
            leader_store,
            local_name,
            stream,
        }: StreamedSqlStoreOptions<S, LS>,
    ) -> Self {
        Self {
            leader_store,
            local_name,
            stream,
        }
    }
}

#[async_trait]
impl<S: Stream<SqlStreamHandler>, LS: Store> SqlStore for StreamedSqlStore<S, LS> {
    type Error = Error<S::Error, LS::Error>;
    type Connection = Connection<S, LS>;

    async fn connect<Q: Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Connection<S, LS>, S::Error, LS::Error> {
        let stream_name = self.stream.name();

        let current_leader = self
            .leader_store
            .get(stream_name.clone())
            .await
            .map_err(Error::LeaderStore)?;

        let current_leader_name = current_leader
            .map(|bytes| String::from_utf8(bytes.to_vec()))
            .transpose()
            .map_err(Error::InvalidLeaderName)?;

        if current_leader_name.is_none() || current_leader_name.as_deref() == Some(&self.local_name)
        {
            self.leader_store
                .put(
                    stream_name,
                    Bytes::from(self.local_name.clone().into_bytes()),
                )
                .await
                .map_err(Error::LeaderStore)?;
        }

        let (caught_up_tx, caught_up_rx) = oneshot::channel();

        let handler = SqlStreamHandler {
            applied_migrations: Arc::new(Mutex::new(Vec::new())),
            caught_up_tx: Arc::new(Mutex::new(Some(caught_up_tx))),
            database: Database::connect(":memory:").await?,
        };

        let handled_migrations = handler.applied_migrations.clone();

        // TODO: properly handle errors in the spawned task
        tokio::spawn({
            let stream = self.stream.clone();
            async move {
                stream.handle(handler).await.unwrap();
            }
        });

        // Wait for the stream to catch up before applying migrations
        caught_up_rx
            .await
            .map_err(|_| Error::CaughtUpChannelClosed)?;

        let applied_migrations = handled_migrations.lock().await.clone();
        for migration in migrations {
            let migration_sql = migration.into();
            if !applied_migrations.contains(&migration_sql) {
                let request = Request::Migrate(migration_sql);
                let bytes: Bytes = request.try_into().unwrap();
                self.stream.request(bytes).await.map_err(Error::Stream)?;
            }
        }

        Ok(Connection::new(self.stream.clone()))
    }
}

macro_rules! impl_scoped_sql_store {
    ($name:ident, $parent:ident, $parent_trait:ident, $stream:ident, $store:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, Debug)]
        pub struct $name<S: $stream<SqlStreamHandler>, LS: $store> {
            leader_store: LS,
            local_name: String,
            stream: S,
        }

        #[async_trait]
        impl<S: $stream<SqlStreamHandler>, LS: $store> $parent_trait for $name<S, LS> {
            type Error = Error<S::Error, LS::Error>;
            type Scoped = $parent<S::Scoped, LS::Scoped>;

            fn scope<Scope: Clone + Into<String> + Send>(&self, scope: Scope) -> Self::Scoped {
                $parent {
                    leader_store: self.leader_store.scope(scope.clone().into()),
                    local_name: self.local_name.clone(),
                    stream: self.stream.scope(scope.into()),
                }
            }
        }
    };
}

impl_scoped_sql_store!(
    StreamedSqlStore1,
    StreamedSqlStore,
    SqlStore1,
    Stream1,
    Store1,
    "A trait representing a single-scoped SQL store with asynchronous operations."
);
impl_scoped_sql_store!(
    StreamedSqlStore2,
    StreamedSqlStore1,
    SqlStore2,
    Stream2,
    Store2,
    "A trait representing a double-scoped SQL store with asynchronous operations."
);
impl_scoped_sql_store!(
    StreamedSqlStore3,
    StreamedSqlStore2,
    SqlStore3,
    Stream3,
    Store3,
    "A trait representing a triple-scoped SQL store with asynchronous operations."
);

#[cfg(test)]
mod tests {
    use super::*;
    use proven_sql::{SqlConnection, SqlParam};
    use proven_store_memory::MemoryStore;
    use proven_stream_memory::MemoryStream;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_store() {
        let result = timeout(Duration::from_secs(5), async {
            let leader_store = MemoryStore::new();
            let stream = MemoryStream::new();

            let sql_store = StreamedSqlStore::new(StreamedSqlStoreOptions {
                leader_store,
                local_name: "my-machine".to_string(),
                stream,
            });

            let connection = sql_store
                .connect(vec![
                    "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
                ])
                .await
                .unwrap();

            let response = connection
                .execute(
                    "INSERT INTO users (id, email) VALUES (?1, ?2)".to_string(),
                    vec![
                        SqlParam::Integer(1),
                        SqlParam::Text("alice@example.com".to_string()),
                    ],
                )
                .await
                .unwrap();

            assert_eq!(response, 1);

            let response = connection
                .query("SELECT id, email FROM users".to_string(), vec![])
                .await
                .unwrap();

            assert_eq!(response.column_count, 2);
            assert_eq!(
                response.column_names,
                vec!["id".to_string(), "email".to_string()]
            );
            assert_eq!(
                response.column_types,
                vec!["INTEGER".to_string(), "TEXT".to_string()]
            );
            assert_eq!(
                response.rows,
                vec![vec![
                    SqlParam::Integer(1),
                    SqlParam::Text("alice@example.com".to_string())
                ]]
            );
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }
}
