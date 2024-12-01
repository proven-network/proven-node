mod connection;
mod error;
mod request;
mod response;

use std::sync::Arc;

pub use connection::Connection;
pub use error::{Error, HandlerError, HandlerResult, Result};
use proven_stream_memory::MemoryStream;
use proven_stream_nats::NatsStream;
pub use request::Request;
pub use response::Response;

use async_trait::async_trait;
use bytes::Bytes;
use proven_libsql::Database;
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use proven_store::{Store, Store1, Store2, Store3};
use proven_stream::StreamHandler;
use proven_stream::{Stream, Stream1, Stream2, Stream3};
use tokio::sync::{oneshot, Mutex};

#[derive(Clone)]
pub struct SqlStreamHandler {
    applied_migrations: Arc<Mutex<Vec<String>>>,
    caught_up_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    database: Database,
}

#[async_trait]
impl StreamHandler<MemoryStream<Self>> for SqlStreamHandler {
    type HandlerError = HandlerError;

    async fn handle_request(&self, bytes: Bytes) -> HandlerResult<Bytes> {
        let request: Request = bytes.try_into()?;
        println!("Request: {:?}", request);

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
                let rows = self.database.query(&sql, params).await?;
                Response::Query(rows)
            }
        };

        response
            .try_into()
            .map_err(|e| HandlerError::CborSerialize(Arc::new(e)))
    }

    async fn on_caught_up(&self) -> HandlerResult<()> {
        if let Some(tx) = self.caught_up_tx.lock().await.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

#[async_trait]
impl StreamHandler<NatsStream<Self>> for SqlStreamHandler {
    type HandlerError = HandlerError;

    async fn handle_request(&self, bytes: Bytes) -> HandlerResult<Bytes> {
        let request: Request = bytes.try_into()?;
        println!("Request: {:?}", request);

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
                let rows = self.database.query(&sql, params).await?;
                Response::Query(rows)
            }
        };

        response
            .try_into()
            .map_err(|e| HandlerError::CborSerialize(Arc::new(e)))
    }

    async fn on_caught_up(&self) -> HandlerResult<()> {
        if let Some(tx) = self.caught_up_tx.lock().await.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct StreamedSqlStoreOptions<LS: Store, ST: Stream<SqlStreamHandler>>
where
    SqlStreamHandler: StreamHandler<ST>,
{
    pub leader_store: LS,
    pub local_name: String,
    pub stream: ST,
}

#[derive(Clone)]
pub struct StreamedSqlStore<LS: Store, ST: Stream<SqlStreamHandler>>
where
    SqlStreamHandler: StreamHandler<ST>,
{
    leader_store: LS,
    local_name: String,
    stream: ST,
}

impl<LS: Store, ST: Stream<SqlStreamHandler>> StreamedSqlStore<LS, ST>
where
    SqlStreamHandler: StreamHandler<ST>,
{
    pub fn new(
        StreamedSqlStoreOptions {
            leader_store,
            local_name,
            stream,
        }: StreamedSqlStoreOptions<LS, ST>,
    ) -> Self {
        Self {
            leader_store,
            local_name,
            stream,
        }
    }
}

#[async_trait]
impl<LS: Store, ST: Stream<SqlStreamHandler>> SqlStore for StreamedSqlStore<LS, ST>
where
    SqlStreamHandler: StreamHandler<ST>,
    ST::Request: From<Bytes> + Into<Bytes>,
    ST::Response: From<Bytes> + Into<Bytes>,
{
    type Error = Error<ST::Error, LS::Error>;
    type Connection = Connection<ST, LS>;

    async fn connect<Q: Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Connection<ST, LS>, ST::Error, LS::Error> {
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
                self.stream
                    .request(bytes.into())
                    .await
                    .map_err(Error::Stream)?;
            }
        }

        Ok(Connection::new(self.stream.clone()))
    }
}

#[derive(Clone)]
pub struct StreamedSqlStore1<LS: Store1, ST: Stream1<SqlStreamHandler>>
where
    SqlStreamHandler: StreamHandler<ST::Scoped>,
{
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store1, ST: Stream1<SqlStreamHandler>> SqlStore1 for StreamedSqlStore1<LS, ST>
where
    SqlStreamHandler: StreamHandler<ST::Scoped>,
    ST::Request: From<Bytes> + Into<Bytes>,
    ST::Response: From<Bytes> + Into<Bytes>,
    <ST::Scoped as Stream<SqlStreamHandler>>::Request: From<Bytes> + Into<Bytes>,
    <ST::Scoped as Stream<SqlStreamHandler>>::Response: From<Bytes> + Into<Bytes>,
{
    type Error = Error<ST::Error, LS::Error>;
    type Scoped = StreamedSqlStore<LS::Scoped, ST::Scoped>;

    fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        StreamedSqlStore {
            leader_store: self.leader_store.scope(scope.clone().into()),
            local_name: self.local_name.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

#[derive(Clone)]
pub struct StreamedSqlStore2<LS: Store2, ST: Stream2<SqlStreamHandler>>
where
    SqlStreamHandler: StreamHandler<<ST::Scoped as Stream1<SqlStreamHandler>>::Scoped>,
{
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store2, ST: Stream2<SqlStreamHandler>> SqlStore2 for StreamedSqlStore2<LS, ST>
where
    SqlStreamHandler: StreamHandler<<ST::Scoped as Stream1<SqlStreamHandler>>::Scoped>,
    ST::Request: From<Bytes> + Into<Bytes>,
    ST::Response: From<Bytes> + Into<Bytes>,
    <ST::Scoped as Stream1<SqlStreamHandler>>::Request: From<Bytes> + Into<Bytes>,
    <ST::Scoped as Stream1<SqlStreamHandler>>::Response: From<Bytes> + Into<Bytes>,
{
    type Error = Error<ST::Error, LS::Error>;
    type Scoped = StreamedSqlStore1<LS::Scoped, ST::Scoped>;

    fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        StreamedSqlStore1 {
            leader_store: self.leader_store.scope(scope.clone().into()),
            local_name: self.local_name.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

#[derive(Clone)]
pub struct StreamedSqlStore3<LS: Store3, ST: Stream3<SqlStreamHandler>>
where
    SqlStreamHandler: StreamHandler<
        <<ST::Scoped as Stream2<SqlStreamHandler>>::Scoped as Stream1<SqlStreamHandler>>::Scoped,
    >,
{
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store3, ST: Stream3<SqlStreamHandler>> SqlStore3 for StreamedSqlStore3<LS, ST>
where
    SqlStreamHandler: StreamHandler<
        <<ST::Scoped as Stream2<SqlStreamHandler>>::Scoped as Stream1<SqlStreamHandler>>::Scoped,
    >,
    ST::Request: From<Bytes> + Into<Bytes>,
    ST::Response: From<Bytes> + Into<Bytes>,
    <ST::Scoped as Stream2<SqlStreamHandler>>::Request: From<Bytes> + Into<Bytes>,
    <ST::Scoped as Stream2<SqlStreamHandler>>::Response: From<Bytes> + Into<Bytes>,
{
    type Error = Error<ST::Error, LS::Error>;
    type Scoped = StreamedSqlStore2<LS::Scoped, ST::Scoped>;

    fn scope<Scope: Clone + Into<String> + Send>(&self, scope: Scope) -> Self::Scoped {
        StreamedSqlStore2 {
            leader_store: self.leader_store.scope(scope.clone().into()),
            local_name: self.local_name.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

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
