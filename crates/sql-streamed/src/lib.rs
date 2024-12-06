//! Implementation of SQL storage using a streams as an append-only log.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod connection;
mod error;
mod request;
mod response;
/// Stream handler for SQL queries and migrations.
pub mod stream_handler;

pub use connection::Connection;
pub use error::Error;
use request::Request;
use response::Response;
use stream_handler::SqlStreamHandler;
use stream_handler::SqlStreamHandlerOptions;

use async_trait::async_trait;
use bytes::Bytes;
use proven_libsql::Database;
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use proven_store::Store;
use proven_stream::{Stream, Stream1, Stream2, Stream3};
use tokio::sync::oneshot;

/// Options for configuring a `StreamedSqlStore`.
#[derive(Clone)]
pub struct StreamedSqlStoreOptions<S, LS> {
    /// The store that keeps track of the current leader.
    pub leader_store: LS,

    /// The name of the local machine.
    pub local_name: String,

    /// The stream to use as an append-only log.
    pub stream: S,
}

/// Sql store that uses a stream as an append-only log.
#[derive(Clone)]
pub struct StreamedSqlStore<S, LS>
where
    S: Stream<SqlStreamHandler>,
    LS: Store,
{
    leader_store: LS,
    local_name: String,
    stream: S,
}

impl<S, LS> StreamedSqlStore<S, LS>
where
    S: Stream<SqlStreamHandler>,
    LS: Store,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
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
impl<S, LS> SqlStore for StreamedSqlStore<S, LS>
where
    S: Stream<SqlStreamHandler>,
    LS: Store,
{
    type Error = Error<S::Error, LS::Error>;
    type Connection = Connection<S, LS>;

    async fn connect<Q: Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Self::Connection, Self::Error> {
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

        let handler = SqlStreamHandler::new(SqlStreamHandlerOptions {
            caught_up_tx,
            database: Database::connect(":memory:").await?,
        });

        tokio::spawn({
            let handler = handler.clone();
            let stream = self.stream.clone();
            async move {
                stream.handle(handler).await.unwrap();
            }
        });

        // Wait for the stream to catch up before applying migrations
        caught_up_rx
            .await
            .map_err(|_| Error::CaughtUpChannelClosed)?;

        let applied_migrations = handler.applied_migrations().await.clone();
        for migration in migrations {
            let migration_sql = migration.into();
            if !applied_migrations.contains(&migration_sql) {
                let request = Request::Migrate(migration_sql);

                let response = self.stream.request(request).await.map_err(Error::Stream)?;

                if let Response::Failed(error) = response {
                    return Err(Error::Libsql(error));
                }
            }
        }

        Ok(Connection::new(self.stream.clone()))
    }
}

macro_rules! impl_scoped_sql_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $stream_trait:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! StreamedSqlStore $index]]
            [!set! #trait_name = [!ident! SqlStore $index]]

            #[doc = $doc]
            #[derive(Clone)]
            pub struct #name<S, LS>
            where
                S: $stream_trait<SqlStreamHandler>,
                LS: Store,
            {
                leader_store: LS,
                local_name: String,
                stream: S,
            }

            impl<S, LS> #name<S, LS>
            where
                Self: Clone + Send + Sync + 'static,
                S: $stream_trait<SqlStreamHandler>,
                LS: Store,
            {
                /// Creates a new `#name` with the specified options.
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
            impl<S, LS> #trait_name for #name<S, LS>
            where
                Self: Clone + Send + Sync + 'static,
                S: $stream_trait<SqlStreamHandler>,
                LS: Store,
            {
                type Error = Error<S::Error, LS::Error>;
                type Scoped = $parent<S::Scoped, LS>;

                fn [!ident! scope_ $index]<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
                    $parent {
                        leader_store: self.leader_store.clone(),
                        local_name: self.local_name.clone(),
                        stream: self.stream.[!ident! scope_ $index](scope.into()),
                    }
                }
            }
        }
    };
}

impl_scoped_sql_store!(
    1,
    StreamedSqlStore,
    SqlStore,
    Stream1,
    "A single-scoped SQL store that uses a stream as an append-only log."
);
impl_scoped_sql_store!(
    2,
    StreamedSqlStore1,
    SqlStore1,
    Stream2,
    "A double-scoped SQL store that uses a stream as an append-only log."
);
impl_scoped_sql_store!(
    3,
    StreamedSqlStore2,
    SqlStore2,
    Stream3,
    "A triple-scoped SQL store that uses a stream as an append-only log."
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

    #[tokio::test]
    async fn test_invalid_sql_migration() {
        let result = timeout(Duration::from_secs(5), async {
            let leader_store = MemoryStore::new();
            let stream = MemoryStream::new();

            let sql_store = StreamedSqlStore::new(StreamedSqlStoreOptions {
                leader_store,
                local_name: "my-machine".to_string(),
                stream,
            });

            let connection_result = sql_store.connect(vec!["INVALID SQL STATEMENT"]).await;

            assert!(
                connection_result.is_err(),
                "Expected an error due to invalid SQL"
            );
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }
}
