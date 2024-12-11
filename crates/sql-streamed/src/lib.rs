//! Implementation of SQL storage using a streams as an append-only log.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod connection;
mod error;
mod request;
mod response;
mod stream_handler;

use std::sync::Arc;

pub use connection::Connection;
pub use error::Error;
pub use request::Request;
pub use response::Response;

use async_trait::async_trait;
use proven_libsql::Database;
use proven_messaging::client::Client;
use proven_messaging::stream::{InitializedStream, Stream, Stream1, Stream2, Stream3};
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use stream_handler::{SqlStreamHandler, SqlStreamHandlerOptions};
use tokio::sync::{oneshot, Mutex};

/// Options for configuring a `StreamedSqlStore`.
#[derive(Clone, Debug)]
pub struct StreamedSqlStoreOptions<S> {
    /// The stream to use as an append-only log.
    pub stream: S,
}

/// Sql store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore<S>
where
    S: Stream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    S::Initialized: InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    stream: S,
}

impl<S> StreamedSqlStore<S>
where
    S: Stream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    S::Initialized: InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub fn new(StreamedSqlStoreOptions { stream }: StreamedSqlStoreOptions<S>) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl<S> SqlStore for StreamedSqlStore<S>
where
    S: Stream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    S::Initialized: InitializedStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    type Error = Error<
        <S::Initialized as InitializedStream<
            Request,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >>::Error<ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    >;
    type Connection = Connection<S>;

    async fn connect<Q: Clone + Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Self::Connection, Self::Error> {
        let (caught_up_tx, caught_up_rx) = oneshot::channel();

        let applied_migrations = Arc::new(Mutex::new(Vec::new()));

        let handler = SqlStreamHandler::new(SqlStreamHandlerOptions {
            applied_migrations: applied_migrations.clone(),
            database: Database::connect(":memory:").await?,
            caught_up_tx,
        });

        let stream = self.stream.init().await.unwrap();

        let _service = stream
            .start_service("SQL_SERVICE", handler.clone())
            .await
            .map_err(Error::Stream)?;

        let client = stream.client("SQL_SERVICE", handler).await.unwrap();

        // Wait for the stream to catch up before applying migrations
        caught_up_rx
            .await
            .map_err(|_| Error::CaughtUpChannelClosed)?;

        let applied_migrations = applied_migrations.lock().await.clone();
        for migration in migrations {
            let migration_sql = migration.into();
            if !applied_migrations.contains(&migration_sql) {
                let request = Request::Migrate(migration_sql);

                if let Response::Failed(error) =
                    client.request(request).await.map_err(|_| Error::Client)?
                {
                    return Err(Error::Libsql(error));
                }
            }
        }

        Ok(Connection::new(client))
    }
}

macro_rules! impl_scoped_sql_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $stream_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Clone, Debug)]
            pub struct [< StreamedSqlStore $index >]<S>
            where
                S: $stream_trait<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
            {
                stream: S,
            }

            impl<S> [< StreamedSqlStore $index >]<S>
            where
                Self: Clone + Send + Sync + 'static,
                S: $stream_trait<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
            {
                /// Creates a new `StreamedSqlStore` with the specified options.
                pub fn new(
                    StreamedSqlStoreOptions {
                        stream,
                    }: StreamedSqlStoreOptions<S>,
                ) -> Self {
                    Self {
                        stream,
                    }
                }
            }

            #[async_trait]
            impl<S> [< SqlStore $index >] for [< StreamedSqlStore $index >]<S>
            where
                Self: Clone + Send + Sync + 'static,
                S: $stream_trait<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
            {
                type Error = Error<S::Error<ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>>;

                type Scoped = $parent<S::Scoped>;

                fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
                    $parent {
                        stream: self.stream.scope(scope.into()),
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
    use proven_messaging_memory::stream::{MemoryStream, MemoryStreamOptions};
    use proven_sql::{SqlConnection, SqlParam};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_store() {
        let result = timeout(Duration::from_secs(5), async {
            let stream = MemoryStream::new("test_sql_store", MemoryStreamOptions);

            let sql_store = StreamedSqlStore::new(StreamedSqlStoreOptions { stream });

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
            let stream = MemoryStream::new("test_invalid_sql_migration", MemoryStreamOptions);

            let sql_store = StreamedSqlStore::new(StreamedSqlStoreOptions { stream });

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
