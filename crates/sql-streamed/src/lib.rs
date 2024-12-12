//! Implementation of SQL storage using a streams as an append-only log.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::type_complexity)]

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

use std::fmt::Debug;

use async_trait::async_trait;
use proven_libsql::Database;
use proven_messaging::client::Client;
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream, Stream1, Stream2, Stream3};
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use stream_handler::{SqlStreamHandler, SqlStreamHandlerOptions};
use tokio::sync::{oneshot, Mutex};

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// A SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore<S>
where
    S: Stream<Request, DeserializeError, SerializeError>,
{
    client_options:
        <<S::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
            SqlStreamHandler,
        > as Client<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    service_options:
        <<S::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
            SqlStreamHandler,
        > as Service<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    stream: S,
}

impl<S> StreamedSqlStore<S>
where
    S: Stream<Request, DeserializeError, SerializeError>,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub const fn new(
        stream: S,
        service_options: <<S::Initialized as InitializedStream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Service<SqlStreamHandler> as Service<
            SqlStreamHandler,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
        client_options: <<S::Initialized as InitializedStream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Client<SqlStreamHandler> as Client<
            SqlStreamHandler,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
    ) -> Self {
        Self {
            client_options,
            service_options,
            stream,
        }
    }
}

#[async_trait]
impl<S> SqlStore for StreamedSqlStore<S>
where
    S: Stream<Request, DeserializeError, SerializeError>,
{
    type Error = Error<S>;

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
            .start_service("SQL_SERVICE", self.service_options.clone(), handler.clone())
            .await
            .map_err(Error::Service)?;

        let client = stream
            .client("SQL_SERVICE", self.client_options.clone(), handler)
            .await
            .unwrap();

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
                    client.request(request).await.map_err(Error::Client)?
                {
                    return Err(Error::Libsql(error));
                }
            }
        }

        Ok(Connection::new(client))
    }
}

// TODO: Maybe go back to macros in future if I can make it work with trait ambiguity.

/// A single-scoped SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore1<S>
where
    S: Stream1<Request, DeserializeError, SerializeError>,
{
    client_options: <<<S::Scoped as Stream<Request,
    DeserializeError,
    SerializeError>>::Initialized as InitializedStream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Client<SqlStreamHandler> as Client<

        SqlStreamHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,
    service_options: <<<S::Scoped as Stream<Request,
    DeserializeError,
    SerializeError>>::Initialized as InitializedStream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Service<SqlStreamHandler> as Service<
        SqlStreamHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,
    stream: S,
}

impl<S> StreamedSqlStore1<S>
where
    S: Stream1<Request, DeserializeError, SerializeError>,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub const fn new(
        stream: S,
        service_options: <<<S::Scoped as Stream<Request,
        DeserializeError,
        SerializeError>>::Initialized as InitializedStream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Service<SqlStreamHandler> as Service<
            SqlStreamHandler,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
        client_options: <<<S::Scoped as Stream<Request,
        DeserializeError,
        SerializeError>>::Initialized as InitializedStream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Client<SqlStreamHandler> as Client<
            SqlStreamHandler,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
    ) -> Self {
        Self {
            client_options,
            service_options,
            stream,
        }
    }
}

#[async_trait]
impl<S> SqlStore1 for StreamedSqlStore1<S>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream1<Request, DeserializeError, SerializeError>,
{
    type Scoped = StreamedSqlStore<S::Scoped>;

    fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
        StreamedSqlStore {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

/// A double-scoped SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore2<S>
where
    S: Stream2<Request, DeserializeError, SerializeError>,
{
    client_options:
        <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
            SqlStreamHandler,
        > as Client<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    service_options:
        <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
            SqlStreamHandler,
        > as Service<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    stream: S,
}

impl<S> StreamedSqlStore2<S>
where
    S: Stream2<Request, DeserializeError, SerializeError>,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub const fn new(
        stream: S,
        service_options: <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
        SqlStreamHandler,
    > as Service<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
        client_options: <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
        SqlStreamHandler,
    > as Client<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    ) -> Self {
        Self {
            client_options,
            service_options,
            stream,
        }
    }
}

#[async_trait]
impl<S> SqlStore2 for StreamedSqlStore2<S>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream2<Request, DeserializeError, SerializeError>,
{
    type Scoped = StreamedSqlStore1<S::Scoped>;

    fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
        StreamedSqlStore1 {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

/// A triple-scoped SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore3<S>
where
    S: Stream3<Request, DeserializeError, SerializeError>,
{
    client_options:
        <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
            SqlStreamHandler,
        > as Client<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    service_options:
        <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
            SqlStreamHandler,
        > as Service<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    stream: S,
}

impl<S> StreamedSqlStore3<S>
where
    S: Stream3<Request, DeserializeError, SerializeError>,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub const fn new(
        stream: S,
        service_options: <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
        SqlStreamHandler,
    > as Service<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
        client_options: <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
        SqlStreamHandler,
    > as Client<SqlStreamHandler, Request, DeserializeError, SerializeError>>::Options,
    ) -> Self {
        Self {
            client_options,
            service_options,
            stream,
        }
    }
}

#[async_trait]
impl<S> SqlStore3 for StreamedSqlStore3<S>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream3<Request, DeserializeError, SerializeError>,
{
    type Scoped = StreamedSqlStore2<S::Scoped>;

    fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
        StreamedSqlStore2 {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_messaging_memory::{
        client::MemoryClientOptions,
        service::MemoryServiceOptions,
        stream::{MemoryStream, MemoryStreamOptions},
    };
    use proven_sql::{SqlConnection, SqlParam};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_store() {
        let result = timeout(Duration::from_secs(5), async {
            let stream = MemoryStream::new("test_sql_store", MemoryStreamOptions);

            let sql_store =
                StreamedSqlStore::new(stream, MemoryServiceOptions, MemoryClientOptions);

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

            let sql_store =
                StreamedSqlStore::new(stream, MemoryServiceOptions, MemoryClientOptions);

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
