//! Implementation of SQL storage using a streams as an append-only log.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::type_complexity)]

mod connection;
mod error;
mod request;
mod response;
mod service_handler;

pub use connection::Connection;
pub use error::Error;
pub use request::Request;
pub use response::Response;

use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream, Stream1, Stream2, Stream3};
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use proven_store::{Store, Store1, Store2, Store3};
use service_handler::SqlServiceHandler;
use tokio::sync::{Mutex, oneshot};

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// A SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore<S, SS>
where
    S: Stream<Request, DeserializeError, SerializeError>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    client_options:
        <<S::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
            SqlServiceHandler<S::Initialized, SS>,
        > as Client<
            SqlServiceHandler<S::Initialized, SS>,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
    service_options:
        <<S::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
            SqlServiceHandler<S::Initialized, SS>,
        > as Service<
            SqlServiceHandler<S::Initialized, SS>,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
    snapshot_store: SS,
    stream: S,
}

impl<S, SS> StreamedSqlStore<S, SS>
where
    S: Stream<Request, DeserializeError, SerializeError>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub const fn new(
        stream: S,
        service_options: <<S::Initialized as InitializedStream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Service<SqlServiceHandler<S::Initialized, SS>> as Service<
            SqlServiceHandler<S::Initialized, SS>,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
        client_options: <<S::Initialized as InitializedStream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Client<SqlServiceHandler<S::Initialized, SS>> as Client<
            SqlServiceHandler<S::Initialized, SS>,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
        snapshot_store: SS,
    ) -> Self {
        Self {
            client_options,
            service_options,
            snapshot_store,
            stream,
        }
    }
}

#[async_trait]
impl<S, SS> SqlStore for StreamedSqlStore<S, SS>
where
    S: Stream<Request, DeserializeError, SerializeError>,
    SS: Store<Bytes, Infallible, Infallible>,
{
    type Error = Error;

    type Connection = Connection<S, SS>;

    async fn connect<Q: Clone + Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Self::Connection, Self::Error> {
        let (caught_up_tx, caught_up_rx) = oneshot::channel();

        let applied_migrations = Arc::new(Mutex::new(Vec::new()));

        let stream = self.stream.init().await.unwrap();

        let client = stream
            .client::<_, SqlServiceHandler<S::Initialized, SS>>(
                "SQL_SERVICE",
                self.client_options.clone(),
            )
            .await
            .unwrap();

        // TODO: Use distributed locks to decide if this machine should run the service
        // Just assume single-node operation for now
        let run_service = true;

        if run_service {
            let handler = SqlServiceHandler::new(
                applied_migrations.clone(),
                caught_up_tx,
                self.snapshot_store.clone(),
                stream.clone(),
            );

            let _service = stream
                .start_service("SQL_SERVICE", self.service_options.clone(), handler.clone())
                .await
                .map_err(|e| Error::Service(e.to_string()))?;

            // Wait for the stream to catch up before applying migrations
            caught_up_rx
                .await
                .map_err(|_| Error::CaughtUpChannelClosed)?;

            let applied_migrations = applied_migrations.lock().await.clone();
            for migration in migrations {
                let migration_sql = migration.into();
                if !applied_migrations.contains(&migration_sql) {
                    let request = Request::Migrate(migration_sql);

                    if let ClientResponseType::Response(Response::Failed(error)) = client
                        .request(request)
                        .await
                        .map_err(|e| Error::Client(e.to_string()))?
                    {
                        return Err(Error::Libsql(error));
                    }
                }
            }
        }

        Ok(Connection::new(client))
    }
}

// TODO: Maybe go back to macros in future if I can make it work with trait ambiguity.

/// A single-scoped SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore1<S, SS>
where
    S: Stream1<Request, DeserializeError, SerializeError>,
    SS: Store1<Bytes, Infallible, Infallible>,
{
    client_options: <<<S::Scoped as Stream<Request,
    DeserializeError,
    SerializeError>>::Initialized as InitializedStream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Client<SqlServiceHandler<<S::Scoped as Stream<Request,
    DeserializeError,
    SerializeError>>::Initialized, SS::Scoped>> as Client<

        SqlServiceHandler<<S::Scoped as Stream<Request,
        DeserializeError,
        SerializeError>>::Initialized, SS::Scoped>,
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
    >>::Service<SqlServiceHandler<<S::Scoped as Stream<Request,
    DeserializeError,
    SerializeError>>::Initialized, SS::Scoped>> as Service<
    SqlServiceHandler<<S::Scoped as Stream<Request,
    DeserializeError,
    SerializeError>>::Initialized, SS::Scoped>,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,
    snapshot_store: SS,
    stream: S,
}

impl<S, SS> StreamedSqlStore1<S, SS>
where
    S: Stream1<Request, DeserializeError, SerializeError>,
    SS: Store1<Bytes, Infallible, Infallible>,
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
        >>::Service<SqlServiceHandler<<S::Scoped as Stream<Request,
        DeserializeError,
        SerializeError>>::Initialized, SS::Scoped>> as Service<
        SqlServiceHandler<<S::Scoped as Stream<Request,
        DeserializeError,
        SerializeError>>::Initialized, SS::Scoped>,
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
        >>::Client<SqlServiceHandler<<S::Scoped as Stream<Request,
        DeserializeError,
        SerializeError>>::Initialized, SS::Scoped>> as Client<
        SqlServiceHandler<<S::Scoped as Stream<Request,
        DeserializeError,
        SerializeError>>::Initialized, SS::Scoped>,
            Request,
            DeserializeError,
            SerializeError,
        >>::Options,
        snapshot_store: SS,
    ) -> Self {
        Self {
            client_options,
            service_options,
            snapshot_store,
            stream,
        }
    }
}

#[async_trait]
impl<S, SS> SqlStore1 for StreamedSqlStore1<S, SS>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream1<Request, DeserializeError, SerializeError>,
    SS: Store1<Bytes, Infallible, Infallible>,
{
    type Error = Error;

    type Connection = Connection<S::Scoped, SS::Scoped>;

    type Scoped = StreamedSqlStore<S::Scoped, SS::Scoped>;

    fn scope<K>(&self, scope: K) -> Self::Scoped
    where
        K: AsRef<str> + Copy + Send,
    {
        StreamedSqlStore {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            snapshot_store: self.snapshot_store.scope(scope),
            stream: self.stream.scope(scope),
        }
    }
}

/// A double-scoped SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore2<S, SS>
where
    S: Stream2<Request, DeserializeError, SerializeError>,
    SS: Store2<Bytes, Infallible, Infallible>,
{
    client_options:
        <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
            SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
        > as Client<SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
    service_options:
        <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
        SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
        > as Service<SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
    snapshot_store: SS,
    stream: S,
}

impl<S, SS> StreamedSqlStore2<S, SS>
where
    S: Stream2<Request, DeserializeError, SerializeError>,
    SS: Store2<Bytes, Infallible, Infallible>,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub const fn new(
        stream: S,
        service_options: <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
    SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
    > as Service<SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
        client_options: <<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
    SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
    > as Client<SqlServiceHandler<<<S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
        snapshot_store: SS,
    ) -> Self {
        Self {
            client_options,
            service_options,
            snapshot_store,
            stream,
        }
    }
}

#[async_trait]
impl<S, SS> SqlStore2 for StreamedSqlStore2<S, SS>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream2<Request, DeserializeError, SerializeError>,
    SS: Store2<Bytes, Infallible, Infallible>,
{
    type Error = Error;

    type Connection = Connection<
        <S::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped,
        <SS::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped,
    >;

    type Scoped = StreamedSqlStore1<S::Scoped, SS::Scoped>;

    fn scope<K>(&self, scope: K) -> Self::Scoped
    where
        K: AsRef<str> + Copy + Send,
    {
        StreamedSqlStore1 {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            snapshot_store: self.snapshot_store.scope(scope),
            stream: self.stream.scope(scope),
        }
    }
}

/// A triple-scoped SQL store that uses a stream as an append-only log.
#[derive(Clone, Debug)]
pub struct StreamedSqlStore3<S, SS>
where
    S: Stream3<Request, DeserializeError, SerializeError>,
    SS: Store3<Bytes, Infallible, Infallible>,
{
    client_options:
        <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
            SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
        > as Client<SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
    service_options:
        <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
            Request,
            DeserializeError,
            SerializeError,
        >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
        SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
        > as Service<SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
    snapshot_store: SS,
    stream: S,
}

impl<S, SS> StreamedSqlStore3<S, SS>
where
    S: Stream3<Request, DeserializeError, SerializeError>,
    SS: Store3<Bytes, Infallible, Infallible>,
{
    /// Creates a new `StreamedSqlStore` with the specified options.
    pub const fn new(
        stream: S,
        service_options: <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Service<
    SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
    > as Service<SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
        client_options: <<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
        Request,
        DeserializeError,
        SerializeError,
    >>::Initialized as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
    SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>,
    > as Client<SqlServiceHandler<<<<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<Request, DeserializeError, SerializeError>>::Scoped as Stream<
    Request,
    DeserializeError,
    SerializeError,
>>::Initialized, <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<Bytes, Infallible, Infallible>>::Scoped>, Request, DeserializeError, SerializeError>>::Options,
        snapshot_store: SS,
    ) -> Self {
        Self {
            client_options,
            service_options,
            snapshot_store,
            stream,
        }
    }
}

#[async_trait]
impl<S, SS> SqlStore3 for StreamedSqlStore3<S, SS>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream3<Request, DeserializeError, SerializeError>,
    SS: Store3<Bytes, Infallible, Infallible>,
{
    type Error = Error;

    type Connection = Connection<
        <<S::Scoped as Stream2<Request, DeserializeError, SerializeError>>::Scoped as Stream1<
            Request,
            DeserializeError,
            SerializeError,
        >>::Scoped,
        <<SS::Scoped as Store2<Bytes, Infallible, Infallible>>::Scoped as Store1<
            Bytes,
            Infallible,
            Infallible,
        >>::Scoped,
    >;

    type Scoped = StreamedSqlStore2<S::Scoped, SS::Scoped>;

    fn scope<K>(&self, scope: K) -> Self::Scoped
    where
        K: AsRef<str> + Copy + Send,
    {
        StreamedSqlStore2 {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            snapshot_store: self.snapshot_store.scope(scope),
            stream: self.stream.scope(scope),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;
    use proven_messaging_memory::{
        client::MemoryClientOptions,
        service::MemoryServiceOptions,
        stream::{MemoryStream, MemoryStreamOptions},
    };
    use proven_sql::{SqlConnection, SqlParam};
    use proven_store_memory::MemoryStore;
    use tokio::time::{Duration, sleep, timeout};

    #[tokio::test]
    async fn test_sql_store() {
        let result = timeout(Duration::from_secs(5), async {
            let stream = MemoryStream::new("test_sql_store", MemoryStreamOptions);

            let sql_store = StreamedSqlStore::new(
                stream,
                MemoryServiceOptions,
                MemoryClientOptions,
                MemoryStore::new(),
            );

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

            let mut rows = connection
                .query("SELECT id, email FROM users".to_string(), vec![])
                .await
                .unwrap();

            let mut results = Vec::new();
            while let Some(row) = rows.next().await {
                results.push(row);
            }

            assert_eq!(results.len(), 1);
            assert_eq!(
                results[0],
                vec![
                    SqlParam::IntegerWithName("id".to_string(), 1),
                    SqlParam::TextWithName("email".to_string(), "alice@example.com".to_string()),
                ]
            );
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_no_results() {
        let result = timeout(Duration::from_secs(5), async {
            let stream = MemoryStream::new("test_no_results", MemoryStreamOptions);

            let sql_store = StreamedSqlStore::new(
                stream,
                MemoryServiceOptions,
                MemoryClientOptions,
                MemoryStore::new(),
            );

            let connection = sql_store
                .connect(vec![
                    "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
                ])
                .await
                .unwrap();

            let mut rows = connection
                .query("SELECT id, email FROM users".to_string(), vec![])
                .await
                .unwrap();

            let mut results = Vec::new();
            while let Some(row) = rows.next().await {
                results.push(row);
            }

            assert_eq!(results.len(), 0);
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_invalid_sql_migration() {
        let result = timeout(Duration::from_secs(5), async {
            let stream = MemoryStream::new("test_invalid_sql_migration", MemoryStreamOptions);

            let sql_store = StreamedSqlStore::new(
                stream,
                MemoryServiceOptions,
                MemoryClientOptions,
                MemoryStore::new(),
            );

            let connection_result = sql_store.connect(vec!["INVALID SQL STATEMENT"]).await;

            assert!(
                connection_result.is_err(),
                "Expected an error due to invalid SQL"
            );
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    // A more complete snapshot test (verifying rollup) is in the nats.rs test file.
    #[tokio::test]
    async fn test_snapshotting() {
        let result = timeout(Duration::from_secs(10), async {
            let stream = MemoryStream::new("test_snapshotting", MemoryStreamOptions);

            let snapshot_store = MemoryStore::new();

            let sql_store = StreamedSqlStore::new(
                stream,
                MemoryServiceOptions,
                MemoryClientOptions,
                snapshot_store.clone(),
            );

            let connection = sql_store
                .connect(vec![
                    "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
                ])
                .await
                .unwrap();

            // Execute 1000 inserts
            for i in 0..1000 {
                connection
                    .execute(
                        "INSERT INTO users (id, email) VALUES (?1, ?2)".to_string(),
                        vec![
                            SqlParam::Integer(i),
                            SqlParam::Text("alice@example.com".to_string()),
                        ],
                    )
                    .await
                    .unwrap();
            }

            // Wait for 3 seconds to allow snapshotting
            sleep(Duration::from_secs(3)).await;

            // Check that the snapshot store has one key
            let keys = snapshot_store.keys().await.unwrap();
            assert_eq!(keys.len(), 1);
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }
}
