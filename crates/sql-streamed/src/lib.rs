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
use proven_libsql::Database;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream, Stream1, Stream2, Stream3};
use proven_sql::{SqlStore, SqlStore1, SqlStore2, SqlStore3};
use proven_store::{Store, Store1, Store2, Store3};
use service_handler::SqlServiceHandler;
use tempfile::NamedTempFile;
use tokio::sync::{oneshot, Mutex};

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
    type Error = Error<S, SS>;

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
            // TODO: Actually use snapshots
            let _existing_snapshots = self.snapshot_store.keys().await.unwrap();

            let db_path = NamedTempFile::new()
                .map_err(|e| Error::TempFile(e))?
                .into_temp_path()
                .to_path_buf();

            let handler = SqlServiceHandler::new(
                applied_migrations.clone(),
                caught_up_tx,
                Database::connect(db_path).await?,
                self.snapshot_store.clone(),
                stream.clone(),
            );

            let _service = stream
                .start_service("SQL_SERVICE", self.service_options.clone(), handler.clone())
                .await
                .map_err(Error::Service)?;

            // Wait for the stream to catch up before applying migrations
            caught_up_rx
                .await
                .map_err(|_| Error::CaughtUpChannelClosed)?;

            let applied_migrations = applied_migrations.lock().await.clone();
            for migration in migrations {
                let migration_sql = migration.into();
                if !applied_migrations.contains(&migration_sql) {
                    let request = Request::Migrate(migration_sql);

                    if let ClientResponseType::Response(Response::Failed(error)) =
                        client.request(request).await.map_err(Error::Client)?
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
    type Scoped = StreamedSqlStore<S::Scoped, SS::Scoped>;

    fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
        StreamedSqlStore {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            snapshot_store: self.snapshot_store.scope(scope.clone().into()),
            stream: self.stream.scope(scope.into()),
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
    type Scoped = StreamedSqlStore1<S::Scoped, SS::Scoped>;

    fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
        StreamedSqlStore1 {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            snapshot_store: self.snapshot_store.scope(scope.clone().into()),
            stream: self.stream.scope(scope.into()),
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
    type Scoped = StreamedSqlStore2<S::Scoped, SS::Scoped>;

    fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> Self::Scoped {
        StreamedSqlStore2 {
            client_options: self.client_options.clone(),
            service_options: self.service_options.clone(),
            snapshot_store: self.snapshot_store.scope(scope.clone().into()),
            stream: self.stream.scope(scope.into()),
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
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_store() {
        let result = timeout(Duration::from_secs(5), async {
            let stream = MemoryStream::new("test_sql_store", MemoryStreamOptions);

            let snapshot_store = MemoryStore::new();

            let sql_store = StreamedSqlStore::new(
                stream,
                MemoryServiceOptions,
                MemoryClientOptions,
                snapshot_store,
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
                    SqlParam::Integer(1),
                    SqlParam::Text("alice@example.com".to_string())
                ]
            );
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }

    #[tokio::test]
    async fn test_invalid_sql_migration() {
        let result = timeout(Duration::from_secs(5), async {
            let stream = MemoryStream::new("test_invalid_sql_migration", MemoryStreamOptions);

            let snapshot_store = MemoryStore::new();

            let sql_store = StreamedSqlStore::new(
                stream,
                MemoryServiceOptions,
                MemoryClientOptions,
                snapshot_store,
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
}
