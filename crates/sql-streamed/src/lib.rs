mod connection;
mod error;
mod request;
mod response;

use std::marker::PhantomData;
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
use proven_stream::{Stream, Stream1, Stream2, Stream3};

#[derive(Clone)]
pub struct StreamedSqlStoreOptions<LS: Store, ST: Stream<HandlerError>> {
    pub leader_store: LS,
    pub local_name: String,
    pub stream: ST,
}

#[derive(Clone)]
pub struct StreamedSqlStore<LS: Store, ST: Stream<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

impl<LS: Store, ST: Stream<HandlerError> + 'static> StreamedSqlStore<LS, ST> {
    async fn handle_request(mut database: Database, request: Request) -> HandlerResult<Response> {
        match request {
            Request::Execute(sql, params) => {
                let affected_rows = database.execute(&sql, params).await?;
                Ok(Response::Execute(affected_rows))
            }
            Request::ExecuteBatch(sql, params) => {
                let affected_rows = database.execute_batch(&sql, params).await?;
                Ok(Response::ExecuteBatch(affected_rows))
            }
            Request::Migrate(sql) => {
                let needed_to_run = database.migrate(&sql).await?;
                Ok(Response::Migrate(needed_to_run))
            }
            Request::Query(sql, params) => {
                let rows = database.query(&sql, params).await?;
                Ok(Response::Query(rows))
            }
        }
    }
}

impl<LS: Store, ST: Stream<HandlerError>> StreamedSqlStore<LS, ST> {
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
impl<LS: Store, ST: Stream<HandlerError> + 'static> SqlStore for StreamedSqlStore<LS, ST> {
    type Error = Error<ST::Error, LS::Error>;
    type Connection = Connection<ST, LS>;

    async fn connect(&self) -> Result<Connection<ST, LS>, ST::Error, LS::Error> {
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

        let database = Database::connect().await;

        tokio::spawn({
            let stream = self.stream.clone();

            async move {
                stream
                    .handle(move |bytes: Bytes| {
                        let database = database.clone();
                        Box::pin(async move {
                            let request: Request = bytes.try_into()?;
                            println!("Request: {:?}", request);

                            let response =
                                StreamedSqlStore::<LS, ST>::handle_request(database, request)
                                    .await?;

                            response
                                .try_into()
                                .map_err(|e| HandlerError::CborSerialize(Arc::new(e)))
                        })
                    })
                    .await
                    .unwrap();
            }
        });

        Ok(Connection {
            stream: self.stream.clone(),
            _marker: PhantomData,
        })
    }
}

#[derive(Clone)]
pub struct StreamedSqlStore1<LS: Store1, ST: Stream1<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store1, ST: Stream1<HandlerError>> SqlStore1 for StreamedSqlStore1<LS, ST> {
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
pub struct StreamedSqlStore2<LS: Store2, ST: Stream2<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store2, ST: Stream2<HandlerError>> SqlStore2 for StreamedSqlStore2<LS, ST> {
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
pub struct StreamedSqlStore3<LS: Store3, ST: Stream3<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store3, ST: Stream3<HandlerError>> SqlStore3 for StreamedSqlStore3<LS, ST> {
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
    use proven_sql::{Connection as SqlConnection, SqlParam};
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

            let connection = sql_store.connect().await.unwrap();

            let response = connection
                .migrate("CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)".to_string())
                .await
                .unwrap();

            assert!(response);

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
