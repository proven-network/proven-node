mod connection;
mod database;
mod error;
mod request;
mod response;

use std::marker::PhantomData;
use std::sync::Arc;

pub use connection::Connection;
pub use database::Database;
pub use error::{Error, HandlerError, HandlerResult, Result};
pub use request::Request;
pub use response::Response;

use async_trait::async_trait;
use bytes::Bytes;
use proven_sql::{Rows, SqlParam, SqlStore, SqlStore1, SqlStore2, SqlStore3};
use proven_store::{Store, Store1, Store2, Store3};
use proven_stream::{Stream, Stream1, Stream2, Stream3};

#[derive(Clone)]
pub struct NatsSqlStoreOptions<LS: Store, ST: Stream<HandlerError>> {
    pub leader_store: LS,
    pub local_name: String,
    pub stream: ST,
}

#[derive(Clone)]
pub struct NatsSqlStore<LS: Store, ST: Stream<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

impl<LS: Store, ST: Stream<HandlerError> + 'static> NatsSqlStore<LS, ST> {
    async fn handle_request(database: Database, request: Request) -> HandlerResult<Response> {
        match request {
            Request::Execute(sql, params) => {
                let affected_rows = database.execute(&sql, params).await?;
                Ok(Response::Execute(affected_rows))
            }
            Request::Query(sql, params) => Ok(Response::Query(
                Self::handle_query(database, sql, params).await?,
            )),
        }
    }

    async fn handle_query(
        database: Database,
        sql: String,
        params: Vec<SqlParam>,
    ) -> HandlerResult<Rows> {
        let mut libsql_rows = database.query(&sql, params).await?;
        let column_count = libsql_rows.column_count();

        let column_names = Self::get_column_names(&libsql_rows, column_count);
        let column_types = Self::get_column_types(&libsql_rows, column_count);
        let rows_vec = Self::get_row_values(&mut libsql_rows, column_count).await?;

        Ok(Rows {
            column_count: column_count as u16,
            column_names,
            column_types,
            rows: rows_vec,
        })
    }

    fn get_column_names(rows: &libsql::Rows, column_count: i32) -> Vec<String> {
        (0..column_count)
            .map(|i| rows.column_name(i).unwrap().to_string())
            .collect()
    }

    fn get_column_types(rows: &libsql::Rows, column_count: i32) -> Vec<String> {
        (0..column_count)
            .map(|i| match rows.column_type(i).unwrap() {
                libsql::ValueType::Text => "TEXT".to_string(),
                libsql::ValueType::Integer => "INTEGER".to_string(),
                libsql::ValueType::Real => "REAL".to_string(),
                libsql::ValueType::Blob => "BLOB".to_string(),
                libsql::ValueType::Null => "NULL".to_string(),
            })
            .collect()
    }

    async fn get_row_values(
        rows: &mut libsql::Rows,
        column_count: i32,
    ) -> HandlerResult<Vec<Vec<SqlParam>>> {
        let mut rows_vec = Vec::new();
        while let Some(row) = rows.next().await? {
            let row_vec = (0..column_count)
                .map(|i| match row.get_value(i).unwrap() {
                    libsql::Value::Null => SqlParam::Null,
                    libsql::Value::Integer(i) => SqlParam::Integer(i),
                    libsql::Value::Real(r) => SqlParam::Real(r),
                    libsql::Value::Text(s) => SqlParam::Text(s),
                    libsql::Value::Blob(b) => SqlParam::Blob(Bytes::from(b)),
                })
                .collect();
            rows_vec.push(row_vec);
        }
        Ok(rows_vec)
    }
}

impl<LS: Store, ST: Stream<HandlerError>> NatsSqlStore<LS, ST> {
    pub fn new(
        NatsSqlStoreOptions {
            leader_store,
            local_name,
            stream,
        }: NatsSqlStoreOptions<LS, ST>,
    ) -> Self {
        Self {
            leader_store,
            local_name,
            stream,
        }
    }
}

#[async_trait]
impl<LS: Store, ST: Stream<HandlerError> + 'static> SqlStore for NatsSqlStore<LS, ST> {
    type Error = Error<ST::Error, LS::Error>;
    type Connection = Connection<ST, LS>;

    async fn connect<N: Clone + Into<String> + Send + 'static>(
        &self,
        db_name: N,
    ) -> Result<Connection<ST, LS>, ST::Error, LS::Error> {
        let current_leader = self
            .leader_store
            .get(db_name.clone())
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
                    db_name.clone(),
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
                    .handle(db_name.into(), move |bytes: Bytes| {
                        let database = database.clone();
                        Box::pin(async move {
                            let request: Request = bytes.try_into()?;
                            println!("Request: {:?}", request);

                            let response =
                                NatsSqlStore::<LS, ST>::handle_request(database, request).await?;

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
pub struct NatsSqlStore1<LS: Store1, ST: Stream1<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store1, ST: Stream1<HandlerError>> SqlStore1 for NatsSqlStore1<LS, ST> {
    type Error = Error<ST::Error, LS::Error>;
    type Scoped = NatsSqlStore<LS::Scoped, ST::Scoped>;

    fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        NatsSqlStore {
            leader_store: self.leader_store.scope(scope.clone().into()),
            local_name: self.local_name.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

#[derive(Clone)]
pub struct NatsSqlStore2<LS: Store2, ST: Stream2<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store2, ST: Stream2<HandlerError>> SqlStore2 for NatsSqlStore2<LS, ST> {
    type Error = Error<ST::Error, LS::Error>;
    type Scoped = NatsSqlStore1<LS::Scoped, ST::Scoped>;

    fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        NatsSqlStore1 {
            leader_store: self.leader_store.scope(scope.clone().into()),
            local_name: self.local_name.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

#[derive(Clone)]
pub struct NatsSqlStore3<LS: Store3, ST: Stream3<HandlerError>> {
    leader_store: LS,
    local_name: String,
    stream: ST,
}

#[async_trait]
impl<LS: Store3, ST: Stream3<HandlerError>> SqlStore3 for NatsSqlStore3<LS, ST> {
    type Error = Error<ST::Error, LS::Error>;
    type Scoped = NatsSqlStore2<LS::Scoped, ST::Scoped>;

    fn scope<Scope: Clone + Into<String> + Send>(&self, scope: Scope) -> Self::Scoped {
        NatsSqlStore2 {
            leader_store: self.leader_store.scope(scope.clone().into()),
            local_name: self.local_name.clone(),
            stream: self.stream.scope(scope.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_sql::Connection as SqlConnection;
    use proven_store_memory::MemoryStore;
    use proven_stream_nats::ScopeMethod;
    use proven_stream_nats::{NatsStream, NatsStreamOptions};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_store() {
        let result = timeout(Duration::from_secs(5), async {
            let client = async_nats::connect("nats://localhost:4222").await.unwrap();

            let leader_store = MemoryStore::new();

            let stream = NatsStream::new(NatsStreamOptions {
                client: client.clone(),
                local_name: "my-machine".to_string(),
                scope_method: ScopeMethod::StreamPostfix,
                stream_name: "neww_sql".to_string(),
            });

            let sql_store = NatsSqlStore::new(NatsSqlStoreOptions {
                leader_store,
                local_name: "my-machine".to_string(),
                stream,
            });

            let connection = sql_store.connect("new_test".to_string()).await.unwrap();

            let response = connection
                .execute(
                    "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)".to_string(),
                    vec![],
                )
                .await
                .unwrap();

            assert_eq!(response, 0);

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
