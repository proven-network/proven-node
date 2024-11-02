mod database;
mod error;

pub use database::Database;
pub use error::{Error, Result};

use proven_store::{Store, Store1};
use proven_stream::{Stream, Stream1};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    Execute(String),
    Query(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Rows {
    column_count: u16,
    column_names: Vec<String>,
    column_types: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Execute(u64),
    Query(Rows),
}

pub struct Connection<S: Stream<Error>> {
    stream: S,
}

impl<S: Stream<Error>> Connection<S> {
    pub async fn execute(&self, sql: String) -> Result<u64> {
        let request = Request::Execute(sql);
        let bytes = serde_cbor::to_vec(&request).unwrap();

        let raw_response = self
            .stream
            .request("execute".to_string(), bytes)
            .await
            .unwrap();

        let response: Response = serde_cbor::from_slice(&raw_response)?;
        match response {
            Response::Execute(affected_rows) => Ok(affected_rows),
            _ => unreachable!(),
        }
    }

    pub async fn query(&self, sql: String) -> Result<Rows> {
        let request = Request::Query(sql);
        let bytes = serde_cbor::to_vec(&request).unwrap();

        let raw_response = self
            .stream
            .request("query".to_string(), bytes)
            .await
            .unwrap();

        let response: Response = serde_cbor::from_slice(&raw_response)?;
        match response {
            Response::Query(rows) => Ok(rows),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub struct SqlManagerOptions<LS: Store1, S: Stream1<Error>> {
    pub leader_store: LS,
    pub local_name: String,
    pub stream: S,
}

pub struct SqlManager<LS: Store1, S: Stream1<Error>> {
    leader_store: LS,
    local_name: String,
    stream: S,
}

impl<LS: Store1, S: Stream1<Error>> SqlManager<LS, S> {
    pub fn new(
        SqlManagerOptions {
            leader_store,
            local_name,
            stream,
        }: SqlManagerOptions<LS, S>,
    ) -> Self {
        Self {
            leader_store,
            local_name,
            stream,
        }
    }

    pub async fn connect(&self, application_id: String, db_name: String) -> Connection<S::Scoped> {
        let scoped_leader_store = self.leader_store.scope(application_id.clone());
        let scoped_stream = self.stream.scope(application_id.clone());
        let current_leader = scoped_leader_store.get(db_name.clone()).await.unwrap();

        // If no current_leader, then we will try become the leader
        // If we are the leader, then we extend the lease
        if current_leader.is_none()
            || self.local_name == String::from_utf8(current_leader.clone().unwrap()).unwrap()
        {
            scoped_leader_store
                .put(db_name.clone(), self.local_name.clone().into_bytes())
                .await
                .unwrap();
        }

        let database = Database::connect().await;

        tokio::spawn({
            let database = database.clone();
            let scoped_stream = scoped_stream.clone();
            let db_name = db_name.clone();

            async move {
                scoped_stream
                    .handle(db_name, move |bytes: Vec<u8>| {
                        let database = database.clone();
                        Box::pin(async move {
                            let request: Request = serde_cbor::from_slice(&bytes)?;
                            println!("Request: {:?}", request);

                            match request {
                                Request::Execute(sql) => {
                                    let affected_rows = database.execute(&sql, ()).await?;

                                    Ok(serde_cbor::to_vec(&Response::Execute(affected_rows))?)
                                }
                                Request::Query(sql) => {
                                    let mut libsql_rows = database.query(&sql, ()).await?;

                                    let column_count = libsql_rows.column_count();

                                    // Iterate through the columns and collect the names and types
                                    let column_names = (0..column_count)
                                        .map(|i| libsql_rows.column_name(i).unwrap().to_string())
                                        .collect();

                                    let column_types = (0..column_count)
                                        .map(|i| match libsql_rows.column_type(i).unwrap() {
                                            libsql::ValueType::Text => "TEXT".to_string(),
                                            libsql::ValueType::Integer => "INTEGER".to_string(),
                                            libsql::ValueType::Real => "REAL".to_string(),
                                            libsql::ValueType::Blob => "BLOB".to_string(),
                                            libsql::ValueType::Null => "NULL".to_string(),
                                        })
                                        .collect();

                                    // Iterate through the rows and collect the values
                                    let mut rows_vec = Vec::new();
                                    while let Some(row) = libsql_rows.next().await? {
                                        let row_vec = (0..column_count)
                                            .map(|i| {
                                                let row_string: String = row.get(i).unwrap();
                                                row_string
                                            })
                                            .collect();
                                        rows_vec.push(row_vec);
                                    }

                                    let final_rows = Rows {
                                        column_count: column_count as u16,
                                        column_names,
                                        column_types,
                                        rows: rows_vec,
                                    };

                                    Ok(serde_cbor::to_vec(&Response::Query(final_rows))?)
                                }
                            }
                        })
                    })
                    .await
                    .unwrap();
            }
        });

        Connection {
            stream: scoped_stream,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_store_memory::MemoryStore;
    use proven_stream_nats::ScopeMethod;
    use proven_stream_nats::{NatsStream, NatsStreamOptions};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_manager() {
        let result = timeout(Duration::from_secs(5), async {
            let client = async_nats::connect("nats://localhost:4222").await.unwrap();

            let leader_store = MemoryStore::new();

            let stream = NatsStream::new(NatsStreamOptions {
                client: client.clone(),
                local_name: "my-machine".to_string(),
                scope_method: ScopeMethod::StreamPostfix,
                stream_name: "sql".to_string(),
            });

            let sql_manager = SqlManager::new(SqlManagerOptions {
                leader_store,
                local_name: "my-machine".to_string(),
                stream,
            });

            let connection = sql_manager
                .connect("test".to_string(), "test".to_string())
                .await;

            let response = connection
                .execute("CREATE TABLE IF NOT EXISTS users (email TEXT)".to_string())
                .await
                .unwrap();

            assert_eq!(response, 0);

            let response = connection
                .execute("INSERT INTO users (email) VALUES ('test@example.com')".to_string())
                .await
                .unwrap();

            assert_eq!(response, 1);

            let response = connection
                .query("SELECT * FROM users".to_string())
                .await
                .unwrap();

            assert_eq!(response.column_count, 1);
            assert_eq!(response.column_names, vec!["email".to_string()]);
            assert_eq!(response.column_types, vec!["TEXT".to_string()]);
            assert_eq!(response.rows, vec![vec!["test@example.com".to_string()]]);
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }
}
