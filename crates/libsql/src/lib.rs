//! Wrapper around [libsql](https://github.com/tursodatabase/libsql) which
//! provides additional functionality like migration
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod sql_type;

use std::fmt::Debug;

pub use error::Error;
use sql_type::SqlType;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use libsql::{Builder, Connection, Value};
use proven_sql::SqlParam;
use sha2::{Digest, Sha256};

static RESERVED_TABLE_PREFIX: &str = "__proven_";
static CREATE_MIGRATIONS_TABLE_SQL: &str = include_str!("../sql/create_migrations_table.sql");
static INSERT_MIGRATION_SQL: &str = include_str!("../sql/insert_migration.sql");

/// A libsql database wrapper.
#[derive(Clone)]
pub struct Database {
    connection: Connection,
}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Database {
    /// Connects to the database at the given path.
    ///
    /// # Errors
    ///
    /// This function will return an error if the connection to the database fails.
    pub async fn connect(path: impl AsRef<std::path::Path> + Send) -> Result<Self, Error> {
        let connection = Builder::new_local(path).build().await?.connect()?;

        connection
            .execute(CREATE_MIGRATIONS_TABLE_SQL, Self::convert_params(vec![]))
            .await?;

        Ok(Self { connection })
    }

    /// Executes a mutation SQL query with the given parameters.
    ///
    /// # Errors
    ///
    /// This function will return an error if the query contains a reserved table prefix,
    /// if the SQL type is incorrect, or if there is an issue executing the query.
    pub async fn execute(&self, query: &str, params: Vec<SqlParam>) -> Result<u64, Error> {
        if query.contains(RESERVED_TABLE_PREFIX) {
            return Err(Error::UsedReservedTablePrefix);
        }

        match Self::classify_sql(query) {
            SqlType::Migration => Err(Error::IncorrectSqlType(
                SqlType::Mutation,
                SqlType::Migration,
            )),
            SqlType::Mutation => {
                let libsql_params = Self::convert_params(params);
                self.connection
                    .execute(query, libsql_params)
                    .await
                    .map_err(Error::from)
            }
            SqlType::Query => Err(Error::IncorrectSqlType(SqlType::Mutation, SqlType::Query)),
        }
    }

    /// Executes a batch of mutation SQL queries with the given parameters.
    ///
    /// # Errors
    ///
    /// This function will return an error if the query contains a reserved table prefix,
    /// if the SQL type is incorrect, or if there is an issue executing the query.
    pub async fn execute_batch(
        &self,
        query: &str,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, Error> {
        if query.contains(RESERVED_TABLE_PREFIX) {
            return Err(Error::UsedReservedTablePrefix);
        }

        match Self::classify_sql(query) {
            SqlType::Migration => Err(Error::IncorrectSqlType(
                SqlType::Mutation,
                SqlType::Migration,
            )),
            SqlType::Mutation => {
                let libsql_params = params
                    .into_iter()
                    .map(Self::convert_params)
                    .collect::<Vec<_>>();

                let mut total: u64 = 0;

                for params in libsql_params {
                    total += self.connection.execute(query, params).await?;
                }

                Ok(total)
            }
            SqlType::Query => Err(Error::IncorrectSqlType(SqlType::Mutation, SqlType::Query)),
        }
    }

    /// Executes a migration SQL query.
    ///
    /// # Errors
    ///
    /// This function will return an error if the query contains a reserved table prefix,
    /// if the SQL type is incorrect, or if there is an issue executing the query.
    pub async fn migrate(&self, query: &str) -> Result<bool, Error> {
        if query.contains(RESERVED_TABLE_PREFIX) {
            return Err(Error::UsedReservedTablePrefix);
        }

        match Self::classify_sql(query) {
            SqlType::Migration => {
                let mut hasher = Sha256::new();
                hasher.update(query);
                let hash = format!("{:x}", hasher.finalize());

                // first check if the migration has already been run
                let mut rows = self
                    .connection
                    .query(
                        "SELECT COUNT(*) FROM __proven_migrations WHERE query_hash = ?1",
                        Self::convert_params(vec![SqlParam::Text(hash.to_string())]),
                    )
                    .await?;

                if let Some(row) = rows.next().await? {
                    if let Value::Integer(int) = row.get(0)? {
                        if int > 0 {
                            return Ok(false);
                        }
                    }
                }

                let transation = self.connection.transaction().await?;

                transation
                    .execute(query, Self::convert_params(vec![]))
                    .await?;

                let migration_params =
                    vec![SqlParam::Text(hash), SqlParam::Text(query.to_string())];

                transation
                    .execute(INSERT_MIGRATION_SQL, Self::convert_params(migration_params))
                    .await?;

                transation.commit().await?;

                Ok(true)
            }
            SqlType::Mutation => Err(Error::IncorrectSqlType(
                SqlType::Migration,
                SqlType::Mutation,
            )),
            SqlType::Query => Err(Error::IncorrectSqlType(SqlType::Migration, SqlType::Query)),
        }
    }

    /// Executes a query SQL statement with the given parameters.
    ///
    /// # Errors
    ///
    /// This function will return an error if the query contains a reserved table prefix,
    /// if the SQL type is incorrect, or if there is an issue executing the query.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an issue with unwrapping the row value.
    pub async fn query(
        &self,
        query: &str,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn Stream<Item = Vec<SqlParam>> + Send + Unpin>, Error> {
        if query.contains(RESERVED_TABLE_PREFIX) {
            return Err(Error::UsedReservedTablePrefix);
        }

        match Self::classify_sql(query) {
            SqlType::Migration => Err(Error::IncorrectSqlType(SqlType::Query, SqlType::Migration)),
            SqlType::Mutation => Err(Error::IncorrectSqlType(SqlType::Query, SqlType::Mutation)),
            SqlType::Query => {
                let libsql_params = Self::convert_params(params);

                let libsql_rows = self
                    .connection
                    .query(query, libsql_params)
                    .await
                    .map_err(Error::from)?;

                let column_count = libsql_rows.column_count();

                Ok(Box::new(Box::pin(libsql_rows.into_stream().map(
                    move |r| {
                        r.map_or_else(
                            |_| Vec::new(),
                            |row| {
                                (0..column_count)
                                    .filter_map(move |i| {
                                        row.get_value(i).ok().map(|value| match value {
                                            Value::Null => SqlParam::Null,
                                            Value::Integer(i) => SqlParam::Integer(i),
                                            Value::Real(r) => SqlParam::Real(r),
                                            Value::Text(s) => SqlParam::Text(s),
                                            Value::Blob(b) => SqlParam::Blob(Bytes::from(b)),
                                        })
                                    })
                                    .collect::<Vec<SqlParam>>()
                            },
                        )
                    },
                ))))
            }
        }
    }

    fn convert_params(params: Vec<SqlParam>) -> Vec<Value> {
        params
            .into_iter()
            .map(|p| match p {
                SqlParam::Null => Value::Null,
                SqlParam::Integer(i) => Value::Integer(i),
                SqlParam::Real(r) => Value::Real(r),
                SqlParam::Text(s) => Value::Text(s),
                SqlParam::Blob(b) => Value::Blob(b.to_vec()),
            })
            .collect()
    }

    fn classify_sql(sql: &str) -> SqlType {
        let sql = sql.trim_start().to_uppercase();

        if sql.starts_with("SELECT") {
            SqlType::Query
        } else if sql.starts_with("CREATE") || sql.starts_with("ALTER") || sql.starts_with("DROP") {
            SqlType::Migration
        } else {
            SqlType::Mutation
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::pin;

    use super::*;

    #[tokio::test]
    async fn test_execute() {
        let db = Database::connect(":memory:").await.unwrap();
        let result = db
            .execute(
                "INSERT INTO users (email) VALUES ('test@example.com')",
                vec![],
            )
            .await;
        assert!(result.is_err()); // Will fail because table doesn't exist, but for the right reason
    }

    #[tokio::test]
    async fn test_basics() {
        let db = Database::connect(":memory:").await.unwrap();

        db.migrate("CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)")
            .await
            .unwrap();

        db.execute(
            "INSERT INTO users (id, email) VALUES (?1, ?2)",
            vec![
                SqlParam::Integer(1),
                SqlParam::Text("alice@example.com".to_string()),
            ],
        )
        .await
        .unwrap();

        let rows = db
            .query("SELECT id, email FROM users", vec![])
            .await
            .unwrap();

        pin!(rows);

        if let Some(row) = rows.next().await {
            assert_eq!(
                row,
                vec![
                    SqlParam::Integer(1),
                    SqlParam::Text("alice@example.com".to_string())
                ]
            );
        } else {
            panic!("No rows returned");
        }

        assert!(rows.next().await.is_none(), "Too many rows returned");
    }

    #[tokio::test]
    async fn test_create_table_can_only_change_via_migration() {
        let db = Database::connect(":memory:").await.unwrap();

        let result = db
            .execute(
                "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
                vec![],
            )
            .await;

        assert!(result.is_err());

        let result = db
            .migrate("CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)")
            .await;

        assert!(result.is_ok());
    }
}
