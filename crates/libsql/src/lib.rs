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
use sql_type::StatementType;

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
            StatementType::Migration => Err(Error::IncorrectSqlType(
                StatementType::Mutation,
                StatementType::Migration,
            )),
            StatementType::Mutation => {
                let libsql_params = Self::convert_params(params);
                self.connection
                    .execute(query, libsql_params)
                    .await
                    .map_err(Error::from)
            }
            StatementType::Query => Err(Error::IncorrectSqlType(
                StatementType::Mutation,
                StatementType::Query,
            )),
            StatementType::Unknown => Err(Error::IncorrectSqlType(
                StatementType::Mutation,
                StatementType::Unknown,
            )),
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
            StatementType::Migration => Err(Error::IncorrectSqlType(
                StatementType::Mutation,
                StatementType::Migration,
            )),
            StatementType::Mutation => {
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
            StatementType::Query => Err(Error::IncorrectSqlType(
                StatementType::Mutation,
                StatementType::Query,
            )),
            StatementType::Unknown => Err(Error::IncorrectSqlType(
                StatementType::Mutation,
                StatementType::Unknown,
            )),
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
            StatementType::Migration => {
                // Check for schema names in table operations
                let upper_query = query.trim_start().to_uppercase();
                let mut words = upper_query.split_whitespace();
                while let Some(word) = words.next() {
                    if word == "TABLE" {
                        if let Some(next_word) = words.next() {
                            if next_word.contains('.') {
                                return Err(Error::SchemaNameNotAllowed);
                            }
                        }
                        break;
                    }
                }

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
            StatementType::Mutation => Err(Error::IncorrectSqlType(
                StatementType::Migration,
                StatementType::Mutation,
            )),
            StatementType::Query => Err(Error::IncorrectSqlType(
                StatementType::Migration,
                StatementType::Query,
            )),
            StatementType::Unknown => Err(Error::IncorrectSqlType(
                StatementType::Migration,
                StatementType::Unknown,
            )),
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
            StatementType::Migration => Err(Error::IncorrectSqlType(
                StatementType::Query,
                StatementType::Migration,
            )),
            StatementType::Mutation => Err(Error::IncorrectSqlType(
                StatementType::Query,
                StatementType::Mutation,
            )),
            StatementType::Query => {
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
            StatementType::Unknown => Err(Error::IncorrectSqlType(
                StatementType::Query,
                StatementType::Unknown,
            )),
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

    fn classify_sql(sql: &str) -> StatementType {
        let sql = sql.trim_start().to_uppercase();

        if sql.starts_with("SELECT") {
            StatementType::Query
        } else if sql.starts_with("CREATE TABLE")
            || sql.starts_with("ALTER TABLE")
            || sql.starts_with("DROP TABLE")
            || sql.starts_with("CREATE INDEX")
            || sql.starts_with("DROP INDEX")
        {
            StatementType::Migration
        } else if sql.starts_with("INSERT")
            || sql.starts_with("REPLACE")
            || sql.starts_with("UPDATE")
            || sql.starts_with("DELETE")
            || sql.starts_with("UPSERT")
        {
            StatementType::Mutation
        } else {
            StatementType::Unknown
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

    #[tokio::test]
    async fn test_schema_names_not_allowed_in_migrations() {
        let db = Database::connect(":memory:").await.unwrap();

        let result = db
            .migrate("CREATE TABLE main.users (id INTEGER, email TEXT)")
            .await;

        assert!(matches!(result, Err(Error::SchemaNameNotAllowed)));

        // Ensure regular table creation still works
        let result = db
            .migrate("CREATE TABLE users (id INTEGER, email TEXT)")
            .await;

        assert!(result.is_ok());
    }
}
