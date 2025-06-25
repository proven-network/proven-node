//! Wrapper around [libsql](https://github.com/tursodatabase/libsql) which
//! provides additional functionality like migrations and backup.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod statement_type;

use std::fmt::Debug;
use std::future::Future;
use std::path::PathBuf;

pub use error::Error;
use statement_type::StatementType;

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
    connection: Option<Connection>,
    path: PathBuf,
}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("connection", &self.connection.is_some())
            .field("path", &self.path)
            .finish()
    }
}

impl Database {
    /// Connects to the database at the given path.
    ///
    /// # Errors
    ///
    /// This function will return an error if the connection to the database fails
    /// or if the path is `":memory:"`.
    pub async fn connect(path: impl AsRef<std::path::Path> + Send) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();

        if path.to_str() == Some(":memory:") {
            return Err(Error::MustUseFile);
        }

        let connection = Builder::new_local(&path).build().await?.connect()?;

        connection
            .execute(CREATE_MIGRATIONS_TABLE_SQL, Self::convert_params(vec![]))
            .await?;

        Ok(Self {
            connection: Some(connection),
            path,
        })
    }

    /// Temporarily disconnects from the database and runs the given backup function.
    ///
    /// # Errors
    ///
    /// This function will return an error if a backup is already in progress
    /// or if there is an issue restarting the connection.
    pub async fn backup<E, F, Fut>(&mut self, backup_fn: F) -> Result<Result<(), E>, Error>
    where
        E: std::error::Error + Send + 'static,
        F: FnOnce(std::path::PathBuf) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let connection = self.connection.take().ok_or(Error::BackupInProgress)?;

        drop(connection);
        let result = backup_fn(self.path.clone()).await;

        self.connection = Some(Builder::new_local(&self.path).build().await?.connect()?);

        Ok(result)
    }

    /// Gets the connection to the database.
    ///
    /// # Errors
    ///
    /// This function will return an error if a backup is in process.
    fn get_connection(&self) -> Result<&Connection, Error> {
        self.connection.as_ref().ok_or(Error::BackupInProgress)
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
                self.get_connection()?
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
                    total += self.get_connection()?.execute(query, params).await?;
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
                    .get_connection()?
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

                let transation = self.get_connection()?.transaction().await?;

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

                let mut libsql_rows = self
                    .get_connection()?
                    .query(query, libsql_params)
                    .await
                    .map_err(Error::from)?;

                let libsql_first_row_opt = libsql_rows.next().await?;

                // Early return empty stream if no results
                if libsql_first_row_opt.is_none() {
                    return Ok(Box::new(futures::stream::empty()));
                }

                let libsql_first_row = libsql_first_row_opt.unwrap();

                let column_count = libsql_rows.column_count();

                let first_row_with_names = (0..column_count)
                    .filter_map(|i| {
                        let column_name =
                            libsql_rows.column_name(i).unwrap_or_default().to_string();
                        libsql_first_row.get_value(i).ok().map(|value| match value {
                            Value::Blob(b) => SqlParam::BlobWithName(column_name, Bytes::from(b)),
                            Value::Integer(n) => SqlParam::IntegerWithName(column_name, n),
                            Value::Null => SqlParam::NullWithName(column_name),
                            Value::Real(r) => SqlParam::RealWithName(column_name, r),
                            Value::Text(s) => SqlParam::TextWithName(column_name, s),
                        })
                    })
                    .collect::<Vec<SqlParam>>();

                Ok(Box::new(Box::pin(
                    // Reinclude the first row since it was consumed to peek for empty results
                    futures::stream::once(async { first_row_with_names }).chain(
                        libsql_rows.into_stream().map(move |r| {
                            r.map_or_else(
                                |_| Vec::new(),
                                |row| {
                                    (0..column_count)
                                        .filter_map(move |i| {
                                            row.get_value(i).ok().map(|value| match value {
                                                Value::Blob(b) => SqlParam::Blob(Bytes::from(b)),
                                                Value::Integer(n) => SqlParam::Integer(n),
                                                Value::Null => SqlParam::Null,
                                                Value::Real(r) => SqlParam::Real(r),
                                                Value::Text(s) => SqlParam::Text(s),
                                            })
                                        })
                                        .collect::<Vec<SqlParam>>()
                                },
                            )
                        }),
                    ),
                )))
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
                SqlParam::Blob(b) | SqlParam::BlobWithName(_, b) => Value::Blob(b.to_vec()),
                SqlParam::Integer(i) | SqlParam::IntegerWithName(_, i) => Value::Integer(i),
                SqlParam::Null | SqlParam::NullWithName(_) => Value::Null,
                SqlParam::Real(r) | SqlParam::RealWithName(_, r) => Value::Real(r),
                SqlParam::Text(s) | SqlParam::TextWithName(_, s) => Value::Text(s),
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
    use super::*;

    use std::convert::Infallible;

    use tempfile::NamedTempFile;
    use tokio::pin;

    fn get_temp_db_path() -> PathBuf {
        NamedTempFile::new().unwrap().into_temp_path().to_path_buf()
    }

    #[tokio::test]
    async fn test_connect_file_only() {
        let result = Database::connect(":memory:").await;
        assert!(matches!(result, Err(Error::MustUseFile)));
    }

    #[tokio::test]
    async fn test_execute() {
        let db_path = get_temp_db_path();
        let db = Database::connect(db_path).await.unwrap();
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
        let db_path = get_temp_db_path();
        let db = Database::connect(db_path).await.unwrap();

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

        match rows.next().await {
            Some(row) => {
                assert_eq!(
                    row,
                    vec![
                        SqlParam::IntegerWithName("id".to_string(), 1),
                        SqlParam::TextWithName(
                            "email".to_string(),
                            "alice@example.com".to_string()
                        )
                    ]
                );
            }
            _ => {
                panic!("No rows returned");
            }
        }

        assert!(rows.next().await.is_none(), "Too many rows returned");
    }

    #[tokio::test]
    async fn test_no_results() {
        let db_path = get_temp_db_path();
        let db = Database::connect(db_path).await.unwrap();

        db.migrate("CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)")
            .await
            .unwrap();

        let result = db.query("SELECT id, email FROM users", vec![]).await;

        assert!(result.is_ok());

        let result = result.unwrap().next().await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_create_table_can_only_change_via_migration() {
        let db_path = get_temp_db_path();
        let db = Database::connect(db_path).await.unwrap();

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
        let db_path = get_temp_db_path();
        let db = Database::connect(db_path).await.unwrap();

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

    #[tokio::test]
    async fn test_vector_search() {
        let db_path = get_temp_db_path();
        let db = Database::connect(db_path).await.unwrap();

        db.migrate(
            "CREATE TABLE movies (
            title TEXT,
            year INT,
            embedding FLOAT32(3)
        );",
        )
        .await
        .unwrap();

        db.execute(
            "INSERT INTO movies (title, year, embedding)
            VALUES
            (
                'Napoleon',
                2023,
                vector('[1,2,3]')
            ),
            (
                'Black Hawk Down',
                2001,
                vector('[10,11,12]')
            ),
            (
                'Gladiator',
                2000,
                vector('[7,8,9]')
            ),
            (
                'Blade Runner',
                1982,
                vector('[4,5,6]')
            );",
            vec![],
        )
        .await
        .unwrap();

        let rows = db.query("SELECT title, vector_extract(embedding), vector_distance_cos(embedding, '[5,6,7]') FROM movies;", vec![])
            .await
            .unwrap();

        pin!(rows);

        // Expect 4 rows back with distances
        let mut count = 0;
        while let Some(row) = rows.next().await {
            count += 1;
            match count {
                1 => {
                    assert_eq!(
                        row,
                        vec![
                            SqlParam::TextWithName("title".to_string(), "Napoleon".to_string()),
                            SqlParam::TextWithName(
                                "vector_extract(embedding)".to_string(),
                                "[1,2,3]".to_string()
                            ),
                            SqlParam::RealWithName(
                                "vector_distance_cos(embedding, '[5,6,7]')".to_string(),
                                0.031_670_335_680_246_35
                            )
                        ]
                    );
                }
                2 => {
                    assert_eq!(
                        row,
                        vec![
                            SqlParam::Text("Black Hawk Down".to_string()),
                            SqlParam::Text("[10,11,12]".to_string()),
                            SqlParam::Real(0.001_869_742_991_402_745_2)
                        ]
                    );
                }
                3 => {
                    assert_eq!(
                        row,
                        vec![
                            SqlParam::Text("Gladiator".to_string()),
                            SqlParam::Text("[7,8,9]".to_string()),
                            SqlParam::Real(0.000_562_482_455_279_678_1)
                        ]
                    );
                }
                4 => {
                    assert_eq!(
                        row,
                        vec![
                            SqlParam::Text("Blade Runner".to_string()),
                            SqlParam::Text("[4,5,6]".to_string()),
                            SqlParam::Real(0.000_354_254_007_106_646_9)
                        ]
                    );
                }
                _ => panic!("Too many rows returned"),
            }
        }
    }

    #[tokio::test]
    async fn test_backup() {
        let db_path = get_temp_db_path();
        let mut db = Database::connect(db_path.clone()).await.unwrap();

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

        let backup_fn = |path: PathBuf| async move {
            // Simulate a successful backup operation
            assert_eq!(path, db_path);
            Ok(())
        };

        let result = db.backup::<Infallible, _, _>(backup_fn).await.unwrap();
        assert!(result.is_ok());

        // Verify the data is still there after the backup
        let rows = db
            .query("SELECT id, email FROM users", vec![])
            .await
            .unwrap();

        pin!(rows);

        match rows.next().await {
            Some(row) => {
                assert_eq!(
                    row,
                    vec![
                        SqlParam::IntegerWithName("id".to_string(), 1),
                        SqlParam::TextWithName(
                            "email".to_string(),
                            "alice@example.com".to_string()
                        )
                    ]
                );
            }
            _ => {
                panic!("No rows returned");
            }
        }

        assert!(rows.next().await.is_none(), "Too many rows returned");
    }

    #[tokio::test]
    async fn test_backup_failed_callback() {
        let db_path = get_temp_db_path();
        let mut db = Database::connect(db_path.clone()).await.unwrap();

        let backup_fn = |path: PathBuf| async move {
            // Simulate a failed backup operation
            assert_eq!(path, db_path);
            Err(std::io::Error::other("Backup failed"))
        };

        let result = db.backup(backup_fn).await.unwrap();
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e.to_string(), "Backup failed");
        }
    }
}
