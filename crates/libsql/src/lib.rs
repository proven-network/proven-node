mod conversion;
mod error;
mod sql_type;

use conversion::convert_libsql_rows;
pub use error::{Error, Result};
use sql_type::SqlType;

use libsql::{Builder, Connection, Value};
use proven_sql::{Rows, SqlParam};

static RESERVED_TABLE_PREFIX: &str = "__proven_";
static CREATE_MIGRATIONS_TABLE_SQL: &str = include_str!("../sql/create_migrations_table.sql");
static INSERT_MIGRATION_SQL: &str = include_str!("../sql/insert_migration.sql");

#[derive(Clone)]
pub struct Database {
    connection: Connection,
}

impl Database {
    pub async fn connect(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let connection = Builder::new_local(path)
            .build()
            .await
            .unwrap()
            .connect()
            .unwrap();

        connection
            .execute(CREATE_MIGRATIONS_TABLE_SQL, Self::convert_params(vec![]))
            .await?;

        Ok(Self { connection })
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

    pub async fn execute(&self, query: &str, params: Vec<SqlParam>) -> Result<u64> {
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
                    .map_err(|e| Error::Libsql(e.into()))
            }
            SqlType::Query => Err(Error::IncorrectSqlType(SqlType::Mutation, SqlType::Query)),
        }
    }

    pub async fn execute_batch(&self, query: &str, params: Vec<Vec<SqlParam>>) -> Result<u64> {
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

    pub async fn migrate(&mut self, query: &str) -> Result<bool> {
        if query.contains(RESERVED_TABLE_PREFIX) {
            return Err(Error::UsedReservedTablePrefix);
        }

        match Self::classify_sql(query) {
            SqlType::Migration => {
                // first check if the migration has already been run
                let mut rows = self
                    .connection
                    .query(
                        "SELECT COUNT(*) FROM __proven_migrations WHERE query = ?1",
                        Self::convert_params(vec![SqlParam::Text(query.to_string())]),
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

                let migration_params = vec![
                    SqlParam::Integer(0),
                    SqlParam::Text("TODO: hash".to_string()),
                    SqlParam::Text(query.to_string()),
                ];

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

    pub async fn query(&self, query: &str, params: Vec<SqlParam>) -> Result<Rows> {
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
                    .map_err(|e| Error::Libsql(e.into()))?;

                convert_libsql_rows(libsql_rows).await
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
}

#[cfg(test)]
mod tests {
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
        let mut db = Database::connect(":memory:").await.unwrap();

        let response = db
            .migrate("CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)")
            .await;
        assert!(response.is_ok());

        let affected = db
            .execute(
                "INSERT INTO users (id, email) VALUES (?1, ?2)",
                vec![
                    SqlParam::Integer(1),
                    SqlParam::Text("alice@example.com".to_string()),
                ],
            )
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = db
            .query("SELECT id, email FROM users", vec![])
            .await
            .unwrap();

        assert_eq!(rows.column_count, 2);
        assert_eq!(
            rows.column_names,
            vec!["id".to_string(), "email".to_string()]
        );
        assert_eq!(
            rows.column_types,
            vec!["INTEGER".to_string(), "TEXT".to_string()]
        );
        assert_eq!(
            rows.rows,
            vec![vec![
                SqlParam::Integer(1),
                SqlParam::Text("alice@example.com".to_string())
            ]]
        );
    }

    #[tokio::test]
    async fn test_create_table_can_only_change_via_migration() {
        let mut db = Database::connect(":memory:").await.unwrap();

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
