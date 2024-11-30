mod connection;
mod error;

use connection::Connection;

use std::path::PathBuf;

use async_trait::async_trait;
pub use error::Error;
use proven_sql::{SqlConnection, SqlStore, SqlStore1, SqlStore2, SqlStore3};

#[derive(Clone)]
pub struct DirectSqlStore {
    dir: PathBuf,
}

impl DirectSqlStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

#[async_trait]
impl SqlStore for DirectSqlStore {
    type Error = Error;
    type Connection = Connection;

    async fn connect<Q: Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Self::Connection, Self::Error> {
        // Ensure the directory exists
        tokio::fs::create_dir_all(self.dir.parent().unwrap())
            .await
            .unwrap();

        let connection = Connection::new(self.dir.clone()).await?;

        for migration in migrations {
            connection.migrate(migration).await?;
        }

        Ok(connection)
    }
}

#[async_trait]
impl SqlStore1 for DirectSqlStore {
    type Error = Error;
    type Scoped = Self;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut dir = self.dir.clone();
        dir.push(scope.into());
        Self::new(dir)
    }
}

#[async_trait]
impl SqlStore2 for DirectSqlStore {
    type Error = Error;
    type Scoped = Self;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut dir = self.dir.clone();
        dir.push(scope.into());
        Self::new(dir)
    }
}

#[async_trait]
impl SqlStore3 for DirectSqlStore {
    type Error = Error;
    type Scoped = Self;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut dir = self.dir.clone();
        dir.push(scope.into());
        Self::new(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proven_sql::{SqlConnection, SqlParam};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sql_store() {
        let mut dir = tempdir().unwrap().into_path();
        dir.push("test.db");

        let store = DirectSqlStore::new(dir);

        let connection = store
            .connect(vec![
                "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
            ])
            .await
            .unwrap();

        let response = connection
            .execute(
                "INSERT INTO users (id, email) VALUES (?1, ?2)",
                vec![
                    SqlParam::Integer(1),
                    SqlParam::Text("alice@example.com".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(response, 1);

        let response = connection
            .query("SELECT id, email FROM users", vec![])
            .await
            .unwrap();

        assert_eq!(response.column_count, 2);
        assert_eq!(response.column_names, vec!["id", "email"]);
        assert_eq!(response.column_types, vec!["INTEGER", "TEXT"]);
        assert_eq!(
            response.rows,
            vec![vec![
                SqlParam::Integer(1),
                SqlParam::Text("alice@example.com".to_string())
            ]]
        );
    }
}
