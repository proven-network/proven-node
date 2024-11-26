mod connection;
mod error;

use connection::Connection;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use error::Error;
use proven_sql::{SqlConnection, SqlStore, SqlStore1, SqlStore2, SqlStore3};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct DirectSqlStore {
    dir: PathBuf,
    migrations: Arc<Mutex<Vec<String>>>,
}

impl DirectSqlStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            migrations: Default::default(),
        }
    }
}

#[async_trait]
impl SqlStore for DirectSqlStore {
    type Error = Error;
    type Connection = Connection;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let connection = Connection::new(self.dir.clone()).await?;

        for migration in self.migrations.lock().await.iter() {
            connection.migrate(migration).await?;
        }

        Ok(connection)
    }

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Self {
        self.migrations.lock().await.push(query.into());

        self.clone()
    }
}

#[async_trait]
impl SqlStore1 for DirectSqlStore {
    type Error = Error;
    type Scoped = Self;

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Self {
        self.migrations.lock().await.push(query.into());

        self.clone()
    }

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

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Self {
        self.migrations.lock().await.push(query.into());

        self.clone()
    }

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

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Self {
        self.migrations.lock().await.push(query.into());

        self.clone()
    }

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
        let store = SqlStore::migrate(
            &store,
            "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
        )
        .await;
        let connection = store.connect().await.unwrap();

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
