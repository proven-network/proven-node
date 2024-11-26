mod error;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use error::Error;
use proven_libsql::Database;
use proven_sql::{Rows, SqlConnection, SqlParam, SqlStore, SqlStore1, SqlStore2, SqlStore3};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct DirectSqlStore {
    path: PathBuf,
}

impl DirectSqlStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl SqlStore for DirectSqlStore {
    type Error = Error;
    type Connection = Connection;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let database = Arc::new(Mutex::new(Database::connect(self.path.clone()).await));

        Ok(Connection { database })
    }
}

#[derive(Clone)]
pub struct DirectSqlStore1 {
    path: PathBuf,
}

#[async_trait]
impl SqlStore1 for DirectSqlStore1 {
    type Error = Error;
    type Scoped = DirectSqlStore;

    fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut new_path = self.path.clone();
        new_path.set_file_name(format!(
            "{}_{}",
            self.path.file_name().unwrap().to_string_lossy(),
            scope.into()
        ));
        DirectSqlStore { path: new_path }
    }
}

#[derive(Clone)]
pub struct DirectSqlStore2 {
    path: PathBuf,
}

#[async_trait]
impl SqlStore2 for DirectSqlStore2 {
    type Error = Error;
    type Scoped = DirectSqlStore1;

    fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut new_path = self.path.clone();
        new_path.set_file_name(format!(
            "{}_{}",
            self.path.file_name().unwrap().to_string_lossy(),
            scope.into()
        ));
        DirectSqlStore1 { path: new_path }
    }
}

#[derive(Clone)]
pub struct DirectSqlStore3 {
    path: PathBuf,
}

#[async_trait]
impl SqlStore3 for DirectSqlStore3 {
    type Error = Error;
    type Scoped = DirectSqlStore2;

    fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut new_path = self.path.clone();
        new_path.set_file_name(format!(
            "{}_{}",
            self.path.file_name().unwrap().to_string_lossy(),
            scope.into()
        ));
        DirectSqlStore2 { path: new_path }
    }
}

#[derive(Clone)]
pub struct Connection {
    database: Arc<Mutex<Database>>,
}

#[async_trait]
impl SqlConnection for Connection {
    type Error = Error;

    async fn execute<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        Ok(self
            .database
            .lock()
            .await
            .execute(&query.into(), params)
            .await?)
    }

    async fn execute_batch<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, Self::Error> {
        Ok(self
            .database
            .lock()
            .await
            .execute_batch(&query.into(), params)
            .await?)
    }

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error> {
        Ok(self.database.lock().await.migrate(&query.into()).await?)
    }

    async fn query<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Rows, Self::Error> {
        Ok(self
            .database
            .lock()
            .await
            .query(&query.into(), params)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_sql::SqlConnection;

    #[tokio::test]
    async fn test_sql_store() {
        let store = DirectSqlStore::new("test.db");
        let connection = store.connect().await.unwrap();

        let response = connection
            .migrate("CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)")
            .await
            .unwrap();

        assert!(response);

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
