//! Implementation of SQL storage using files on disk, for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod connection;
mod error;

use connection::Connection;

use std::path::PathBuf;

use async_trait::async_trait;
pub use error::Error;
use proven_sql::{SqlConnection, SqlStore, SqlStore1, SqlStore2, SqlStore3};

/// A SQL store that uses files on disk for local development.
#[derive(Clone, Debug)]
pub struct DirectSqlStore {
    dir: PathBuf,
}

impl DirectSqlStore {
    /// Creates a new `DirectSqlStore` with the specified directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

#[async_trait]
impl SqlStore for DirectSqlStore {
    type Error = Error;
    type Connection = Connection;

    async fn connect<Q: Clone + Into<String> + Send>(
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

macro_rules! impl_scoped_sql_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Clone, Debug)]
            pub struct [< DirectSqlStore $index >] {
                dir: PathBuf,
            }

            impl [< DirectSqlStore $index >] {
                /// Creates a new `[< DirectSqlStore $index >]` with the specified directory.
                pub fn new(dir: impl Into<PathBuf>) -> Self {
                    Self { dir: dir.into() }
                }
            }

            impl [< SqlStore $index >] for [< DirectSqlStore $index >] {
                type Error = Error;

                type Scoped = $parent;

                fn scope<S: Clone + Into<String> + Send + 'static>(&self, scope: S) -> Self::Scoped {
                    let mut new_dir = self.dir.clone();
                    new_dir.push(scope.into());
                    $parent::new(new_dir)
                }
            }
        }
    };
}

impl_scoped_sql_store!(
    1,
    DirectSqlStore,
    SqlStore,
    "A single-scoped SQL store that uses files on disk for local development."
);
impl_scoped_sql_store!(
    2,
    DirectSqlStore1,
    SqlStore1,
    "A double-scoped SQL store that uses files on disk for local development."
);
impl_scoped_sql_store!(
    3,
    DirectSqlStore2,
    SqlStore2,
    "A triple-scoped SQL store that uses files on disk for local development."
);

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use proven_sql::{SqlConnection, SqlParam};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sql_store() {
        let mut dir = tempdir().unwrap().into_path();
        dir.push("test_sql_store.db");

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

        let mut rows = connection
            .query("SELECT id, email FROM users", vec![])
            .await
            .unwrap();

        let mut results = Vec::new();
        while let Some(row) = rows.next().await {
            results.push(row);
        }

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            vec![
                SqlParam::IntegerWithName("id".to_string(), 1),
                SqlParam::TextWithName("email".to_string(), "alice@example.com".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn test_no_results() {
        let mut dir = tempdir().unwrap().into_path();
        dir.push("test_no_results.db");

        let store = DirectSqlStore::new(dir);

        let connection = store
            .connect(vec![
                "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
            ])
            .await
            .unwrap();

        let mut rows = connection
            .query("SELECT id, email FROM users", vec![])
            .await
            .unwrap();

        let mut results = Vec::new();
        while let Some(row) = rows.next().await {
            results.push(row);
        }

        assert_eq!(results.len(), 0);
    }
}
