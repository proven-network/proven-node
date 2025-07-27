//! Implementation of SQL storage using files on disk, for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod connection;
mod error;
mod transaction;

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
    ($index:expr_2021, $parent:ident, $parent_trait:ident, $doc:expr_2021) => {
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

                type Connection = Connection;

                type Scoped = $parent;

                fn scope<S>(&self, scope: S) -> Self::Scoped
                where
                    S: AsRef<str> + Copy + Send,
                {
                    let mut new_dir = self.dir.clone();
                    new_dir.push(scope.as_ref());
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
    use proven_sql::{SqlConnection, SqlParam, SqlTransaction};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sql_store() {
        let mut dir = tempdir().unwrap().keep();
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
        let mut dir = tempdir().unwrap().keep();
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

    #[tokio::test]
    async fn test_transactions() {
        let dir = tempdir().unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test_transactions.db");

        let store = DirectSqlStore::new(path);
        let connection = store
            .connect(vec![
                "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER)",
            ])
            .await
            .unwrap();

        // Insert initial data
        connection
            .execute(
                "INSERT INTO accounts (id, balance) VALUES (?1, ?2), (?3, ?4)",
                vec![
                    SqlParam::Integer(1),
                    SqlParam::Integer(100),
                    SqlParam::Integer(2),
                    SqlParam::Integer(50),
                ],
            )
            .await
            .unwrap();

        // Test successful transaction
        let tx = connection.begin_transaction().await.unwrap();

        tx.execute(
            "UPDATE accounts SET balance = balance - ?1 WHERE id = ?2",
            vec![SqlParam::Integer(30), SqlParam::Integer(1)],
        )
        .await
        .unwrap();

        tx.execute(
            "UPDATE accounts SET balance = balance + ?1 WHERE id = ?2",
            vec![SqlParam::Integer(30), SqlParam::Integer(2)],
        )
        .await
        .unwrap();

        tx.commit().await.unwrap();

        // Verify the transaction succeeded
        let mut rows = connection
            .query("SELECT id, balance FROM accounts ORDER BY id", vec![])
            .await
            .unwrap();

        let first_row = rows.next().await.unwrap();
        assert_eq!(
            first_row,
            vec![
                SqlParam::IntegerWithName("id".to_string(), 1),
                SqlParam::IntegerWithName("balance".to_string(), 70),
            ]
        );

        let second_row = rows.next().await.unwrap();
        assert_eq!(
            second_row,
            vec![SqlParam::Integer(2), SqlParam::Integer(80),]
        );

        // Test rollback
        let tx = connection.begin_transaction().await.unwrap();

        tx.execute(
            "UPDATE accounts SET balance = balance - ?1 WHERE id = ?2",
            vec![SqlParam::Integer(20), SqlParam::Integer(1)],
        )
        .await
        .unwrap();

        tx.rollback().await.unwrap();

        // Verify rollback worked - balance should be unchanged
        let mut rows = connection
            .query(
                "SELECT balance FROM accounts WHERE id = ?1",
                vec![SqlParam::Integer(1)],
            )
            .await
            .unwrap();

        let row = rows.next().await.unwrap();
        assert_eq!(
            row,
            vec![SqlParam::IntegerWithName("balance".to_string(), 70)]
        );
    }
}
