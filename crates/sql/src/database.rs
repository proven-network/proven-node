use crate::error::Result;

use std::sync::Arc;

use libsql::{params::IntoParams, Builder, Connection, Rows};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    pub async fn connect() -> Self {
        let conn = Builder::new_local(":memory:")
            .build()
            .await
            .unwrap()
            .connect()
            .unwrap();

        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    pub async fn execute(&self, query: &str, params: impl IntoParams) -> Result<u64> {
        Ok(self.conn.lock().await.execute(query, params).await?)
    }

    pub async fn query(&self, query: &str, params: impl IntoParams) -> Result<Rows> {
        Ok(self.conn.lock().await.query(query, params).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_execute() {
        let db = Database::connect().await;
        let result = db
            .execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_query() {
        let db = Database::connect().await;

        let _ = db
            .execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
            .await;

        let _ = db
            .execute(
                "INSERT INTO users (email) VALUES (?1)",
                vec!["alice@example.com"],
            )
            .await;

        let result = db.query("SELECT email FROM users", ()).await;
        assert!(result.is_ok());

        let mut rows = result.unwrap();
        let row = rows.next().await.unwrap();
        let email: String = row.unwrap().get(0).unwrap();
        assert_eq!(email, "alice@example.com");
    }
}
