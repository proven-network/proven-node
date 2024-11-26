use std::sync::Arc;

use libsql::{Builder, Connection, Rows};
use proven_sql::SqlParam;
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

    pub async fn execute(&self, query: &str, params: Vec<SqlParam>) -> Result<u64, libsql::Error> {
        let libsql_params = Self::convert_params(params);
        self.conn.lock().await.execute(query, libsql_params).await
    }

    pub async fn execute_batch(
        &self,
        query: &str,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, libsql::Error> {
        let libsql_params = params
            .into_iter()
            .map(Self::convert_params)
            .collect::<Vec<_>>();

        let locked = self.conn.lock().await;
        let mut total: u64 = 0;

        for params in libsql_params {
            total += locked.execute(query, params).await?;
        }

        Ok(total)
    }

    pub async fn query(&self, query: &str, params: Vec<SqlParam>) -> Result<Rows, libsql::Error> {
        let libsql_params = Self::convert_params(params);
        self.conn.lock().await.query(query, libsql_params).await
    }

    fn convert_params(params: Vec<SqlParam>) -> Vec<libsql::Value> {
        params
            .into_iter()
            .map(|p| match p {
                SqlParam::Null => libsql::Value::Null,
                SqlParam::Integer(i) => libsql::Value::Integer(i),
                SqlParam::Real(r) => libsql::Value::Real(r),
                SqlParam::Text(s) => libsql::Value::Text(s),
                SqlParam::Blob(b) => libsql::Value::Blob(b.to_vec()),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_execute() {
        let db = Database::connect().await;
        let result = db
            .execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", vec![])
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_query() {
        let db = Database::connect().await;

        let _ = db
            .execute(
                "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
                vec![],
            )
            .await;

        let _ = db
            .execute(
                "INSERT INTO users (id, email) VALUES (?1, ?2)",
                vec![
                    SqlParam::Integer(1),
                    SqlParam::Text("alice@example.com".to_string()),
                ],
            )
            .await;

        let result = db.query("SELECT id, email FROM users", vec![]).await;
        assert!(result.is_ok());

        let mut rows = result.unwrap();
        let row = rows.next().await.unwrap();
        let email: String = row.unwrap().get(1).unwrap();
        assert_eq!(email, "alice@example.com");
    }
}
