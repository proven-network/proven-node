use crate::Error;

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use proven_libsql::Database;
use proven_sql::{Rows, SqlConnection, SqlParam};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct Connection {
    database: Arc<Mutex<Database>>,
}

impl Connection {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        let database = Arc::new(Mutex::new(Database::connect(path).await?));

        Ok(Self { database })
    }
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
