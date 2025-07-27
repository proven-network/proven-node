use crate::{Error, transaction::Transaction};

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use proven_libsql::Database;
use proven_sql::{SqlConnection, SqlParam};
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
    type Transaction = Transaction;

    async fn execute<Q: Clone + Into<String> + Send>(
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

    async fn execute_batch<Q: Clone + Into<String> + Send>(
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

    async fn migrate<Q: Clone + Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error> {
        Ok(self.database.lock().await.migrate(&query.into()).await?)
    }

    #[allow(clippy::significant_drop_in_scrutinee)]
    async fn query<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn futures::Stream<Item = Vec<SqlParam>> + Send + Unpin>, Self::Error> {
        match self
            .database
            .lock()
            .await
            .query(&query.into(), params)
            .await
        {
            Ok(query_stream) => Ok(Box::new(query_stream)),
            Err(e) => Err(e.into()),
        }
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let tx = self.database.lock().await.begin_transaction().await?;
        Ok(Transaction::new(tx))
    }
}
