//! Transaction implementation for sql-direct.

use std::sync::Arc;

use async_trait::async_trait;
use proven_sql::{SqlParam, SqlTransaction};
use tokio::sync::Mutex;

use crate::Error;

/// A transaction handle for sql-direct.
pub struct Transaction {
    /// The underlying `proven_libsql` transaction wrapped in `Arc<Mutex>` for thread safety
    inner: Arc<Mutex<Option<proven_libsql::Transaction>>>,
}

impl Transaction {
    /// Create a new transaction from a `proven_libsql` transaction.
    pub(crate) fn new(tx: proven_libsql::Transaction) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(tx))),
        }
    }
}

#[async_trait]
impl SqlTransaction for Transaction {
    type Error = Error;

    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.as_mut() {
            Ok(tx.execute(&query.into(), params).await?)
        } else {
            Err(Error::TransactionCompleted)
        }
    }

    async fn query<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn futures::Stream<Item = Vec<SqlParam>> + Send + Unpin>, Self::Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.as_mut() {
            Ok(tx.query(&query.into(), params).await?)
        } else {
            Err(Error::TransactionCompleted)
        }
    }

    async fn commit(self) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.take() {
            tx.commit().await?;
            Ok(())
        } else {
            Err(Error::TransactionCompleted)
        }
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.take() {
            tx.rollback().await?;
            Ok(())
        } else {
            Err(Error::TransactionCompleted)
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Transaction will be automatically rolled back by proven_libsql if not committed
    }
}
