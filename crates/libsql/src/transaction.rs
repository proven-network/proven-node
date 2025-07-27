//! Transaction wrapper for libsql.

use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use libsql::Value;
use proven_sql::SqlParam;
use tokio::sync::Mutex;

use crate::{Database, Error};

/// A wrapper around libsql transaction that provides our SQL interface.
pub struct Transaction {
    /// The underlying libsql transaction
    inner: Arc<Mutex<Option<libsql::Transaction>>>,
}

impl Transaction {
    /// Create a new transaction wrapper.
    pub(crate) fn new(tx: libsql::Transaction) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(tx))),
        }
    }

    /// Execute a SQL statement within the transaction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The transaction has already been committed or rolled back
    /// - The SQL execution fails
    pub async fn execute(&self, query: &str, params: Vec<SqlParam>) -> Result<u64, Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.as_mut() {
            Ok(tx.execute(query, Database::convert_params(params)).await?)
        } else {
            Err(Error::TransactionCompleted)
        }
    }

    /// Execute a SQL query within the transaction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The transaction has already been committed or rolled back
    /// - The SQL query fails
    ///
    /// # Panics
    ///
    /// This function will panic if the first row is `None`.
    pub async fn query(
        &self,
        query: &str,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn Stream<Item = Vec<SqlParam>> + Send + Unpin>, Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.as_mut() {
            let mut libsql_rows = tx.query(query, Database::convert_params(params)).await?;

            let libsql_first_row_opt = libsql_rows.next().await?;

            // Early return empty stream if no results
            if libsql_first_row_opt.is_none() {
                return Ok(Box::new(futures::stream::empty()));
            }

            let libsql_first_row = libsql_first_row_opt.expect("First row checked above");
            let column_count = libsql_rows.column_count();

            let first_row_with_names = (0..column_count)
                .filter_map(|i| {
                    let column_name = libsql_rows.column_name(i).unwrap_or_default().to_string();
                    libsql_first_row.get_value(i).ok().map(|value| match value {
                        Value::Blob(b) => SqlParam::BlobWithName(column_name, Bytes::from(b)),
                        Value::Integer(n) => SqlParam::IntegerWithName(column_name, n),
                        Value::Null => SqlParam::NullWithName(column_name),
                        Value::Real(r) => SqlParam::RealWithName(column_name, r),
                        Value::Text(s) => SqlParam::TextWithName(column_name, s),
                    })
                })
                .collect::<Vec<SqlParam>>();

            Ok(Box::new(Box::pin(
                // Reinclude the first row since it was consumed to peek for empty results
                futures::stream::once(async { first_row_with_names }).chain(
                    libsql_rows.into_stream().map(move |r| {
                        r.map_or_else(
                            |_| Vec::new(),
                            |row| {
                                (0..column_count)
                                    .filter_map(move |i| {
                                        row.get_value(i).ok().map(|value| match value {
                                            Value::Blob(b) => SqlParam::Blob(Bytes::from(b)),
                                            Value::Integer(n) => SqlParam::Integer(n),
                                            Value::Null => SqlParam::Null,
                                            Value::Real(r) => SqlParam::Real(r),
                                            Value::Text(s) => SqlParam::Text(s),
                                        })
                                    })
                                    .collect::<Vec<SqlParam>>()
                            },
                        )
                    }),
                ),
            )))
        } else {
            Err(Error::TransactionCompleted)
        }
    }

    /// Commit the transaction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The transaction has already been committed or rolled back
    /// - The commit operation fails
    pub async fn commit(self) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.take() {
            tx.commit().await?;
            Ok(())
        } else {
            Err(Error::TransactionCompleted)
        }
    }

    /// Rollback the transaction.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The transaction has already been committed or rolled back
    /// - The rollback operation fails
    pub async fn rollback(self) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.take() {
            tx.rollback().await?;
            Ok(())
        } else {
            Err(Error::TransactionCompleted)
        }
    }
}
