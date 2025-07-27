//! Abstract interface for managing SQL storage.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod sql_param;

pub use sql_param::SqlParam;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use deno_error::JsErrorClass;

/// Marker trait for `SQLStore` errors
pub trait SqlStoreError: Debug + Error + JsErrorClass + Send + Sync {}

/// A trait representing an active connection to a SQL DB.
#[async_trait]
pub trait SqlConnection
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the connection
    type Error: SqlStoreError;

    /// Execute a SQL statement that modifies data
    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error>;

    /// Execute a batch of SQL statements that modify data
    async fn execute_batch<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, Self::Error>;

    /// Execute a SQL statement that modifies schema and returns bool indicating if needed to be run
    async fn migrate<Q: Clone + Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error>;

    /// Execute a SQL query that returns data
    async fn query<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn futures::Stream<Item = Vec<SqlParam>> + Send + Unpin>, Self::Error>;

    /// The transaction type for this connection
    type Transaction: SqlTransaction<Error = Self::Error>;

    /// Begin a new transaction
    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error>;
}

/// A trait representing an active SQL transaction.
#[async_trait]
pub trait SqlTransaction
where
    Self: Send + Sync + 'static,
{
    /// The error type for the transaction
    type Error: SqlStoreError;

    /// Execute a SQL statement within the transaction
    async fn execute<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error>;

    /// Execute a SQL query within the transaction
    async fn query<Q: Clone + Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Box<dyn futures::Stream<Item = Vec<SqlParam>> + Send + Unpin>, Self::Error>;

    /// Commit the transaction
    async fn commit(self) -> Result<(), Self::Error>;

    /// Rollback the transaction
    async fn rollback(self) -> Result<(), Self::Error>;
}

/// A trait representing a SQL store with asynchronous operations.
#[async_trait]
pub trait SqlStore
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the store
    type Error: SqlStoreError;

    /// The connection type for the store
    type Connection: SqlConnection<Error = Self::Error>;

    /// Connect to the SQL store - running any provided migrations
    async fn connect<Q: Clone + Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Self::Connection, Self::Error>;
}

macro_rules! define_scoped_sql_store {
    ($index:expr_2021, $parent:ident, $doc:expr_2021) => {
        paste::paste! {
            #[doc = $doc]
            pub trait [< SqlStore $index >]
            where
                Self: Clone + Send + Sync + 'static,
            {
                /// The error type for the store.
                type Error: SqlStoreError;

                /// The connection type for the store.
                type Connection: SqlConnection<Error = Self::Error>;

                /// The scoped version of the store.
                type Scoped: $parent<Error = Self::Error> + Clone + Send + Sync + 'static;

                /// Create a scoped version of the store.
                fn scope<S>(&self, scope: S) -> Self::Scoped
                where
                    S: AsRef<str> + Copy + Send;
            }
        }
    };
}

define_scoped_sql_store!(
    1,
    SqlStore,
    "A trait representing a single-scoped SQL store with asynchronous operations."
);
define_scoped_sql_store!(
    2,
    SqlStore1,
    "A trait representing a double-scoped SQL store with asynchronous operations."
);
define_scoped_sql_store!(
    3,
    SqlStore2,
    "A trait representing a triple-scoped SQL store with asynchronous operations."
);
