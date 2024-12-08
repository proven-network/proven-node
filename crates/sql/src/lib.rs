//! Abstract interface for managing SQL storage.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod rows;
mod sql_param;

pub use rows::Rows;
pub use sql_param::SqlParam;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for `SQLStore` errors
pub trait SqlStoreError: Debug + Error + Send + Sync {}

/// A trait representing an active connection to a SQL DB.
#[async_trait]
pub trait SqlConnection
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the connection
    type Error: SqlStoreError;

    /// Execute a SQL statement that modifies data
    async fn execute<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<u64, Self::Error>;

    /// Execute a batch of SQL statements that modify data
    async fn execute_batch<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<Vec<SqlParam>>,
    ) -> Result<u64, Self::Error>;

    /// Execute a SQL statement that modifies schema and returns bool indicating if needed to be run
    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Result<bool, Self::Error>;

    /// Execute a SQL query that returns data
    async fn query<Q: Into<String> + Send>(
        &self,
        query: Q,
        params: Vec<SqlParam>,
    ) -> Result<Rows, Self::Error>;
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
    async fn connect<Q: Into<String> + Send>(
        &self,
        migrations: Vec<Q>,
    ) -> Result<Self::Connection, Self::Error>;
}

macro_rules! define_scoped_sql_store {
    ($index:expr, $parent:ident, $doc:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc]
            pub trait [< SqlStore $index >]
            where
                Self: Clone + Send + Sync + 'static,
            {
                /// The error type for the store.
                type Error: SqlStoreError;

                /// The scoped version of the store.
                type Scoped: $parent<Error = Self::Error> + Clone + Send + Sync + 'static;

                /// Create a scoped version of the store.
                fn scope<S: Clone + Into<String> + Send + 'static>(&self, scope: S) -> Self::Scoped;
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
