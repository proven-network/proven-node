mod rows;
mod sql_param;

pub use rows::Rows;
pub use sql_param::SqlParam;

use async_trait::async_trait;
use std::error::Error;
use std::fmt::Debug;

/// Marker trait for SQLStore errors
pub trait SqlStoreError: Debug + Error + Send + Sync {}

#[async_trait]
pub trait SqlConnection: Clone + Send + Sync + 'static {
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
///
/// # Associated Types
/// - `Error`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
/// - `Connection`: The connection type that implements the `SqlConnection` trait.
///
/// # Required Methods
/// - `async fn connect(&self) -> Result<Self::Connection, Self::Error>`: Connects to the SQL store.
/// - `async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Self`: Executes a SQL statement that modifies schema and returns bool indicating if needed to be run.
#[async_trait]
pub trait SqlStore: Clone + Send + Sync + 'static {
    type Error: SqlStoreError;
    type Connection: SqlConnection<Error = Self::Error>;

    async fn connect(&self) -> Result<Self::Connection, Self::Error>;

    async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Self;
}

macro_rules! define_scoped_sql_store {
    ($name:ident, $parent:ident, $doc:expr) => {
        #[async_trait]
        #[doc = $doc]
        pub trait $name: Clone + Send + Sync + 'static {
            type Error: SqlStoreError;
            type Scoped: $parent<Error = Self::Error>;

            async fn migrate<Q: Into<String> + Send>(&self, query: Q) -> Self;

            fn scope<S: Clone + Into<String> + Send + 'static>(&self, scope: S) -> Self::Scoped;
        }
    };
}

define_scoped_sql_store!(
    SqlStore1,
    SqlStore,
    "A trait representing a single-scoped SQL store with asynchronous operations."
);
define_scoped_sql_store!(
    SqlStore2,
    SqlStore1,
    "A trait representing a double-scoped SQL store with asynchronous operations."
);
define_scoped_sql_store!(
    SqlStore3,
    SqlStore2,
    "A trait representing a triple-scoped SQL store with asynchronous operations."
);
