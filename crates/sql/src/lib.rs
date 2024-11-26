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
pub trait Connection: Clone + Send + Sync + 'static {
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

#[async_trait]
pub trait SqlStore: Clone + Send + Sync + 'static {
    type Error: SqlStoreError;
    type Connection: Connection<Error = Self::Error>;

    async fn connect<N: Clone + Into<String> + Send + 'static>(
        &self,
        db_name: N,
    ) -> Result<Self::Connection, Self::Error>;
}

#[async_trait]
pub trait SqlStore1: Clone + Send + Sync + 'static {
    type Error: SqlStoreError;
    type Scoped: SqlStore<Error = Self::Error>;

    fn scope<S: Clone + Into<String> + Send + 'static>(&self, scope: S) -> Self::Scoped;
}

#[async_trait]
/// Double-scoped SQL interface (e.g. for tenant isolation)
pub trait SqlStore2: Clone + Send + Sync + 'static {
    type Error: SqlStoreError;
    type Scoped: SqlStore1<Error = Self::Error>;

    fn scope<S: Clone + Into<String> + Send + 'static>(&self, scope: S) -> Self::Scoped;
}

#[async_trait]
/// Triple-scoped SQL interface
pub trait SqlStore3: Clone + Send + Sync + 'static {
    type Error: SqlStoreError;
    type Scoped: SqlStore2<Error = Self::Error>;

    fn scope<S: Clone + Into<String> + Send + 'static>(&self, scope: S) -> Self::Scoped;
}
