use bytes::Bytes;
use proven_sql::SqlParam;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A request to a SQL store.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Request {
    /// Executes a SQL mutation.
    Execute(String, Vec<SqlParam>),

    /// Executes a batch of SQL mutations.
    ExecuteBatch(String, Vec<Vec<SqlParam>>),

    /// Executes a schema migration.
    Migrate(String),

    /// Executes a SQL query.
    Query(String, Vec<SqlParam>),

    /// Begin a new transaction.
    BeginTransaction,

    /// Execute within a transaction.
    TransactionExecute(Uuid, String, Vec<SqlParam>),

    /// Query within a transaction.
    TransactionQuery(Uuid, String, Vec<SqlParam>),

    /// Commit a transaction.
    TransactionCommit(Uuid),

    /// Rollback a transaction.
    TransactionRollback(Uuid),
}

impl TryFrom<Bytes> for Request {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref())
    }
}

impl TryInto<Bytes> for Request {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
