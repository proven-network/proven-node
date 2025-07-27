use bytes::Bytes;
use proven_sql::SqlParam;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A response from a SQL store.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Response {
    /// Successful execution with affected rows
    Execute(u64),

    /// Successful batch execution with total affected rows
    ExecuteBatch(u64),

    /// Query row result
    Row(Vec<SqlParam>),

    /// Query result with all rows
    Rows(Vec<Vec<SqlParam>>),

    /// Migration result (whether it needed to run)
    Migrate(bool),

    /// Operation failed with error
    Failed(crate::libsql_error::LibsqlError),

    /// Transaction began successfully
    TransactionBegun(Uuid),

    /// Transaction execute result
    TransactionExecute(u64),

    /// Transaction query result  
    TransactionRow(Vec<SqlParam>),

    /// Transaction committed successfully
    TransactionCommitted,

    /// Transaction rolled back successfully
    TransactionRolledBack,
}

impl TryFrom<Bytes> for Response {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref())
    }
}

impl TryInto<Bytes> for Response {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
