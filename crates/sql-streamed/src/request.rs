use bytes::Bytes;
use proven_sql::SqlParam;
use serde::{Deserialize, Serialize};

/// A request to a SQL store.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Request {
    /// Executes a SQL mutation.
    Execute(String, Vec<SqlParam>),

    /// Executes a batch of SQL mutations.
    ExecuteBatch(String, Vec<Vec<SqlParam>>),

    /// Executes a schema migration.
    Migrate(String),

    /// Executes a SQL query.
    Query(String, Vec<SqlParam>),
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
