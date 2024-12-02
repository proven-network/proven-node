use bytes::Bytes;
use proven_sql::Rows;
use serde::{Deserialize, Serialize};

/// A response from a SQL store.
/// TODO: Handle error responses.
#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    /// The number of rows affected by a mutation.
    Execute(u64),

    /// The number of rows affected by a batch of mutations.
    ExecuteBatch(u64),

    /// Whether a schema migration was needed.
    Migrate(bool),

    /// The rows returned by a query.
    Query(Rows),
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
