use std::sync::Arc;

use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// CBOR deserialization error.
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// CBOR serialization error.
    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// Error from eszip.
    #[error("failure handling eszip: {0}")]
    CodePackage(String),

    /// Issue with the module graph.
    #[error(transparent)]
    ModuleGraph(#[from] deno_graph::ModuleError),
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}
