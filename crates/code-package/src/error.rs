use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Error from eszip.
    #[error("failure handling eszip: {0}")]
    CodePackage(String),

    /// Issue with the module graph.
    #[error(transparent)]
    ModuleGraph(Box<deno_graph::ModuleError>),
}

impl From<deno_graph::ModuleError> for Error {
    fn from(error: deno_graph::ModuleError) -> Self {
        Self::ModuleGraph(Box::new(error))
    }
}
