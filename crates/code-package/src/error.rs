use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Error from eszip.
    #[error("failure handling eszip: {0}")]
    CodePackage(String),

    /// Issue with the module graph.
    #[error(transparent)]
    ModuleGraph(#[from] deno_graph::ModuleError),
}
