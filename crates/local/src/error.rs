use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    /// Already started
    #[error("already started")]
    AlreadyStarted,

    /// Core error
    #[error(transparent)]
    Core(#[from] proven_core::Error),

    /// Ethereum Reth error
    #[error(transparent)]
    EthereumReth(#[from] proven_ethereum_reth::Error),

    /// Could not set global default subscriber.
    #[error("could not set global default subscriber: {0}")]
    SetTracing(#[from] tracing::dispatcher::SetGlobalDefaultError),

    /// IO error
    #[error("IO error: {0}")]
    Io(String),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// NATS server error.
    #[error(transparent)]
    NatsServer(#[from] proven_nats_server::Error),

    /// Postgres error.
    #[error(transparent)]
    Postgres(#[from] proven_postgres::Error),

    /// Error related to private key operations.
    #[error("private key error: {0}")]
    PrivateKey(String),

    /// Proven network error.
    #[error(transparent)]
    ProvenNetwork(#[from] proven_network::Error),

    /// Babylon aggregator error.
    #[error(transparent)]
    RadixAggregator(#[from] proven_radix_aggregator::Error),

    /// Babylon gateway error.
    #[error(transparent)]
    RadixGateway(#[from] proven_radix_gateway::Error),

    /// Babylon node error.
    #[error(transparent)]
    RadixNode(#[from] proven_radix_node::Error),
}
