use std::process::ExitStatus;

use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The enclave has already been started.
    #[error("The enclave has already been started")]
    AlreadyStarted,

    /// Address parsing failed.
    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),

    /// Application manager error.
    #[error(transparent)]
    ApplicationManager(#[from] proven_applications::Error),

    /// AWS Secrets Manager error.
    #[error(transparent)]
    AsmStore(#[from] proven_store_asm::Error),

    /// Tokio task error.
    #[error(transparent)]
    Async(#[from] tokio::task::JoinError),

    /// Bad key error.
    #[error("The key is invalid")]
    BadKey,

    /// Bad UTF-8 error.
    #[error(transparent)]
    BadUtf8(#[from] std::string::FromUtf8Error),

    /// Bootable error.
    #[error(transparent)]
    Bootable(Box<dyn std::error::Error + Send + Sync>),

    /// CIDR parsing error.
    #[error(transparent)]
    Cidr(#[from] cidr::errors::NetworkParseError),

    /// Consensus error.
    #[error("Consensus error: {0}")]
    Consensus(String),

    /// `Core` error.
    #[error(transparent)]
    Core(#[from] proven_core::Error),

    /// DNS proxy error.
    #[error(transparent)]
    DnscryptProxy(#[from] proven_dnscrypt_proxy::Error),

    /// External file system error.
    #[error(transparent)]
    ExternalFs(#[from] proven_external_fs::Error),

    /// Identity manager error.
    #[error(transparent)]
    IdentityManager(#[from] proven_identity::Error),

    /// Instance metadata error.
    #[error(transparent)]
    Imds(#[from] proven_imds::Error),

    /// Instance details error.
    #[error(transparent)]
    InstanceDetails(#[from] proven_instance_details::Error),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// JSON parsing error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Key management service error.
    #[error(transparent)]
    Kms(#[from] proven_kms::Error),

    /// NATS monitor error.
    #[error(transparent)]
    NatsMonitor(#[from] proven_nats_monitor::Error),

    /// NATS server error.
    #[error(transparent)]
    NatsServer(#[from] proven_nats_server::Error),

    /// NATS KV error.
    #[error(transparent)]
    NatsStore(#[from] proven_store_nats::Error),

    /// Network configuration error.
    #[cfg(target_os = "linux")]
    #[error(transparent)]
    Netlink(#[from] rtnetlink::Error),

    /// No loopback interface found.
    #[cfg(target_os = "linux")]
    #[error("No loopback interface found")]
    NoLoopback,

    /// Process exited with non-zero status.
    #[error("{0} unexpectedly exited with non-zero code: {1}")]
    NonZeroExit(&'static str, ExitStatus),

    /// Nitro Secure Module error.
    #[error(transparent)]
    Nsm(#[from] proven_attestation_nsm::Error),

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

    /// Simple Storage Service error.
    #[error(transparent)]
    S3Store(#[from] proven_store_s3::Error),

    /// VSOCK L3 network proxy error.
    #[error(transparent)]
    VsockProxy(#[from] proven_vsock_proxy::Error),

    /// VSOCK RPC CAC error.
    #[error(transparent)]
    VsockRpcCac(#[from] proven_vsock_cac::Error),

    /// VSOCK tracing error.
    #[error(transparent)]
    VsockTracing(#[from] proven_vsock_tracing::enclave::Error),
}
