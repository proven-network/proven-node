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
    ApplicationManager(
        #[from]
        proven_applications::Error<
            proven_sql_streamed::Error<
                proven_stream_nats::Error<proven_sql_streamed::stream_handler::Error>,
                proven_store_nats::Error,
            >,
        >,
    ),

    /// AWS Secrets Manager error.
    #[error(transparent)]
    AsmStore(#[from] proven_store_asm::Error),

    /// Tokio task error.
    #[error(transparent)]
    Async(#[from] tokio::task::JoinError),

    /// Babylon aggregator error.
    #[error(transparent)]
    BabylonAggregator(#[from] proven_radix_aggregator::Error),

    /// Babylon gateway error.
    #[error(transparent)]
    BabylonGateway(#[from] proven_radix_gateway::Error),

    /// Babylon node error.
    #[error(transparent)]
    BabylonNode(#[from] proven_radix_node::Error),

    /// Bad key error.
    #[error("The key is invalid")]
    BadKey,

    /// Bad UTF-8 error.
    #[error(transparent)]
    BadUtf8(#[from] std::string::FromUtf8Error),

    /// CIDR parsing error.
    #[error(transparent)]
    Cidr(#[from] cidr::errors::NetworkParseError),

    /// `Core` error.
    #[error(transparent)]
    Core(#[from] proven_core::Error<proven_http_letsencrypt::Error<proven_store_s3::Error>>),

    /// DNS proxy error.
    #[error(transparent)]
    DnscryptProxy(#[from] proven_dnscrypt_proxy::Error),

    /// External file system error.
    #[error(transparent)]
    ExternalFs(#[from] proven_external_fs::Error),

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

    /// Simple Storage Service error.
    #[error(transparent)]
    S3Store(#[from] proven_store_s3::Error),

    /// VSOCK L3 network proxy error.
    #[error(transparent)]
    VsockProxy(#[from] proven_vsock_proxy::Error),

    /// VSOCK RPC error.
    #[error(transparent)]
    VsockRpc(#[from] proven_vsock_rpc::Error),

    /// VSOCK tracing error.
    #[error(transparent)]
    VsockTracing(#[from] proven_vsock_tracing::enclave::Error),
}
