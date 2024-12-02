use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The enclave has already been started")]
    AlreadyStarted,

    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),

    #[error(transparent)]
    ApplicationManager(
        #[from]
        proven_applications::Error<
            proven_sql_streamed::Error<
                proven_stream_nats::Error<proven_sql_streamed::SqlStreamHandler>,
                proven_store_nats::Error,
            >,
        >,
    ),

    #[error(transparent)]
    AsmStore(#[from] proven_store_asm::Error),

    #[error(transparent)]
    Async(#[from] tokio::task::JoinError),

    #[error(transparent)]
    BabylonAggregator(#[from] proven_radix_aggregator::Error),

    #[error(transparent)]
    BabylonGateway(#[from] proven_radix_gateway::Error),

    #[error(transparent)]
    BabylonNode(#[from] proven_radix_node::Error),

    #[error("The key is invalid")]
    BadKey,

    #[error(transparent)]
    Cidr(#[from] cidr::errors::NetworkParseError),

    #[error(transparent)]
    Core(#[from] proven_core::Error<proven_http_letsencrypt::Error<proven_store_s3::Error>>),

    #[error(transparent)]
    DnscryptProxy(#[from] proven_dnscrypt_proxy::Error),

    #[error(transparent)]
    ExternalFs(#[from] proven_external_fs::Error),

    #[error(transparent)]
    Imds(#[from] proven_imds::Error),

    #[error(transparent)]
    InstanceDetails(#[from] proven_instance_details::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Kms(#[from] proven_kms::Error),

    #[error(transparent)]
    NatsMonitor(#[from] proven_nats_monitor::Error),

    #[error(transparent)]
    NatsServer(#[from] proven_nats_server::Error),

    #[error(transparent)]
    NatsStore(#[from] proven_store_nats::Error),

    #[cfg(target_os = "linux")]
    #[error(transparent)]
    Netlink(#[from] rtnetlink::Error),

    #[error("No loopback interface found")]
    NoLoopback,

    #[error(transparent)]
    Nsm(#[from] proven_attestation_nsm::Error),

    #[error(transparent)]
    Postgres(#[from] proven_postgres::Error),

    #[error("Failed to set up route")]
    RouteSetup,

    #[error(transparent)]
    S3Store(#[from] proven_store_s3::Error),

    #[error(transparent)]
    VsockProxy(#[from] proven_vsock_proxy::Error),

    #[error(transparent)]
    VsockRpc(#[from] proven_vsock_rpc::Error),

    #[error(transparent)]
    VsockTracing(#[from] proven_vsock_tracing::Error),
}
