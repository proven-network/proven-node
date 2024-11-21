use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    AlreadyStarted,

    #[from]
    AddrParse(std::net::AddrParseError),

    #[from]
    AsmStore(proven_store_asm::Error),

    #[from]
    Async(tokio::task::JoinError),

    #[from]
    BabylonAggregator(proven_radix_aggregator::Error),

    #[from]
    BabylonGateway(proven_radix_gateway::Error),

    #[from]
    BabylonNode(proven_radix_node::Error),

    BadKey,

    #[from]
    Cidr(cidr::errors::NetworkParseError),

    #[from]
    Core(proven_core::Error),

    #[from]
    DnscryptProxy(proven_dnscrypt_proxy::Error),

    #[from]
    ExternalFs(proven_external_fs::Error),

    #[from]
    Imds(proven_imds::Error),

    #[from]
    InstanceDetails(proven_instance_details::Error),

    #[from]
    Io(std::io::Error),

    #[from]
    Kms(proven_kms::Error),

    #[from]
    NatsMonitor(proven_nats_monitor::Error),

    #[from]
    NatsServer(proven_nats_server::Error),

    #[from]
    NatsStore(proven_store_nats::Error),

    #[from]
    Netlink(rtnetlink::Error),

    NoLoopback,

    #[from]
    Nsm(proven_attestation_nsm::Error),

    #[from]
    Postgres(proven_postgres::Error),

    RouteSetup,

    #[from]
    S3Store(proven_store_s3::Error),

    #[from]
    VsockProxy(proven_vsock_proxy::Error),

    #[from]
    VsockRpc(proven_vsock_rpc::Error),

    #[from]
    VsockTracing(proven_vsock_tracing::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "The enclave has already been started"),
            Error::AddrParse(e) => write!(f, "{}", e),
            Error::AsmStore(e) => write!(f, "{}", e),
            Error::Async(e) => write!(f, "{}", e),
            Error::BabylonAggregator(e) => write!(f, "{}", e),
            Error::BabylonGateway(e) => write!(f, "{}", e),
            Error::BabylonNode(e) => write!(f, "{}", e),
            Error::BadKey => write!(f, "The key is invalid"),
            Error::Cidr(e) => write!(f, "{}", e),
            Error::Core(e) => write!(f, "{}", e),
            Error::DnscryptProxy(e) => write!(f, "{}", e),
            Error::ExternalFs(e) => write!(f, "{}", e),
            Error::Imds(e) => write!(f, "{}", e),
            Error::InstanceDetails(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
            Error::Kms(e) => write!(f, "{}", e),
            Error::NatsMonitor(e) => write!(f, "{}", e),
            Error::NatsServer(e) => write!(f, "{}", e),
            Error::NatsStore(e) => write!(f, "{}", e),
            Error::Netlink(e) => write!(f, "{}", e),
            Error::NoLoopback => write!(f, "No loopback interface found"),
            Error::Nsm(e) => write!(f, "{}", e),
            Error::Postgres(e) => write!(f, "{}", e),
            Error::RouteSetup => write!(f, "Failed to set up route"),
            Error::S3Store(e) => write!(f, "{}", e),
            Error::VsockProxy(e) => write!(f, "{}", e),
            Error::VsockRpc(e) => write!(f, "{}", e),
            Error::VsockTracing(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
