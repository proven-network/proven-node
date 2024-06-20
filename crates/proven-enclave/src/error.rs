use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Core(proven_core::Error),

    #[from]
    Custom(String),

    #[from]
    AddrParse(std::net::AddrParseError),

    #[from]
    AsmStore(proven_store_asm::Error),

    #[from]
    Async(tokio::task::JoinError),

    #[from]
    Cidr(cidr::errors::NetworkParseError),

    #[from]
    DnscryptProxy(proven_dnscrypt_proxy::Error),

    #[from]
    Imds(proven_imds::Error),

    #[from]
    InstanceDetails(proven_instance_details::Error),

    #[from]
    Io(std::io::Error),

    #[from]
    Kms(proven_kms::Error),

    #[from]
    NatsServer(proven_nats_server::Error),

    #[from]
    Netlink(rtnetlink::Error),

    #[from]
    Nsm(proven_attestation_nsm::Error),

    #[from]
    S3Store(proven_store_s3_sse_c::Error),

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
            Error::Core(e) => write!(f, "{}", e),
            Error::Custom(e) => write!(f, "{}", e),
            Error::AddrParse(e) => write!(f, "{}", e),
            Error::AsmStore(e) => write!(f, "{}", e),
            Error::Async(e) => write!(f, "{}", e),
            Error::Cidr(e) => write!(f, "{}", e),
            Error::DnscryptProxy(e) => write!(f, "{}", e),
            Error::Imds(e) => write!(f, "{}", e),
            Error::InstanceDetails(e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
            Error::Kms(e) => write!(f, "{}", e),
            Error::NatsServer(e) => write!(f, "{}", e),
            Error::Netlink(e) => write!(f, "{}", e),
            Error::Nsm(e) => write!(f, "{}", e),
            Error::S3Store(e) => write!(f, "{}", e),
            Error::VsockProxy(e) => write!(f, "{}", e),
            Error::VsockRpc(e) => write!(f, "{}", e),
            Error::VsockTracing(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
