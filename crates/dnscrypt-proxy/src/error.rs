use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("dnscrypt-proxy already started")]
    AlreadyStarted,

    #[error("failed to write nats config: {0}")]
    ConfigWrite(#[from] std::io::Error),

    #[error("failed to parse dnscrypt-proxy output")]
    OutputParse,

    #[error("dnscrypt-proxy exited with non-zero: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    #[error("failed to find local DoH resolver endpoint in Route53")]
    ResolverEndpointNotFound,

    #[error("{0}")]
    Route53(#[from] aws_sdk_route53resolver::Error),

    #[error("failed to spawn dnscrypt-proxy: {0}")]
    Spawn(std::io::Error),
}
