use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to parse socket address.
    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),

    /// Proxy already started.
    #[error("dnscrypt-proxy already started")]
    AlreadyStarted,

    /// Failed to write config file.
    #[error("failed to write nats config: {0}")]
    ConfigWrite(#[from] std::io::Error),

    /// Failed to encode DNS stamp.
    #[error(transparent)]
    DnsStampEncode(#[from] dns_stamp_parser::EncodeError),

    /// Process exited with non-zero.
    #[error("dnscrypt-proxy exited with non-zero: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    /// Failed to parse dnscrypt-proxy output.
    #[error("failed to parse dnscrypt-proxy output")]
    OutputParse,

    /// Failed to parse regex pattern.
    #[error(transparent)]
    RegexParse(#[from] regex::Error),

    /// Failed to find local DOH resolver endpoint in Route53.
    #[error("failed to find local DoH resolver endpoint in Route53")]
    ResolverEndpointNotFound,

    /// Failed to resolve Route53 resolver endpoint.
    #[error(transparent)]
    Route53(#[from] aws_sdk_route53resolver::Error),

    /// Failed to send signal to dnscrypt-proxy.
    #[error(transparent)]
    Signal(#[from] nix::Error),

    /// Failed to spawn dnscrypt-proxy.
    #[error(transparent)]
    Spawn(std::io::Error),
}
