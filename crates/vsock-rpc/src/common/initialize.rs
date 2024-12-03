use std::net::Ipv4Addr;

use cidr::Ipv4Cidr;
use serde::{Deserialize, Serialize};

/// A request to add a new peer to the server.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitializeRequest {
    /// S3 bucket for certificates (encrypted/decrypted inside enclave).
    pub certificates_bucket: String,

    /// CIDR for virtual network between enclave and host.
    pub cidr: Ipv4Cidr,

    /// Email addresses (used for Let's Encrypt).
    pub email: Vec<String>,

    /// Enclave IP address on virtual network between enclave and host.
    pub enclave_ip: Ipv4Addr,

    /// Top-level domain for network connectivity. IE proven.network.
    pub fqdn: String,

    /// DNS resolv.conf from host (used temporarily until DOH starta).
    pub host_dns_resolv: String,

    /// Host IP address on virtual network between enclave and host.
    pub host_ip: Ipv4Addr,

    /// HTTPS port (should match host TCP forwarding).
    pub https_port: u16,

    /// KMS key ID for encrypting/decrypting cipher-text.
    pub kms_key_id: String,

    /// Maximum number of concurrent runtime workers.
    pub max_runtime_workers: u32,

    /// NATS port for messaging between enclaves (should match host TCP forwarding).
    pub nats_port: u16,

    /// NFS mount point for external file-systems.
    pub nfs_mount_point: String,

    /// VSOCK port for proxying layer-3 traffic.
    pub proxy_port: u32,

    /// Should skip gocryptfs integrity checks.
    pub skip_fsck: bool,

    /// Should skip speed-test on boot.
    pub skip_speedtest: bool,

    /// Should skip vacuuming postgres database.
    pub skip_vacuum: bool,

    /// Whether to use stokenet - or mainnet.
    pub stokenet: bool,
}

/// The response to an add peer request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitializeResponse {
    /// Whether the server successfully initialized.
    pub success: bool,
}
