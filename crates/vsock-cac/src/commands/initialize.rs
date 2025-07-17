//! Initialize command implementation.

use cidr::Ipv4Cidr;
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;

/// Request to initialize the enclave.
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

    /// S3 bucket for file-systems (encrypted/decrypted inside enclave).
    pub file_systems_bucket: String,

    /// DNS resolv.conf from host (used temporarily until DOH starts).
    pub host_dns_resolv: String,

    /// Host IP address on virtual network between enclave and host.
    pub host_ip: Ipv4Addr,

    /// HTTPS port (should match host TCP forwarding).
    pub https_port: u16,

    /// KMS key ID for encrypting/decrypting cipher-text.
    pub kms_key_id: String,

    /// Maximum number of concurrent runtime workers.
    pub max_runtime_workers: u32,

    /// NATS cluster port for messaging between enclaves (should match host TCP forwarding).
    pub nats_cluster_port: u16,

    /// Private key for the node.
    pub node_key: String,

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

    /// S3 bucket for SQL snapshots (encrypted/decrypted inside enclave).
    pub sql_snapshots_bucket: String,

    /// The port to listen on for the radix mainnet node.
    pub radix_mainnet_port: u16,

    /// The port to listen on for the radix stokenet node.
    pub radix_stokenet_port: u16,

    /// Whether this network is testnet.
    pub testnet: bool,
}

/// Response to initialization request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitializeResponse {
    /// Whether the initialization was successful.
    pub success: bool,
    /// Optional error message if initialization failed.
    pub error: Option<String>,
}
