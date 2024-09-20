use std::net::Ipv4Addr;

use cidr::Ipv4Cidr;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitializeRequest {
    pub certificates_bucket: String,
    pub cidr: Ipv4Cidr,
    pub email: Vec<String>,
    pub enclave_ip: Ipv4Addr,
    pub fqdn: String,
    pub host_dns_resolv: String,
    pub host_ip: Ipv4Addr,
    pub https_port: u16,
    pub log_port: u32,
    pub nats_port: u16,
    pub proxy_port: u32,
    pub skip_fsck: bool,
    pub skip_speedtest: bool,
    pub stokenet: bool,
    pub tun_device: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitializeResponse {
    pub success: bool,
}
