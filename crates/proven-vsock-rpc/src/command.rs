use std::net::Ipv4Addr;

use cidr::Ipv4Cidr;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitializeArgs {
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
    pub sessions_bucket: String,
    pub stokenet: bool,
    pub tun_device: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Command {
    Initialize(InitializeArgs),
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initialize_round_trip() {
        let command = Command::Initialize(InitializeArgs {
            cidr: Ipv4Cidr::new(Ipv4Addr::new(192, 168, 0, 0), 16).unwrap(),
            email: vec![String::from("test@example.com")],
            fqdn: String::from("example.com"),
            host_dns_resolv: String::from("nameserver 192.168.0.254"),
            host_ip: Ipv4Addr::new(192, 168, 0, 1),
            https_port: 443,
            enclave_ip: Ipv4Addr::new(192, 168, 0, 2),
            log_port: 1235,
            nats_port: 4222,
            proxy_port: 1236,
            sessions_bucket: String::from("sessions_bucket"),
            stokenet: true,
            tun_device: String::from("tun0"),
        });
        let encoded = serde_cbor::to_vec(&command).unwrap();
        let command2: Command = serde_cbor::from_slice(&encoded).unwrap();

        assert_eq!(command, command2);
    }

    #[test]
    fn test_shutdown_round_trip() {
        let command = Command::Shutdown;
        let encoded = serde_cbor::to_vec(&command).unwrap();
        let command2: Command = serde_cbor::from_slice(&encoded).unwrap();

        assert_eq!(command, command2);
    }
}
