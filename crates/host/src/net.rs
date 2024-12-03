use crate::{Error, Result};

use std::net::Ipv4Addr;

use cidr::Ipv4Cidr;
use tokio::process::Command;
use tracing::info;

pub async fn configure_nat(outbound_device: &str, cidr: Ipv4Cidr) -> Result<()> {
    Command::new("iptables")
        .arg("-t")
        .arg("nat")
        .arg("-A")
        .arg("POSTROUTING")
        .arg("-s")
        .arg(cidr.to_string())
        .arg("-o")
        .arg(outbound_device)
        .arg("-j")
        .arg("MASQUERADE")
        .output()
        .await
        .map_err(|e| Error::Io("failed to configure nat", e))?;

    Command::new("iptables")
        .arg("-A")
        .arg("FORWARD")
        .arg("-s")
        .arg(cidr.to_string())
        .arg("-j")
        .arg("ACCEPT")
        .output()
        .await
        .map_err(|e| Error::Io("failed to configure nat", e))?;

    Command::new("iptables")
        .arg("-A")
        .arg("FORWARD")
        .arg("-d")
        .arg(cidr.to_string())
        .arg("-j")
        .arg("ACCEPT")
        .output()
        .await
        .map_err(|e| Error::Io("failed to configure nat", e))?;

    info!("nat configured");

    Ok(())
}

pub async fn configure_route(tun_device: &str, cidr: Ipv4Cidr, enclave_ip: Ipv4Addr) -> Result<()> {
    Command::new("ip")
        .arg("route")
        .arg("add")
        .arg(format!("{}/{}", enclave_ip, cidr.network_length()))
        .arg("dev")
        .arg(tun_device)
        .output()
        .await
        .map_err(|e| Error::Io("failed to configure route", e))?;

    info!("route to enclave created");

    Ok(())
}

pub async fn configure_port_forwarding(
    ip: Ipv4Addr,
    enclave_ip: Ipv4Addr,
    outbound_device: &str,
) -> Result<()> {
    // TCP ports
    for port in [
        111,   // RPC (NFS)
        443,   // HTTPS
        2049,  // NFS
        20001, // NFS
        20002, // NFS
        20003, // NFS
        30000, // Babylon (gossip)
    ] {
        Command::new("iptables")
            .arg("-t")
            .arg("nat")
            .arg("-A")
            .arg("PREROUTING")
            .arg("-i")
            .arg(outbound_device)
            .arg("-p")
            .arg("tcp")
            .arg("--dport")
            .arg(port.to_string())
            .arg("-j")
            .arg("DNAT")
            .arg("--to-destination")
            .arg(format!("{}:{}", enclave_ip, port).as_str())
            .output()
            .await
            .map_err(|e| Error::Io("failed to configure port forwarding", e))?;

        Command::new("iptables")
            .arg("-t")
            .arg("nat")
            .arg("-A")
            .arg("POSTROUTING")
            .arg("-p")
            .arg("tcp")
            .arg("-d")
            .arg(enclave_ip.to_string())
            .arg("--dport")
            .arg(port.to_string())
            .arg("-j")
            .arg("SNAT")
            .arg("--to-source")
            .arg(ip.to_string())
            .output()
            .await
            .map_err(|e| Error::Io("failed to configure port forwarding", e))?;
    }

    // UDP ports
    for port in [
        111,   // RPC (NFS)
        2049,  // NFS
        20001, // NFS
        20002, // NFS
        20003, // NFS
    ] {
        Command::new("iptables")
            .arg("-t")
            .arg("nat")
            .arg("-A")
            .arg("PREROUTING")
            .arg("-i")
            .arg(outbound_device)
            .arg("-p")
            .arg("udp")
            .arg("--dport")
            .arg(port.to_string())
            .arg("-j")
            .arg("DNAT")
            .arg("--to-destination")
            .arg(format!("{}:{}", enclave_ip, port).as_str())
            .output()
            .await
            .map_err(|e| Error::Io("failed to configure port forwarding", e))?;

        Command::new("iptables")
            .arg("-t")
            .arg("nat")
            .arg("-A")
            .arg("POSTROUTING")
            .arg("-p")
            .arg("udp")
            .arg("-d")
            .arg(enclave_ip.to_string())
            .arg("--dport")
            .arg(port.to_string())
            .arg("-j")
            .arg("SNAT")
            .arg("--to-source")
            .arg(ip.to_string())
            .output()
            .await
            .map_err(|e| Error::Io("failed to configure port forwarding", e))?;
    }

    info!("port forwarding to enclave created");

    Ok(())
}
