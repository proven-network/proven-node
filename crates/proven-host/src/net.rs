use crate::error::Result;

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
        .await?;

    Command::new("iptables")
        .arg("-A")
        .arg("FORWARD")
        .arg("-s")
        .arg(cidr.to_string())
        .arg("-j")
        .arg("ACCEPT")
        .output()
        .await?;

    Command::new("iptables")
        .arg("-A")
        .arg("FORWARD")
        .arg("-d")
        .arg(cidr.to_string())
        .arg("-j")
        .arg("ACCEPT")
        .output()
        .await?;

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
        .await?;

    info!("route to enclave created");

    Ok(())
}

pub async fn configure_tcp_forwarding(
    ip: Ipv4Addr,
    enclave_ip: Ipv4Addr,
    outbound_device: &str,
) -> Result<()> {
    // HTTPS, NFS, Babylon gossip, respectively
    for port in [443, 2049, 30000] {
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
            .await?;

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
            .await?;
    }

    info!("tcp forwarding to enclave created");

    Ok(())
}
