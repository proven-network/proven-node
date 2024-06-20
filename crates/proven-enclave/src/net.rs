use crate::error::Result;

use cidr::Ipv4Cidr;
use rtnetlink::LinkHandle;
use std::net::Ipv4Addr;
use tokio::process::Command;
use tracing::info;

pub async fn bring_up_loopback() -> Result<()> {
    let (conn, handle, _receiver) = rtnetlink::new_connection()?;

    let conn_task = tokio::spawn(conn);

    let result = LinkHandle::new(handle).set(1).up().execute().await;

    conn_task.abort();
    _ = conn_task.await;

    Ok(result?)
}

pub async fn setup_default_gateway(
    tun_device: &str,
    host_ip: Ipv4Addr,
    cidr: Ipv4Cidr,
) -> Result<()> {
    Command::new("ip")
        .arg("route")
        .arg("add")
        .arg("default")
        .arg("via")
        .arg(host_ip.to_string())
        .arg("dev")
        .arg(tun_device)
        .output()
        .await?;

    Command::new("ip")
        .arg("route")
        .arg("add")
        .arg(format!(
            "{}/{}",
            cidr.first_address(),
            cidr.network_length()
        ))
        .arg("dev")
        .arg(tun_device)
        .output()
        .await?;

    info!("default gateway to host created");

    Ok(())
}

pub fn write_dns_resolv(contents: String) -> Result<()> {
    info!("writing resolv.conf");

    std::fs::create_dir_all("/run/resolvconf")?;
    let mut resolv = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open("/run/resolvconf/resolv.conf")?;

    Ok(std::io::Write::write_all(&mut resolv, contents.as_bytes())?)
}
