use crate::error::Result;

use std::net::Ipv4Addr;

use cidr::Ipv4Cidr;

#[cfg(not(target_os = "linux"))]
pub use macosx::*;

#[cfg(not(target_os = "linux"))]
pub mod macosx {
    use super::*;
    // No-ops on macOS

    pub async fn bring_up_loopback() -> Result<()> {
        Ok(())
    }

    pub async fn setup_default_gateway(
        _tun_device: &str,
        _host_ip: Ipv4Addr,
        _cidr: Ipv4Cidr,
    ) -> Result<()> {
        Ok(())
    }

    pub fn write_dns_resolv(_contents: String) -> Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(target_os = "linux")]
pub mod linux {
    use super::*;

    use crate::error::Error;

    use futures::{stream::TryStreamExt, TryFutureExt};
    use rtnetlink::LinkUnspec;
    use tokio::process::Command;
    use tracing::{error, info};

    pub async fn bring_up_loopback() -> Result<()> {
        let (conn, handle, _receiver) = rtnetlink::new_connection()?;

        let conn_task = tokio::spawn(conn);

        let mut links = handle.link().get().match_name("lo".to_string()).execute();
        if let Some(link) = links.try_next().map_err(|_| Error::NoLoopback).await? {
            handle
                .link()
                .set(LinkUnspec::new_with_index(link.header.index).up().build())
                .execute()
                .await?
        }

        conn_task.abort();
        _ = conn_task.await;

        Ok(())
    }

    pub async fn setup_default_gateway(
        tun_device: &str,
        host_ip: Ipv4Addr,
        cidr: Ipv4Cidr,
    ) -> Result<()> {
        let cmd = Command::new("ip")
            .arg("route")
            .arg("add")
            .arg("default")
            .arg("via")
            .arg(host_ip.to_string())
            .arg("dev")
            .arg(tun_device)
            .output()
            .await?;

        if !cmd.status.success() {
            error!("failed to setup default gateway");
            error!("stdout: {}", String::from_utf8_lossy(&cmd.stdout));
            error!("stderr: {}", String::from_utf8_lossy(&cmd.stderr));

            return Err(Error::RouteSetup);
        }

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

        if !cmd.status.success() {
            error!("failed to setup default gateway");
            error!("stdout: {}", String::from_utf8_lossy(&cmd.stdout));
            error!("stderr: {}", String::from_utf8_lossy(&cmd.stderr));

            return Err(Error::RouteSetup);
        }

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
}
