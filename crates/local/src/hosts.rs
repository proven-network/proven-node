use crate::error::{Error, Result};

use std::fs::read_to_string;
use std::path::Path;

use console::style;
use hickory_resolver::Resolver;
use tracing::{error, info};

/// Checks if a hostname can be resolved via DNS or hosts file.
/// Exits the process if the hostname cannot be resolved.
pub async fn check_hostname_resolution(hostname: &str) -> Result<()> {
    // First try DNS resolution using Hickory resolver
    let resolver = match Resolver::tokio_from_system_conf() {
        Ok(resolver) => resolver,
        Err(e) => {
            error!("Failed to create DNS resolver: {}", e);
            return Err(Error::Io(format!("Failed to create DNS resolver: {}", e)));
        }
    };
    let dns_lookup_result = resolver.lookup_ip(hostname).await;

    if let Ok(lookup_result) = dns_lookup_result {
        if !lookup_result.iter().collect::<Vec<_>>().is_empty() {
            info!(
                "Hostname {} can be resolved via DNS: {:?}",
                hostname,
                lookup_result.iter().collect::<Vec<_>>()
            );
        } else {
            error!("DNS resolution for {} returned no addresses", hostname);
            if !check_hosts_file(hostname) {
                error!("{} is not configured in hosts file or DNS", hostname);
                show_hosts_file_instructions(hostname);
                std::process::exit(1);
            }
        }
    } else {
        error!(
            "Could not resolve {} via DNS: {:?}",
            hostname,
            dns_lookup_result.err()
        );
        if !check_hosts_file(hostname) {
            error!("{} is not configured in hosts file either", hostname);
            show_hosts_file_instructions(hostname);
            std::process::exit(1);
        }
    }

    Ok(())
}

fn check_hosts_file(hostname: &str) -> bool {
    #[cfg(target_family = "unix")]
    {
        let hosts_path = Path::new("/etc/hosts");
        check_host_entry(hosts_path, hostname)
    }

    #[cfg(target_family = "windows")]
    {
        let hosts_path = Path::new(r"C:\Windows\System32\drivers\etc\hosts");
        check_host_entry(hosts_path, hostname)
    }
}

fn check_host_entry(hosts_path: &Path, hostname: &str) -> bool {
    read_to_string(hosts_path).map_or(false, |contents| {
        contents.lines().any(|line| {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() {
                return false;
            }
            line.split_whitespace()
                .any(|part| part == "127.0.0.1" || part == "::1")
                && line.contains(hostname)
        })
    })
}

/// Shows instructions for adding the hostname to the hosts file.
fn show_hosts_file_instructions(hostname: &str) {
    #[cfg(target_family = "unix")]
    error!(
        "Please add {} to {} or configure DNS properly",
        style(format!("0.0.0.0 {}", hostname)).cyan(),
        style("/etc/hosts").blue(),
    );
    #[cfg(target_family = "windows")]
    error!(
        "Please add {} to {} or configure DNS properly",
        style(format!("0.0.0.0 {}", hostname)).cyan(),
        style(r"C:\Windows\System32\drivers\etc\hosts").blue()
    );
}
