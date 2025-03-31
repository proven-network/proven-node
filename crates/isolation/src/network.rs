//! Network namespace and port forwarding functionality
//! - Network namespace setup and cleanup
//! - Port forwarding configuration
//! - IP configuration for isolated environments

use std::net::{IpAddr, Ipv4Addr};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tracing::debug;

use crate::error::Error;
use crate::error::Result;

/// Counter for generating unique IP addresses
static IP_COUNTER: AtomicU32 = AtomicU32::new(2); // Start at 2 since 1 is reserved for host

/// A virtual ethernet pair for network communication between host and container
#[derive(Debug)]
pub struct VethPair {
    /// The name of the host end of the veth pair
    pub host_name: String,
    /// The name of the container end of the veth pair
    pub container_name: String,
    /// The IP address of the host end
    pub host_ip: IpAddr,
    /// The IP address of the container end
    pub container_ip: IpAddr,
    /// The PID of the container process
    pub container_pid: u32,
}

impl VethPair {
    /// Create a new veth pair
    pub fn new(container_pid: u32) -> Self {
        let counter = IP_COUNTER.fetch_add(1, Ordering::SeqCst);
        let host_name = format!("veth{}", counter);
        let container_name = format!("veth{}", counter + 1);

        let host_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let container_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, counter as u8));

        Self {
            host_name,
            container_name,
            host_ip,
            container_ip,
            container_pid,
        }
    }

    /// Get the container's IP address (as reachable from the host)
    pub fn container_ip(&self) -> IpAddr {
        self.container_ip
    }

    /// Get the host's IP address (as reachable from the container)
    pub fn host_ip(&self) -> IpAddr {
        self.host_ip
    }

    /// Create a new veth pair for an isolated process
    pub async fn create_for_pid(pid: u32) -> Result<Self> {
        let veth = Self::new(pid);

        // Clean up any existing interfaces
        Self::cleanup_existing(&veth.host_name).await?;

        // Create /var/run/netns directory if it doesn't exist
        let output = Command::new("mkdir")
            .args(&["-p", "/var/run/netns"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to create netns directory: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to create netns directory: {}",
                stderr
            )));
        }

        // Create symlink to the network namespace
        let ns_path = format!("/proc/{}/ns/net", pid);
        let ns_name = format!("container_{}", pid);
        let ns_link = format!("/var/run/netns/{}", ns_name);

        let output = Command::new("ln")
            .args(&["-sfT", &ns_path, &ns_link])
            .output()
            .map_err(|e| Error::Network(format!("Failed to create netns symlink: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to create netns symlink: {}",
                stderr
            )));
        }

        // Create veth pair
        let output = Command::new("ip")
            .args(&[
                "link",
                "add",
                &veth.host_name,
                "type",
                "veth",
                "peer",
                "name",
                &veth.container_name,
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to create veth pair: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to create veth pair: {}",
                stderr
            )));
        }

        // Wait for network namespace to be ready
        let mut retries = 0;
        let max_retries = 10;
        let retry_delay = Duration::from_millis(100);

        while retries < max_retries {
            let ns_path = format!("/proc/{}/ns/net", pid);
            if std::path::Path::new(&ns_path).exists() {
                // Verify network namespace is different from host
                let output = Command::new("readlink")
                    .args(&["-f", &ns_path])
                    .output()
                    .map_err(|e| Error::Network(format!("Failed to read namespace link: {}", e)))?;

                let container_ns = String::from_utf8_lossy(&output.stdout);

                let output = Command::new("readlink")
                    .args(&["-f", "/proc/1/ns/net"])
                    .output()
                    .map_err(|e| {
                        Error::Network(format!("Failed to read host namespace link: {}", e))
                    })?;

                let host_ns = String::from_utf8_lossy(&output.stdout);

                if container_ns != host_ns {
                    debug!("Container network namespace: {}", container_ns.trim());
                    debug!("Host network namespace: {}", host_ns.trim());
                    break;
                }
            }

            tokio::time::sleep(retry_delay).await;
            retries += 1;
        }

        if retries == max_retries {
            return Err(Error::Network("Network namespace not ready".to_string()));
        }

        // Move container end to namespace
        let output = Command::new("ip")
            .args(&["link", "set", &veth.container_name, "netns", &ns_name])
            .output()
            .map_err(|e| Error::Network(format!("Failed to move veth to namespace: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to move veth to namespace: {}",
                stderr
            )));
        }

        // Set up container network
        veth.setup_container_network(pid).await?;

        // Set up host network
        veth.setup_host_network().await?;

        Ok(veth)
    }

    /// Clean up any existing interfaces with the same name
    ///
    /// # Errors
    ///
    /// Returns an error if the cleanup fails critically
    async fn cleanup_existing(host_name: &str) -> Result<()> {
        // List existing interfaces
        let output = Command::new("ip")
            .args(&["link", "show"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to list interfaces: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to list interfaces: {}",
                stderr
            )));
        }

        // List network namespaces
        let output = Command::new("ip")
            .args(&["netns", "list"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to list network namespaces: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to list network namespaces: {}",
                stderr
            )));
        }

        // Try to delete the interface from the host
        let output = Command::new("ip")
            .args(&["link", "delete", host_name])
            .output()
            .map_err(|e| Error::Network(format!("Failed to delete interface: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("Failed to delete interface from host: {}", stderr);
        }

        // Try to delete any existing container network namespaces
        let netns_dir = "/var/run/netns";
        if let Ok(entries) = std::fs::read_dir(netns_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.starts_with("container_") {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Set up the container side of the veth pair
    ///
    /// # Errors
    ///
    /// Returns an error if the setup fails
    async fn setup_container_network(&self, pid: u32) -> Result<()> {
        let ns_path = format!("/proc/{}/ns/net", pid);
        let ns_name = format!("container_{}", pid);
        let ns_link = format!("/var/run/netns/{}", ns_name);

        debug!(
            "Setting up container network for PID {} in namespace {}",
            pid, ns_name
        );

        // Create /var/run/netns directory if it doesn't exist
        let output = Command::new("mkdir")
            .args(&["-p", "/var/run/netns"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to create netns directory: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to create netns directory: {}",
                stderr
            )));
        }

        // Create symlink to the network namespace
        let output = Command::new("ln")
            .args(&["-sfT", &ns_path, &ns_link])
            .output()
            .map_err(|e| Error::Network(format!("Failed to create netns symlink: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to create netns symlink: {}",
                stderr
            )));
        }

        debug!("Created network namespace symlink at {}", ns_link);

        // Verify network namespace exists
        let output = Command::new("ip")
            .args(&["netns", "list"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to list network namespaces: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to list network namespaces: {}",
                stderr
            )));
        }

        let namespaces = String::from_utf8_lossy(&output.stdout);
        debug!("Available network namespaces: {}", namespaces);

        // Set container IP
        let output = Command::new("ip")
            .args(&[
                "netns",
                "exec",
                &ns_name,
                "ip",
                "addr",
                "add",
                &format!("{}/24", self.container_ip),
                "dev",
                &self.container_name,
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to set container IP: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to set container IP: {}",
                stderr
            )));
        }

        debug!("Set container IP to {}", self.container_ip);

        // Verify container IP was set correctly
        let output = Command::new("ip")
            .args(&[
                "netns",
                "exec",
                &ns_name,
                "ip",
                "addr",
                "show",
                &self.container_name,
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to verify container IP: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to verify container IP: {}",
                stderr
            )));
        }

        let interface_info = String::from_utf8_lossy(&output.stdout);
        debug!("Container interface info: {}", interface_info);

        // Bring up container interface
        let output = Command::new("ip")
            .args(&[
                "netns",
                "exec",
                &ns_name,
                "ip",
                "link",
                "set",
                &self.container_name,
                "up",
            ])
            .output()
            .map_err(|e| {
                Error::Network(format!("Failed to bring up container interface: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to bring up container interface: {}",
                stderr
            )));
        }

        debug!("Brought up container interface {}", self.container_name);

        // Set default route
        let output = Command::new("ip")
            .args(&[
                "netns",
                "exec",
                &ns_name,
                "ip",
                "route",
                "add",
                "default",
                "via",
                &self.host_ip.to_string(),
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to set default route: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to set default route: {}",
                stderr
            )));
        }

        debug!("Set default route via {}", self.host_ip);

        // Verify routing table
        let output = Command::new("ip")
            .args(&["netns", "exec", &ns_name, "ip", "route", "show"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to show routing table: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to show routing table: {}",
                stderr
            )));
        }

        let routes = String::from_utf8_lossy(&output.stdout);
        debug!("Container routing table: {}", routes);

        Ok(())
    }

    /// Set up the host side of the veth pair
    ///
    /// # Errors
    ///
    /// Returns an error if the setup fails
    async fn setup_host_network(&self) -> Result<()> {
        debug!("Setting up host network for interface {}", self.host_name);

        // Set host IP
        let output = Command::new("ip")
            .args(&[
                "addr",
                "add",
                &format!("{}/24", self.host_ip),
                "dev",
                &self.host_name,
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to set host IP: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!("Failed to set host IP: {}", stderr)));
        }

        debug!("Set host IP to {}", self.host_ip);

        // Verify host IP was set correctly
        let output = Command::new("ip")
            .args(&["addr", "show", &self.host_name])
            .output()
            .map_err(|e| Error::Network(format!("Failed to verify host IP: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to verify host IP: {}",
                stderr
            )));
        }

        let interface_info = String::from_utf8_lossy(&output.stdout);
        debug!("Host interface info: {}", interface_info);

        // Bring up host interface
        let output = Command::new("ip")
            .args(&["link", "set", &self.host_name, "up"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to bring up host interface: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to bring up host interface: {}",
                stderr
            )));
        }

        debug!("Brought up host interface {}", self.host_name);

        // Enable IPv4 forwarding
        let output = Command::new("sysctl")
            .args(&["-w", "net.ipv4.ip_forward=1"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to enable IPv4 forwarding: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to enable IPv4 forwarding: {}",
                stderr
            )));
        }

        debug!("Enabled IPv4 forwarding");

        // Verify IPv4 forwarding is enabled
        let output = Command::new("sysctl")
            .args(&["net.ipv4.ip_forward"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to verify IPv4 forwarding: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to verify IPv4 forwarding: {}",
                stderr
            )));
        }

        let forwarding_status = String::from_utf8_lossy(&output.stdout);
        debug!("IPv4 forwarding status: {}", forwarding_status);

        // Set up NAT for container network
        let output = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-A",
                "POSTROUTING",
                "-s",
                "10.0.0.0/24",
                "-j",
                "MASQUERADE",
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to set up NAT: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!("Failed to set up NAT: {}", stderr)));
        }

        debug!("Set up NAT for container network");

        // Set up port forwarding from host to container
        let output = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-A",
                "PREROUTING",
                "-p",
                "tcp",
                "-i",
                "eth0",
                "--dport",
                "8080",
                "-j",
                "DNAT",
                "--to-destination",
                &format!("{}:8080", self.container_ip),
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to set up port forwarding: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to set up port forwarding: {}",
                stderr
            )));
        }

        debug!(
            "Set up port forwarding from eth0:8080 to {}:8080",
            self.container_ip
        );

        // Add OUTPUT rule for localhost access
        let output = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-A",
                "OUTPUT",
                "-p",
                "tcp",
                "-d",
                "127.0.0.1",
                "--dport",
                "8080",
                "-j",
                "DNAT",
                "--to-destination",
                &format!("{}:8080", self.container_ip),
            ])
            .output()
            .map_err(|e| Error::Network(format!("Failed to set up localhost forwarding: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to set up localhost forwarding: {}",
                stderr
            )));
        }

        debug!("Set up localhost forwarding to {}:8080", self.container_ip);

        // Verify iptables rules
        let output = Command::new("iptables")
            .args(&["-t", "nat", "-L", "-n", "-v"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to list iptables rules: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to list iptables rules: {}",
                stderr
            )));
        }

        let rules = String::from_utf8_lossy(&output.stdout);
        debug!("Current iptables NAT rules:\n{}", rules);

        Ok(())
    }

    /// Clean up the veth pair
    ///
    /// # Errors
    ///
    /// Returns an error if the cleanup fails
    pub async fn cleanup(&self) -> Result<()> {
        debug!("Cleaning up veth pair: {}", self.host_name);

        // Remove port forwarding rules
        let _ = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-D",
                "PREROUTING",
                "-p",
                "tcp",
                "-i",
                "eth0",
                "--dport",
                "8080",
                "-j",
                "DNAT",
                "--to-destination",
                &format!("{}:8080", self.container_ip),
            ])
            .output();

        // Remove localhost forwarding rule
        let _ = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-D",
                "OUTPUT",
                "-p",
                "tcp",
                "-d",
                "127.0.0.1",
                "--dport",
                "8080",
                "-j",
                "DNAT",
                "--to-destination",
                &format!("{}:8080", self.container_ip),
            ])
            .output();

        // Remove NAT rules
        let _ = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-D",
                "POSTROUTING",
                "-s",
                "10.0.0.0/24",
                "-j",
                "MASQUERADE",
            ])
            .output();

        // Delete the veth interface - doing this will automatically
        // remove the other end of the pair as well
        debug!("Deleting veth interface");
        let output = Command::new("ip")
            .args(&["link", "delete", &self.host_name])
            .output()
            .map_err(|e| Error::Network(format!("Failed to delete veth interface: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("Failed to delete veth interface: {}", stderr);
        }

        // Clean up network namespace
        let ns_name = format!("container_{}", self.container_pid);
        debug!("Removing network namespace: {}", ns_name);

        // First try to delete the namespace
        let output = Command::new("ip")
            .args(&["netns", "delete", &ns_name])
            .output()
            .map_err(|e| Error::Network(format!("Failed to delete network namespace: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("Failed to delete network namespace: {}", stderr);
        }

        // Then remove the symlink if it still exists
        let ns_link = format!("/var/run/netns/{}", ns_name);
        if std::path::Path::new(&ns_link).exists() {
            debug!("Removing network namespace symlink: {}", ns_link);
            if let Err(e) = std::fs::remove_file(&ns_link) {
                debug!("Failed to remove network namespace symlink: {}", e);
            }
        }

        debug!("Veth pair cleanup complete");
        Ok(())
    }
}

/// Test if the calling process has root permissions
///
/// # Errors
///
/// Returns an error if the permissions cannot be determined
pub fn check_root_permissions() -> Result<bool> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|e| Error::Io("Failed to execute id command", e))?;

    let uid = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u32>()
        .map_err(|e| Error::Application(format!("Failed to parse uid: {}", e)))?;

    Ok(uid == 0)
}
