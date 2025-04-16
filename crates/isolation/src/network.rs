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
    /// The IP address of the container end
    pub container_ip: IpAddr,

    /// The PID of the container process
    pub container_pid: u32,

    /// The name of the container end of the veth pair
    pub container_name: String,

    /// The IP address of the host end
    pub host_ip: IpAddr,

    /// The name of the host end of the veth pair
    pub host_name: String,

    /// TCP ports to forward from host to container
    pub tcp_port_forwards: Vec<u16>,

    /// UDP ports to forward from host to container
    pub udp_port_forwards: Vec<u16>,
}

impl VethPair {
    /// Create a new veth pair
    pub async fn new(
        pid: u32,
        tcp_port_forwards: Vec<u16>,
        udp_port_forwards: Vec<u16>,
    ) -> Result<Self> {
        let counter = IP_COUNTER.fetch_add(1, Ordering::SeqCst);
        let host_name = format!("veth{}", counter);
        let container_name = format!("veth{}", counter + 1);

        // Use a unique subnet for each veth pair to avoid conflicts
        // Each container gets its own /24 subnet: 10.0.{counter}.0/24
        let subnet_id = counter;
        let host_ip = IpAddr::V4(Ipv4Addr::new(10, 0, subnet_id as u8, 1));
        let container_ip = IpAddr::V4(Ipv4Addr::new(10, 0, subnet_id as u8, 2));

        let veth = Self {
            host_name,
            container_name,
            host_ip,
            container_ip,
            container_pid: pid,
            tcp_port_forwards,
            udp_port_forwards,
        };

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

    /// Get the container's IP address (as reachable from the host)
    pub fn container_ip(&self) -> IpAddr {
        self.container_ip
    }

    /// Get the host's IP address (as reachable from the container)
    pub fn host_ip(&self) -> IpAddr {
        self.host_ip
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

        // Bring up loopback interface in container
        let output = Command::new("ip")
            .args(&["netns", "exec", &ns_name, "ip", "link", "set", "lo", "up"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to bring up loopback interface: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to bring up loopback interface: {}",
                stderr
            )));
        }

        debug!("Brought up loopback interface in container");

        // Set up DNS forwarding
        self.setup_dns_forwarding()?;

        Ok(())
    }

    /// Set up DNS forwarding from container to the host's DNS servers
    ///
    /// # Errors
    ///
    /// Returns an error if setup fails
    fn setup_dns_forwarding(&self) -> Result<()> {
        debug!("Setting up DNS forwarding for container");

        // Read the host's resolv.conf to get nameservers
        let resolv_conf = match std::fs::read_to_string("/etc/resolv.conf") {
            Ok(content) => content,
            Err(e) => {
                debug!("Failed to read host's resolv.conf: {}", e);
                return Err(Error::Network(format!("Failed to configure DNS: {}", e)));
            }
        };

        // Extract nameserver IPs using regex
        let nameservers = resolv_conf
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                if line.starts_with("nameserver") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        return Some(parts[1].to_string());
                    }
                }
                None
            })
            .collect::<Vec<String>>();

        if nameservers.is_empty() {
            debug!("No nameservers found in host's resolv.conf");
            return Err(Error::Network(
                "No nameservers found in host's resolv.conf".to_string(),
            ));
        }

        debug!("Found nameservers: {:?}", nameservers);

        // Add iptables rules to forward DNS queries to each nameserver
        for nameserver in nameservers {
            // Forward UDP DNS queries (port 53)
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "PREROUTING",
                    "-s",
                    &self.container_ip.to_string(),
                    "-p",
                    "udp",
                    "--dport",
                    "53",
                    "-j",
                    "DNAT",
                    "--to",
                    &format!("{}:53", nameserver),
                ])
                .output()
                .map_err(|e| Error::Network(format!("Failed to set up DNS forwarding: {}", e)))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                debug!(
                    "Failed to set up UDP DNS forwarding to {}: {}",
                    nameserver, stderr
                );
                continue; // Try the next nameserver if this one fails
            }

            // Forward TCP DNS queries (less common but sometimes used)
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "PREROUTING",
                    "-s",
                    &self.container_ip.to_string(),
                    "-p",
                    "tcp",
                    "--dport",
                    "53",
                    "-j",
                    "DNAT",
                    "--to",
                    &format!("{}:53", nameserver),
                ])
                .output()
                .map_err(|e| {
                    Error::Network(format!("Failed to set up TCP DNS forwarding: {}", e))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                debug!(
                    "Failed to set up TCP DNS forwarding to {}: {}",
                    nameserver, stderr
                );
            } else {
                debug!("Set up DNS forwarding to {}", nameserver);
                return Ok(()); // Success with this nameserver, stop here
            }
        }

        debug!("Failed to set up DNS forwarding to any nameserver");

        Ok(()) // Return Ok even if we couldn't set up forwarding, to avoid breaking other functionality
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

        // Enable route_localnet for loopback and veth to handle NAT'd localhost traffic
        debug!("Enabling route_localnet");
        for iface in &["lo", &self.host_name] {
            let setting = format!("net.ipv4.conf.{}.route_localnet=1", iface);
            let output = Command::new("sysctl")
                .args(&["-w", &setting])
                .output()
                .map_err(|e| Error::Network(format!("Failed to set {}: {}", setting, e)))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set {}: {}",
                    setting, stderr
                )));
            }
        }

        // Extract subnet from the container IP
        let ip_string = self.container_ip.to_string();
        let subnet_parts: Vec<&str> = ip_string.split('.').collect();
        let container_subnet = format!(
            "{}.{}.{}.0/24",
            subnet_parts[0], subnet_parts[1], subnet_parts[2]
        );

        // Set up TCP port forwarding
        for port in &self.tcp_port_forwards {
            debug!("Setting up TCP rules for port {}", port);

            // --- NAT Table Rules --- //

            // 1. DNAT for external traffic (e.g., coming from eth0)
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "PREROUTING",
                    "-p",
                    "tcp",
                    "!",
                    "-i",
                    "lo", // Match any interface *except* loopback
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output()
                .map_err(|e| Error::Network(format!("Failed to set NAT PREROUTING rule: {}", e)))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set NAT PREROUTING rule: {}",
                    stderr
                )));
            }

            // 2. DNAT for localhost traffic
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "OUTPUT",
                    "-p",
                    "tcp",
                    "-o",
                    "lo", // Only match locally generated packets going to loopback
                    "-d",
                    "127.0.0.1/32",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output()
                .map_err(|e| Error::Network(format!("Failed to set NAT OUTPUT rule: {}", e)))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set NAT OUTPUT rule: {}",
                    stderr
                )));
            }

            // 3. SNAT for localhost -> container traffic
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "POSTROUTING",
                    "-p",
                    "tcp",
                    "-s",
                    "127.0.0.1/32", // Source is localhost
                    "-d",
                    &self.container_ip.to_string(), // Destination is container
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "SNAT",
                    "--to-source",
                    &self.host_ip.to_string(), // Change src to host veth IP
                ])
                .output()
                .map_err(|e| {
                    Error::Network(format!("Failed to set NAT POSTROUTING SNAT rule: {}", e))
                })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set NAT POSTROUTING SNAT rule: {}",
                    stderr
                )));
            }

            // --- Filter Table Rules (FORWARD Chain) --- //

            // 4. Allow NEW connections forwarding TO container (covers external and localhost)
            let output = Command::new("iptables")
                .args(&[
                    "-A",
                    "FORWARD",
                    "-d",
                    &self.container_ip.to_string(), // Destination is container
                    "-p",
                    "tcp",
                    "--dport",
                    &port.to_string(),
                    "-m",
                    "state",
                    "--state",
                    "NEW",
                    "-j",
                    "ACCEPT",
                ])
                .output()
                .map_err(|e| {
                    Error::Network(format!("Failed to set filter FORWARD NEW rule: {}", e))
                })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set filter FORWARD NEW rule: {}",
                    stderr
                )));
            }
        }

        // Set up UDP port forwarding
        for port in &self.udp_port_forwards {
            debug!("Setting up UDP rules for port {}", port);

            // --- NAT Table Rules --- //

            // 1. DNAT for external traffic
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "PREROUTING",
                    "-p",
                    "udp",
                    "!",
                    "-i",
                    "lo",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output()
                .map_err(|e| {
                    Error::Network(format!("Failed to set UDP NAT PREROUTING rule: {}", e))
                })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set UDP NAT PREROUTING rule: {}",
                    stderr
                )));
            }

            // 2. DNAT for localhost traffic
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "OUTPUT",
                    "-p",
                    "udp",
                    "-o",
                    "lo",
                    "-d",
                    "127.0.0.1/32",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output()
                .map_err(|e| Error::Network(format!("Failed to set UDP NAT OUTPUT rule: {}", e)))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set UDP NAT OUTPUT rule: {}",
                    stderr
                )));
            }

            // 3. SNAT for localhost -> container traffic
            let output = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-A",
                    "POSTROUTING",
                    "-p",
                    "udp",
                    "-s",
                    "127.0.0.1/32",
                    "-d",
                    &self.container_ip.to_string(),
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "SNAT",
                    "--to-source",
                    &self.host_ip.to_string(),
                ])
                .output()
                .map_err(|e| {
                    Error::Network(format!(
                        "Failed to set UDP NAT POSTROUTING SNAT rule: {}",
                        e
                    ))
                })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set UDP NAT POSTROUTING SNAT rule: {}",
                    stderr
                )));
            }

            // --- Filter Table Rules (FORWARD Chain) --- //
            // For basic UDP echo/request-reply, allowing forwarded packets based on destination is often enough.
            // No state matching needed typically.

            // 4. Allow forwarding TO container (covers external and localhost)
            let output = Command::new("iptables")
                .args(&[
                    "-A",
                    "FORWARD",
                    "-d",
                    &self.container_ip.to_string(),
                    "-p",
                    "udp",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "ACCEPT",
                ])
                .output()
                .map_err(|e| {
                    Error::Network(format!(
                        "Failed to set UDP filter FORWARD rule (to container): {}",
                        e
                    ))
                })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::Network(format!(
                    "Failed to set UDP filter FORWARD rule (to container): {}",
                    stderr
                )));
            }
        }

        // --- General NAT & Filter Rules (Not port specific) --- //

        // 5. MASQUERADE for container outbound traffic (POSTROUTING)
        debug!("Setting up MASQUERADE for container outbound traffic");
        let output = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-A",
                "POSTROUTING",
                "-s",
                &container_subnet,
                "!",
                "-d",
                &container_subnet, // Don't masquerade container-to-container traffic if applicable
                "-j",
                "MASQUERADE",
            ])
            .output()
            .map_err(|e| {
                Error::Network(format!(
                    "Failed to set NAT POSTROUTING MASQUERADE rule: {}",
                    e
                ))
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to set NAT POSTROUTING MASQUERADE rule: {}",
                stderr
            )));
        }

        // 6. Allow ESTABLISHED/RELATED traffic in FORWARD chain (Handles return traffic)
        debug!("Allowing ESTABLISHED/RELATED traffic in FORWARD chain");
        let output = Command::new("iptables")
            .args(&[
                "-A",
                "FORWARD",
                "-m",
                "state",
                "--state",
                "ESTABLISHED,RELATED",
                "-j",
                "ACCEPT",
            ])
            .output()
            .map_err(|e| {
                Error::Network(format!(
                    "Failed to set filter FORWARD ESTABLISHED rule: {}",
                    e
                ))
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to set filter FORWARD ESTABLISHED rule: {}",
                stderr
            )));
        }

        // Verify iptables rules
        debug!("Current iptables rules:");

        // Check NAT table rules
        let output = Command::new("iptables")
            .args(&["-t", "nat", "-L", "-n", "-v"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to list NAT rules: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to list NAT rules: {}",
                stderr
            )));
        }

        let rules = String::from_utf8_lossy(&output.stdout);
        debug!("NAT table rules:\n{}", rules);

        // Check default table rules
        let output = Command::new("iptables")
            .args(&["-L", "-n", "-v"])
            .output()
            .map_err(|e| Error::Network(format!("Failed to list filter rules: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Network(format!(
                "Failed to list filter rules: {}",
                stderr
            )));
        }

        let rules = String::from_utf8_lossy(&output.stdout);
        debug!("Filter table rules:\n{}", rules);

        Ok(())
    }

    /// Clean up the veth pair
    ///
    /// # Errors
    ///
    /// Returns an error if the cleanup fails
    fn cleanup(&self) -> Result<()> {
        debug!("Cleaning up veth pair: {}", self.host_name);

        // Clean up DNS forwarding rules
        self.cleanup_dns_forwarding();

        // Remove port forwarding rules for TCP ports
        for port in &self.tcp_port_forwards {
            debug!("Removing TCP rules for port {}", port);

            // 1. Remove NAT PREROUTING rule (External DNAT)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "PREROUTING",
                    "-p",
                    "tcp",
                    "!",
                    "-i",
                    "lo",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output();

            // 2. Remove NAT OUTPUT rule (Localhost DNAT)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "OUTPUT",
                    "-p",
                    "tcp",
                    "-o",
                    "lo",
                    "-d",
                    "127.0.0.1/32",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output();

            // 3. Remove NAT POSTROUTING rule (Localhost SNAT)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "POSTROUTING",
                    "-p",
                    "tcp",
                    "-s",
                    "127.0.0.1/32",
                    "-d",
                    &self.container_ip.to_string(),
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "SNAT",
                    "--to-source",
                    &self.host_ip.to_string(),
                ])
                .output();

            // 4. Remove Filter FORWARD NEW rule
            let _ = Command::new("iptables")
                .args(&[
                    "-D",
                    "FORWARD",
                    "-d",
                    &self.container_ip.to_string(),
                    "-p",
                    "tcp",
                    "--dport",
                    &port.to_string(),
                    "-m",
                    "state",
                    "--state",
                    "NEW",
                    "-j",
                    "ACCEPT",
                ])
                .output();
        }

        // Remove port forwarding rules for UDP ports
        for port in &self.udp_port_forwards {
            debug!("Removing UDP rules for port {}", port);

            // 1. Remove NAT PREROUTING rule (External DNAT)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "PREROUTING",
                    "-p",
                    "udp",
                    "!",
                    "-i",
                    "lo",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output();

            // 2. Remove NAT OUTPUT rule (Localhost DNAT)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "OUTPUT",
                    "-p",
                    "udp",
                    "-o",
                    "lo",
                    "-d",
                    "127.0.0.1/32",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "DNAT",
                    "--to-destination",
                    &format!("{}:{}", self.container_ip, port),
                ])
                .output();

            // 3. Remove NAT POSTROUTING rule (Localhost SNAT)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "POSTROUTING",
                    "-p",
                    "udp",
                    "-s",
                    "127.0.0.1/32",
                    "-d",
                    &self.container_ip.to_string(),
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "SNAT",
                    "--to-source",
                    &self.host_ip.to_string(),
                ])
                .output();

            // 4. Remove Filter FORWARD rule (to container)
            let _ = Command::new("iptables")
                .args(&[
                    "-D",
                    "FORWARD",
                    "-d",
                    &self.container_ip.to_string(),
                    "-p",
                    "udp",
                    "--dport",
                    &port.to_string(),
                    "-j",
                    "ACCEPT",
                ])
                .output();
        }

        // Extract subnet from the container IP
        let ip_string = self.container_ip.to_string();
        let subnet_parts: Vec<&str> = ip_string.split('.').collect();
        let container_subnet = format!(
            "{}.{}.{}.0/24",
            subnet_parts[0], subnet_parts[1], subnet_parts[2]
        );

        // Try to remove the MASQUERADE rule for this specific subnet
        let _ = Command::new("iptables")
            .args(&[
                "-t",
                "nat",
                "-D",
                "POSTROUTING",
                "-s",
                &container_subnet,
                "!",
                "-d",
                &container_subnet,
                "-j",
                "MASQUERADE",
            ])
            .output();

        // Delete the veth interface - doing this will automatically
        // remove the other end of the pair as well
        debug!("Deleting veth interface {}", self.host_name);
        let output = Command::new("ip")
            .args(&["link", "delete", &self.host_name])
            .output()
            .map_err(|e| Error::Network(format!("Failed to delete veth interface: {}", e)))?;

        if !output.status.success() {
            // Log error, but don't fail cleanup if interface is already gone
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!(
                "Failed to delete veth interface (might be already gone): {}",
                stderr
            );
        }

        // Clean up network namespace symlink
        let ns_name = format!("container_{}", self.container_pid);
        let ns_link = format!("/var/run/netns/{}", ns_name);
        if std::path::Path::new(&ns_link).exists() {
            debug!("Removing network namespace symlink: {}", ns_link);
            if let Err(e) = std::fs::remove_file(&ns_link) {
                // Log error, but don't fail the whole cleanup
                debug!(
                    "Failed to remove network namespace symlink {}: {}",
                    ns_link, e
                );
            }
        }
        // Note: We don't delete the actual netns (`ip netns del ...`) as it should be automatically
        // cleaned up when the process (container) exits and the symlink is removed.

        debug!("Veth pair cleanup for {} complete", self.host_name);
        Ok(())
    }

    /// Clean up DNS forwarding rules
    fn cleanup_dns_forwarding(&self) {
        debug!("Cleaning up DNS forwarding rules for container");

        // Read the host's resolv.conf to get nameservers
        let resolv_conf = match std::fs::read_to_string("/etc/resolv.conf") {
            Ok(content) => content,
            Err(e) => {
                debug!("Failed to read host's resolv.conf: {}", e);
                return;
            }
        };

        // Extract nameserver IPs using regex
        let nameservers = resolv_conf
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                if line.starts_with("nameserver") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        return Some(parts[1].to_string());
                    }
                }
                None
            })
            .collect::<Vec<String>>();

        if nameservers.is_empty() {
            debug!("No nameservers found in host's resolv.conf");
            return;
        }

        debug!("Found nameservers: {:?}", nameservers);

        // Remove iptables rules to forward DNS queries to each nameserver
        for nameserver in nameservers {
            // Forward UDP DNS queries (port 53)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "PREROUTING",
                    "-s",
                    &self.container_ip.to_string(),
                    "-p",
                    "udp",
                    "--dport",
                    "53",
                    "-j",
                    "DNAT",
                    "--to",
                    &format!("{}:53", nameserver),
                ])
                .output();

            // Forward TCP DNS queries (less common but sometimes used)
            let _ = Command::new("iptables")
                .args(&[
                    "-t",
                    "nat",
                    "-D",
                    "PREROUTING",
                    "-s",
                    &self.container_ip.to_string(),
                    "-p",
                    "tcp",
                    "--dport",
                    "53",
                    "-j",
                    "DNAT",
                    "--to",
                    &format!("{}:53", nameserver),
                ])
                .output();
        }
    }
}

impl Drop for VethPair {
    fn drop(&mut self) {
        let _ = self.cleanup();
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
        .map_err(|e| Error::ParseInt(format!("Failed to parse uid: {}", e)))?;

    Ok(uid == 0)
}
