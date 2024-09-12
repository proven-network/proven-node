mod error;

use aws_sdk_route53resolver::types::IpAddressStatus;
pub use error::{Error, Result};

use std::net::{Ipv4Addr, SocketAddrV4};
use std::process::Stdio;

use aws_config::Region;
use regex::Regex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

pub struct DnscryptProxy {
    region: String,
    vpc_id: String,
    availability_zone: String,
    subnet_id: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl DnscryptProxy {
    /// Creates a new instance of `DnscryptProxy`.
    ///
    /// # Arguments
    ///
    /// * `region` - The AWS region.
    /// * `vpc_id` - The VPC ID.
    /// * `availability_zone` - The availability zone.
    /// * `subnet_id` - The subnet ID.
    ///
    /// # Returns
    ///
    /// A new instance of `DnscryptProxy`.
    pub fn new(
        region: String,
        vpc_id: String,
        availability_zone: String,
        subnet_id: String,
    ) -> Self {
        Self {
            region,
            vpc_id,
            availability_zone,
            subnet_id,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Starts the proxy server.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that can be used to await the completion of the server task.
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        let config = aws_config::from_env()
            .region(Region::new(self.region.clone()))
            .load()
            .await;

        // get inbound route53 resolver by vpc
        let vpc_filter = aws_sdk_route53resolver::types::Filter::builder()
            .name("HostVPCId")
            .values(self.vpc_id.clone())
            .build();
        let direction_filter = aws_sdk_route53resolver::types::Filter::builder()
            .name("Direction")
            .values("INBOUND".to_string())
            .build();
        let status_filter = aws_sdk_route53resolver::types::Filter::builder()
            .name("Status")
            .values("OPERATIONAL".to_string())
            .build();
        let route53_client = aws_sdk_route53resolver::Client::new(&config);
        let resolver_endpoints = route53_client
            .list_resolver_endpoints()
            .filters(vpc_filter)
            .filters(direction_filter)
            .filters(status_filter)
            .send()
            .await
            .map_err(|e| Error::Route53(e.into()))?
            .resolver_endpoints
            .unwrap_or_default();

        let resolver_endpoint = resolver_endpoints
            .iter()
            .find(|resolver_endpoint| {
                resolver_endpoint
                    .protocols
                    .clone()
                    .unwrap_or_default()
                    .contains(&aws_sdk_route53resolver::types::Protocol::Doh)
                    && resolver_endpoint.resolver_endpoint_type
                        != Some(aws_sdk_route53resolver::types::ResolverEndpointType::Ipv6)
            })
            .ok_or(Error::ResolverEndpointNotFound)?;

        let doh_ip = route53_client
            .list_resolver_endpoint_ip_addresses()
            .resolver_endpoint_id(resolver_endpoint.id.clone().unwrap())
            .send()
            .await
            .map_err(|e| Error::Route53(e.into()))?
            .ip_addresses
            .unwrap_or_default()
            .iter()
            .find(|ip| {
                ip.subnet_id == Some(self.subnet_id.clone())
                    && ip.status == Some(IpAddressStatus::Attached)
            })
            .ok_or(Error::ResolverEndpointNotFound)?
            .ip
            .as_ref()
            .ok_or(Error::ResolverEndpointNotFound)?
            .parse::<Ipv4Addr>()
            .map_err(|_| Error::ResolverEndpointNotFound)?;

        self.update_dnscrypt_config(doh_ip).await?;

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the dnscrypt-proxy process
            let mut cmd = Command::new("dnscrypt-proxy")
                .arg("-config")
                .arg("/etc/dnscrypt-proxy/dnscrypt-proxy.toml")
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stderr output of the dnscrypt-proxy process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                let re = Regex::new(r"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]) (\[[A-Z]+\]) (.*)")
                    .unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(caps) = re.captures(&line) {
                        let label = caps.get(2).unwrap().as_str();
                        let message = caps.get(3).unwrap().as_str();
                        match label {
                            "[INFO]" => info!("{}", message),
                            "[NOTICE]" => info!("{}", message),
                            "[DEBUG]" => debug!("{}", message),
                            "[WARNING]" => warn!("{}", message),
                            "[CRITICAL]" => warn!("{}", message),
                            "[ERROR]" => error!("{}", message),
                            "[FATAL]" => error!("{}", message),
                            _ => error!("{}", line),
                        }
                    } else {
                        error!("{}", line);
                    }
                }
            });

            // Wait for the dnscrypt-proxy process to exit or for the shutdown token to be cancelled
            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.unwrap();

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                _ = shutdown_token.cancelled() => {
                    cmd.kill().await.unwrap();

                    Ok(())
                }
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        self.task_tracker.close();

        Ok(server_task)
    }

    /// Shuts down the server.
    pub async fn shutdown(&self) {
        info!("dnscrypt-proxy shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("dnscrypt-proxy shutdown");
    }

    /// Updates the dnscrypt-proxy configuration.
    ///
    /// # Arguments
    ///
    /// * `doh_ip` - The IPv4Addr of the DoH resolver.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    async fn update_dnscrypt_config(&self, doh_ip: Ipv4Addr) -> Result<()> {
        use dns_stamp_parser::{Addr, DnsOverHttps, DnsStamp, Props};

        let props = Props::DNSSEC;
        let sock_addr = SocketAddrV4::new(doh_ip, 443);
        let addr = Some(Addr::SocketAddr(sock_addr.to_string().parse().unwrap()));
        let hostname = format!("route53resolver.{}.amazonaws.com", self.availability_zone);
        let path = "/dns-query".to_string();
        let dns_stamp = DnsStamp::DnsOverHttps(DnsOverHttps {
            props,
            addr,
            hashi: Vec::new(),
            hostname,
            path,
            bootstrap_ipi: Vec::new(),
        });

        let config = format!(
            r#"
# dnscrypt-proxy configuration file

# Listen addresses
listen_addresses = ['127.0.0.1:53', '[::1]:53']

# Server names
server_names = ['route53-resolver']

# Static server definitions
[static.'route53-resolver']
stamp = '{}'
        "#,
            dns_stamp.encode().unwrap()
        );

        tokio::fs::create_dir_all("/etc/dnscrypt-proxy")
            .await
            .unwrap();

        tokio::fs::write("/etc/dnscrypt-proxy/dnscrypt-proxy.toml", config)
            .await
            .map_err(Error::ConfigWrite)
    }
}
