mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use regex::Regex;
use std::net::SocketAddrV4;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

pub struct DnscryptProxy {
    availability_zone: String,
    doh_addr: SocketAddrV4,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl DnscryptProxy {
    /// Creates a new instance of `DnscryptProxy`.
    ///
    /// # Arguments
    ///
    /// * `server_name` - The name of the server.
    /// * `listen_addr` - The address to listen on.
    pub fn new(availability_zone: String, doh_addr: SocketAddrV4) -> Self {
        Self {
            availability_zone,
            doh_addr,
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

        self.update_dnscrypt_config().await?;

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

    /// Generates a DNS-over-HTTPS SDNS stamp.
    ///
    /// # Arguments
    ///
    /// * `server_addr` - The SocketAddrV4 of the server.
    /// * `availability_zone` - The availability zone of the server.
    fn generate_doh_stamp(&self) -> String {
        use dns_stamp_parser::{Addr, DnsOverHttps, DnsStamp, Props};

        let props = Props::DNSSEC;
        let addr = Some(Addr::SocketAddr(self.doh_addr.to_string().parse().unwrap()));
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

        dns_stamp.encode().unwrap()
    }

    /// Updates the dnscrypt-proxy configuration.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    async fn update_dnscrypt_config(&self) -> Result<()> {
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
            self.generate_doh_stamp()
        );

        tokio::fs::write("/etc/dnscrypt-proxy/dnscrypt-proxy.toml", config)
            .await
            .map_err(Error::ConfigWrite)
    }
}
