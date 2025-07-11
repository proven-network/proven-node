use crate::{Error, Result};
use std::net::Ipv4Addr;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};

#[cfg(target_os = "linux")]
use nix::sys::signal::{self, Signal};
#[cfg(target_os = "linux")]
use nix::unistd::Pid;

pub struct Proxy {
    tun_addr: Ipv4Addr,
    tun_mask: u8,
    vsock_port: u32,
    is_host: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Proxy {
    pub fn new(tun_addr: Ipv4Addr, tun_mask: u8, vsock_port: u32, is_host: bool) -> Self {
        Self {
            tun_addr,
            tun_mask,
            vsock_port,
            is_host,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        // Try to find the binary in several locations
        let binary_name = if self.is_host {
            "vsock-proxy-host"
        } else {
            "vsock-proxy-enclave"
        };

        // Check common locations
        let binary_path =
            if std::path::Path::new(&format!("/usr/local/bin/{}", binary_name)).exists() {
                format!("/usr/local/bin/{}", binary_name)
            } else if std::path::Path::new(&format!("/usr/bin/{}", binary_name)).exists() {
                format!("/usr/bin/{}", binary_name)
            } else {
                // Fallback to PATH
                binary_name.to_string()
            };
        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let tun_addr = self.tun_addr.to_string();
        let tun_mask = self.tun_mask.to_string();
        let vsock_port = self.vsock_port.to_string();

        let server_task = self.task_tracker.spawn(async move {
            // Start the Zig proxy process
            let mut cmd = Command::new(binary_path)
                .arg("--tun-addr")
                .arg(&tun_addr)
                .arg("--tun-mask")
                .arg(&tun_mask)
                .arg("--vsock-port")
                .arg(&vsock_port)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn task to handle stdout
            task_tracker.spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    // Parse Zig output format
                    if line.contains("error") {
                        error!("{}", line);
                    } else if line.contains("info") {
                        info!("{}", line);
                    } else {
                        debug!("{}", line);
                    }
                }
                Ok::<(), Error>(())
            });

            // Spawn task to handle stderr
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    error!("{}", line);
                }
                Ok::<(), Error>(())
            });

            // Wait for process exit or shutdown
            tokio::select! {
                result = cmd.wait() => {
                    let status = result.map_err(Error::Spawn)?;
                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }
                    Ok(())
                }
                () = shutdown_token.cancelled() => {
                    #[cfg(target_os = "linux")]
                    {
                        #[allow(clippy::cast_possible_wrap)]
                        let pid = Pid::from_raw(cmd.id().ok_or(Error::OutputParse)? as i32);
                        signal::kill(pid, Signal::SIGTERM).map_err(Error::Signal)?;
                    }
                    #[cfg(not(target_os = "linux"))]
                    {
                        // On non-Linux, just try to kill the process
                        cmd.kill().await.ok();
                    }
                    let _ = cmd.wait().await;
                    Ok(())
                }
            }
        });

        self.task_tracker.close();
        Ok(server_task)
    }

    pub async fn shutdown(&self) {
        info!("vsock-proxy shutting down...");
        self.shutdown_token.cancel();
        self.task_tracker.wait().await;
        info!("vsock-proxy shutdown");
    }
}
