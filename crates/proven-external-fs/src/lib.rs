mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

static CONF_PATH: &str = "/var/lib/proven-node/nfs/encrypted/gocryptfs.conf";
static DECRYPTED_PATH: &str = "/var/lib/proven-node/external-fs";
static ENCRYPTED_PATH: &str = "/var/lib/proven-node/nfs/encrypted";
static PASSFILE_PATH: &str = "/var/lib/proven-node/gocryptfs.passfile";
static NFS_DIR: &str = "/var/lib/proven-node/nfs";

pub struct ExternalFs {
    encryption_key: String,
    nfs_server: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl ExternalFs {
    pub fn new(encryption_key: String, nfs_server: String) -> Self {
        Self {
            encryption_key,
            nfs_server,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        tokio::fs::create_dir_all(NFS_DIR).await.unwrap();

        info!("created NFS directory");

        self.mount_nfs().await?;
        self.write_passfile()?;

        if tokio::fs::metadata(CONF_PATH).await.is_err() {
            info!("gocrpytfs not initialized, initializing...");
            self.init_gocryptfs().await?;
            info!("gocrpytfs initialized");
        }

        tokio::fs::create_dir_all(DECRYPTED_PATH).await.unwrap();

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let gocryptfs_task = self.task_tracker.spawn(async move {
            // Start the gocryptfs process
            let mut cmd = Command::new("sudo")
                .arg("gocrpytfs")
                .arg("-passfile")
                .arg(PASSFILE_PATH)
                .arg("-fg")
                .arg(ENCRYPTED_PATH)
                .arg(DECRYPTED_PATH)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stderr output of the gocryptfs process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    info!("{}", line);
                }
            });

            // Wait for the gocryptfs process to exit or for the shutdown token to be cancelled
            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.unwrap();

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                _ = shutdown_token.cancelled() => {
                    // Run umount command
                    let _ = Command::new("umount")
                        .arg(DECRYPTED_PATH)
                        .stdout(Stdio::inherit())
                        .stderr(Stdio::inherit())
                        .output()
                        .await;

                    cmd.wait().await.unwrap();

                    Ok(())
                }
            }
        });

        self.task_tracker.close();

        Ok(gocryptfs_task)
    }

    /// Shuts down the server.
    pub async fn shutdown(&self) {
        info!("external fs shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("external fs shut down");
    }

    async fn mount_nfs(&self) -> Result<()> {
        let cmd = Command::new("mount")
            .arg("-t")
            .arg("nfs")
            .arg("-o")
            .arg("noatime,nfsvers=4.2,sync,rsize=1048576,wsize=1048576")
            .arg(self.nfs_server.as_str())
            .arg(NFS_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .await;

        match cmd {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => Err(Error::NonZeroExitCode(output.status)),
            Err(e) => Err(Error::Spawn(e)),
        }
    }

    async fn init_gocryptfs(&self) -> Result<()> {
        tokio::fs::create_dir_all(ENCRYPTED_PATH).await.unwrap();

        let cmd = Command::new("gocryptfs")
            .arg("-init")
            .arg("-passfile")
            .arg(PASSFILE_PATH)
            .arg(ENCRYPTED_PATH)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .await;

        info!("{:?}", cmd);

        match cmd {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => Err(Error::NonZeroExitCode(output.status)),
            Err(e) => Err(Error::Spawn(e)),
        }
    }

    fn write_passfile(&self) -> Result<()> {
        info!("writing passfile");

        let mut passfile = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(PASSFILE_PATH)?;

        Ok(std::io::Write::write_all(
            &mut passfile,
            self.encryption_key.as_bytes(),
        )?)
    }
}
