//! Mounts external filesystems into the enclave via NFS, intermediated by a layer of FUSE-based AES-GCM disk-encryption based on enclave-internal cryptographic keys.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

static CONF_FILENAME: &str = "gocryptfs.conf";
static MNT_DIR: &str = "/mnt";
static PASSFILE_DIR: &str = "/var/lib/proven/gocryptfs";

/// Manages an external filesystem mounted via NFS and encrypted with gocryptfs.
pub struct ExternalFs {
    mount_dir: String,
    nfs_mount_dir: String,
    nfs_mount_point: String,
    passfile_path: String,
    skip_fsck: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Options for creating a new `ExternalFs`.
pub struct ExternalFsOptions {
    /// The encryption key for gocryptfs.
    pub encryption_key: String,

    /// The NFS mount point.
    pub nfs_mount_point: String,

    /// The directory to mount the external filesystem.
    pub mount_dir: String,

    /// Whether to skip the gocryptfs integrity check.
    pub skip_fsck: bool,
}

impl ExternalFs {
    /// Creates a new instance of `ExternalFs`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to create required directories
    /// - Failed to write encryption key to passfile
    pub fn new(
        ExternalFsOptions {
            encryption_key,
            nfs_mount_point,
            mount_dir,
            skip_fsck,
        }: ExternalFsOptions,
    ) -> Result<Self> {
        // random name for nfs_dir
        let sub_dir: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        // Paths specific to this instance
        let nfs_mount_dir = format!("{MNT_DIR}/{sub_dir}");
        let passfile_path = format!("{PASSFILE_DIR}/{sub_dir}.passfile");

        // create directories
        std::fs::create_dir_all(&mount_dir)
            .map_err(|e| Error::IoError("failed to create mount directory", e))?;
        std::fs::create_dir_all(&nfs_mount_dir)
            .map_err(|e| Error::IoError("failed to create NFS mount directory", e))?;
        std::fs::create_dir_all(PASSFILE_DIR)
            .map_err(|e| Error::IoError("failed to create passfile directory", e))?;

        // create passfile with encryption key (needed for gocryptfs)
        std::fs::write(&passfile_path, encryption_key)
            .map_err(|e| Error::IoError("failed to write passfile", e))?;

        Ok(Self {
            mount_dir,
            nfs_mount_dir,
            nfs_mount_point,
            passfile_path,
            skip_fsck,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }

    /// Starts the external filesystem.
    ///
    /// # Errors
    ///
    /// This function will return an error if the filesystem is already started,
    /// if there is an error mounting NFS, ensuring permissions, initializing gocryptfs,
    /// or if the gocryptfs process exits with a non-zero status.
    ///
    /// # Panics
    ///
    /// This function will panic if it fails to wait for the gocryptfs process to exit.
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        mount_nfs(self.nfs_mount_point.clone(), self.nfs_mount_dir.clone()).await?;
        ensure_permissions(self.nfs_mount_dir.as_str(), "0700").await?;

        if self.is_initialized() {
            info!("gocryptfs already initialized");

            if self.skip_fsck {
                info!("skipping integrity check...");
            } else {
                info!("running integrity check...");
                self.fsck_gocryptfs().await?;
                info!("integrity check successful");
            }
        } else {
            info!("gocryptfs not initialized, initializing...");
            self.init_gocryptfs().await?;
        }

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let passfile_path = self.passfile_path.clone();
        let nfs_mount_dir = self.nfs_mount_dir.clone();
        let mount_dir = self.mount_dir.clone();

        let gocryptfs_task = self.task_tracker.spawn(async move {
            // Start the gocryptfs process
            let mut cmd = Command::new("gocryptfs")
                .arg("-passfile")
                .arg(passfile_path)
                .arg("-fg")
                .arg("-noprealloc")
                .arg("-nosyslog")
                .arg("-kernel_cache")
                .arg("-sharedstorage")
                .arg(nfs_mount_dir.as_str())
                .arg(mount_dir.as_str())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            info!(
                "mounted gocrypt: {} -> {}",
                nfs_mount_dir, mount_dir
            );

            // Spawn a task to read and process the stdout output of the gocryptfs process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    info!("{}", line);
                }
            });

            // Spawn a task to read and process the stderr output of the gocryptfs process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    warn!("{}", line);
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
                () = shutdown_token.cancelled() => {
                    // Run umount command
                    let output = Command::new("umount")
                        .arg(mount_dir.as_str())
                        .stdout(Stdio::inherit())
                        .stderr(Stdio::inherit())
                        .output()
                        .await;

                    // log output
                    match output {
                        Ok(output) if output.status.success() => info!("gocryptfs umount successful"),
                        Ok(output) => warn!("gocryptfs failed: {:?}", output),
                        Err(e) => warn!("gocryptfs failed: {:?}", e),
                    }

                    cmd.wait().await.unwrap();

                    umount_nfs(nfs_mount_dir).await?;

                    Ok(())
                }
            }
        });

        self.task_tracker.close();

        // Sleep for a bit to allow mount to complete
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        Ok(gocryptfs_task)
    }

    /// Shuts down the server.
    pub async fn shutdown(&self) {
        info!("external fs shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("external fs shut down");
    }

    fn is_initialized(&self) -> bool {
        std::fs::metadata(format!("{}/{}", self.nfs_mount_dir, CONF_FILENAME)).is_ok()
    }

    async fn init_gocryptfs(&self) -> Result<()> {
        let cmd = Command::new("gocryptfs")
            .arg("-init")
            .arg("-passfile")
            .arg(self.passfile_path.as_str())
            .arg(self.nfs_mount_dir.as_str())
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

    async fn fsck_gocryptfs(&self) -> Result<()> {
        let cmd = Command::new("gocryptfs")
            .arg("-fsck")
            .arg("-kernel_cache")
            .arg("-sharedstorage")
            .arg("-passfile")
            .arg(self.passfile_path.as_str())
            .arg(self.nfs_mount_dir.as_str())
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
}

async fn mount_nfs(nfs_mount_point: String, nfs_mount_dir: String) -> Result<()> {
    let cmd = Command::new("mount")
        .arg("-v")
        .arg("-t")
        .arg("nfs")
        .arg("-o")
        .arg("noatime,nolock,nfsvers=3,sync,nconnect=16,rsize=1048576,wsize=1048576")
        .arg(nfs_mount_point.as_str())
        .arg(nfs_mount_dir.as_str())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .await;

    info!("mounted nfs: {} -> {}", nfs_mount_point, nfs_mount_dir);

    match cmd {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(Error::NonZeroExitCode(output.status)),
        Err(e) => Err(Error::Spawn(e)),
    }
}

async fn umount_nfs(nfs_mount_dir: String) -> Result<()> {
    let cmd = Command::new("umount")
        .arg(nfs_mount_dir.as_str())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .await;

    info!("unmounted nfs: {}", nfs_mount_dir);

    match cmd {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(Error::NonZeroExitCode(output.status)),
        Err(e) => Err(Error::Spawn(e)),
    }
}

async fn ensure_permissions(path: &str, permissions: &str) -> Result<()> {
    let output = Command::new("chmod")
        .arg("-R")
        .arg(permissions)
        .arg(path)
        .output()
        .await;

    match output {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(Error::NonZeroExitCode(output.status)),
        Err(e) => Err(Error::Spawn(e)),
    }
}
