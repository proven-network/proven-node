//! Mounts external filesystems into the enclave via NFS, intermediated by a
//! layer of FUSE-based AES-GCM disk-encryption based on enclave-internal
//! cryptographic keys.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::path::PathBuf;
use std::process::Stdio;

use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
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
    mount_dir: PathBuf,
    nfs_mount_dir: PathBuf,
    nfs_mount_point_dir: PathBuf,
    passfile_path: PathBuf,
    skip_fsck: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Options for creating a new `ExternalFs`.
pub struct ExternalFsOptions {
    /// The encryption key for gocryptfs.
    pub encryption_key: String,

    /// The NFS mount point.
    pub nfs_mount_point_dir: PathBuf,

    /// The directory to mount the external filesystem.
    pub mount_dir: PathBuf,

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
            nfs_mount_point_dir,
            mount_dir,
            skip_fsck,
        }: ExternalFsOptions,
    ) -> Result<Self> {
        // random name for nfs_dir
        let rand_id: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        let mnt_dir = PathBuf::from(MNT_DIR);
        let passfile_dir = PathBuf::from(PASSFILE_DIR);

        // Paths specific to this instance
        let nfs_mount_dir = mnt_dir.join(&rand_id);
        let passfile_path = passfile_dir.join(format!("{}.passfile", rand_id));

        // create directories
        std::fs::create_dir_all(&mount_dir)
            .map_err(|e| Error::Io("failed to create mount directory", e))?;
        std::fs::create_dir_all(&nfs_mount_dir)
            .map_err(|e| Error::Io("failed to create NFS mount directory", e))?;
        std::fs::create_dir_all(PASSFILE_DIR)
            .map_err(|e| Error::Io("failed to create passfile directory", e))?;

        // create passfile with encryption key (needed for gocryptfs)
        std::fs::write(&passfile_path, encryption_key)
            .map_err(|e| Error::Io("failed to write passfile", e))?;

        Ok(Self {
            mount_dir,
            nfs_mount_dir,
            nfs_mount_point_dir,
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
    /// This function will return an error if:
    /// - The filesystem is already started
    /// - Failed to mount NFS
    /// - Failed to set permissions
    /// - Failed to initialize gocryptfs
    /// - Failed to perform integrity check
    /// - The gocryptfs process exits with a non-zero status
    /// - Failed to wait for process completion
    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        mount_nfs(&self.nfs_mount_point_dir, &self.nfs_mount_dir).await?;
        ensure_permissions(&self.nfs_mount_dir, "0700").await?;

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
                .arg(&nfs_mount_dir)
                .arg(&mount_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| Error::Io("failed to spawn gocryptfs", e))?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            info!(
                "mounted gocrypt: {} -> {}",
                nfs_mount_dir.display(),
                mount_dir.display()
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
                result = cmd.wait() => {
                    let status = result.map_err(|e| Error::Io("failed to wait for gocryptfs", e))?;

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                () = shutdown_token.cancelled() => {
                    // Run umount command
                    let output = Command::new("umount")
                        .arg(&mount_dir)
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

                    cmd.wait()
                        .await
                        .map_err(|e| Error::Io("failed to wait for gocryptfs shutdown", e))?;

                    umount_nfs(&nfs_mount_dir).await?;

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
        std::fs::metadata(self.nfs_mount_dir.join(CONF_FILENAME)).is_ok()
    }

    async fn init_gocryptfs(&self) -> Result<()> {
        let cmd = Command::new("gocryptfs")
            .arg("-init")
            .arg("-passfile")
            .arg(&self.passfile_path)
            .arg(&self.nfs_mount_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .await;

        info!("{:?}", cmd);

        match cmd {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => Err(Error::NonZeroExitCode(output.status)),
            Err(e) => Err(Error::Io("failed to spawn gocryptfs", e)),
        }
    }

    async fn fsck_gocryptfs(&self) -> Result<()> {
        let cmd = Command::new("gocryptfs")
            .arg("-fsck")
            .arg("-kernel_cache")
            .arg("-sharedstorage")
            .arg("-passfile")
            .arg(&self.passfile_path)
            .arg(&self.nfs_mount_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .await;

        info!("{:?}", cmd);

        match cmd {
            Ok(output) if output.status.success() => Ok(()),
            Ok(output) => Err(Error::NonZeroExitCode(output.status)),
            Err(e) => Err(Error::Io("failed to spawn gocryptfs fsck", e)),
        }
    }
}

async fn mount_nfs(nfs_mount_point_dir: &PathBuf, nfs_mount_dir: &PathBuf) -> Result<()> {
    let cmd = Command::new("mount")
        .arg("-v")
        .arg("-t")
        .arg("nfs")
        .arg("-o")
        .arg("noatime,nolock,nfsvers=3,sync,nconnect=16,rsize=1048576,wsize=1048576")
        .arg(nfs_mount_point_dir)
        .arg(nfs_mount_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .await;

    info!(
        "mounted nfs: {} -> {}",
        nfs_mount_point_dir.display(),
        nfs_mount_dir.display()
    );

    match cmd {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(Error::NonZeroExitCode(output.status)),
        Err(e) => Err(Error::Io("failed to spawn mount", e)),
    }
}

async fn umount_nfs(nfs_mount_dir: &PathBuf) -> Result<()> {
    let cmd = Command::new("umount")
        .arg(nfs_mount_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .await;

    info!("unmounted nfs: {}", nfs_mount_dir.display());

    match cmd {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(Error::NonZeroExitCode(output.status)),
        Err(e) => Err(Error::Io("failed to spawn umount", e)),
    }
}

async fn ensure_permissions(path: &PathBuf, permissions: &str) -> Result<()> {
    let output = Command::new("chmod")
        .arg("-R")
        .arg(permissions)
        .arg(path)
        .output()
        .await;

    match output {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(Error::NonZeroExitCode(output.status)),
        Err(e) => Err(Error::Io("failed to spawn chmod", e)),
    }
}
