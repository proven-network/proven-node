mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use regex::Regex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

pub struct Postgres {
    store_dir: String,
    username: String,
    password: String,
    skip_vacuum: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Postgres {
    pub fn new(store_dir: String, username: String, password: String, skip_vacuum: bool) -> Self {
        Self {
            store_dir,
            username,
            password,
            skip_vacuum,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        if !self.is_initialized() {
            self.initialize_database().await?;
        }

        // ensure postmaster.pid does not exist
        let postmaster_pid = std::path::Path::new(&self.store_dir).join("postmaster.pid");
        if postmaster_pid.exists() {
            std::fs::remove_file(&postmaster_pid).unwrap();
        }

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let store_dir = self.store_dir.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the postgres process
            let mut cmd = Command::new("/usr/local/pgsql/bin/postgres")
                .arg("-D")
                .arg(&store_dir)
                .arg("-c")
                .arg("maintenance_work_mem=1GB")
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stderr output of the postgres process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                let re = Regex::new(
                    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3} UTC \[\d+\] (\w+):  (.*)",
                )
                .unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(caps) = re.captures(&line) {
                        let label = caps.get(1).unwrap().as_str();
                        let message = caps.get(2).unwrap().as_str();
                        match label {
                            "DEBUG1" => debug!("{}", message),
                            "DEBUG2" => debug!("{}", message),
                            "DEBUG3" => debug!("{}", message),
                            "DEBUG4" => debug!("{}", message),
                            "DEBUG5" => debug!("{}", message),
                            "INFO" => info!("{}", message),
                            "NOTICE" => info!("{}", message),
                            "WARNING" => warn!("{}", message),
                            "ERROR" => error!("{}", message),
                            "LOG" => info!("{}", message),
                            "FATAL" => error!("{}", message),
                            "PANIC" => error!("{}", message),
                            _ => error!("{}", line),
                        }
                    } else {
                        error!("{}", line);
                    }
                }
            });

            // Wait for the postgres process to exit or for the shutdown token to be cancelled
            tokio::select! {
                _ = cmd.wait() => {
                    let status = cmd.wait().await.unwrap();

                    if !status.success() {
                        return Err(Error::NonZeroExitCode(status));
                    }

                    Ok(())
                }
                _ = shutdown_token.cancelled() => {
                    let pid = Pid::from_raw(cmd.id().unwrap() as i32);

                    if let Err(e) = signal::kill(pid, Signal::SIGTERM) {
                        error!("Failed to send SIGTERM signal: {}", e);
                    } else {
                        info!("postgres entered smart shutdown...");
                    }

                    let _ = cmd.wait().await;

                    Ok(())
                }
            }
        });

        self.task_tracker.close();

        self.wait_until_ready().await?;

        if !self.skip_vacuum {
            self.vacuum_database().await?;
        }

        Ok(server_task)
    }

    /// Shuts down the server.
    pub async fn shutdown(&self) {
        info!("postgres shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("postgres shutdown");
    }

    async fn initialize_database(&self) -> Result<()> {
        // Write password to a file in tmp for use by initdb
        let password_file = std::path::Path::new("/tmp/pgpass");
        tokio::fs::write(password_file, self.password.clone())
            .await
            .unwrap();

        let cmd = Command::new("/usr/local/pgsql/bin/initdb")
            .arg("-D")
            .arg(&self.store_dir)
            .arg("-U")
            .arg(self.username.clone())
            .arg("--pwfile")
            .arg(password_file)
            .output()
            .await
            .map_err(Error::Spawn)?;

        info!("stdout: {}", String::from_utf8_lossy(&cmd.stdout));
        info!("stderr: {}", String::from_utf8_lossy(&cmd.stderr));

        if !cmd.status.success() {
            return Err(Error::NonZeroExitCode(cmd.status));
        }

        Ok(())
    }

    // check if the database is initialized
    fn is_initialized(&self) -> bool {
        std::path::Path::new(&self.store_dir).exists()
            && std::path::Path::new(&self.store_dir)
                .join("PG_VERSION")
                .exists()
    }

    async fn wait_until_ready(&self) -> Result<()> {
        loop {
            info!("checking if postgres is ready...");
            let cmd = Command::new("/usr/local/pgsql/bin/pg_isready")
                .arg("-h")
                .arg("127.0.0.1")
                .arg("-p")
                .arg("5432")
                .arg("-U")
                .arg(&self.username)
                .arg("-d")
                .arg("postgres")
                .output()
                .await
                .map_err(Error::Spawn)?;

            if cmd.status.success() {
                info!("postgres is ready");
                return Ok(());
            } else {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

    async fn vacuum_database(&self) -> Result<()> {
        info!("vacuuming database...");

        let mut cmd = Command::new("/usr/local/pgsql/bin/vacuumdb")
            .arg("-U")
            .arg(&self.username)
            .arg("--all")
            .arg("--analyze")
            .arg("--full")
            .arg("--jobs=4")
            .arg("--verbose")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(Error::Spawn)?;

        let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
        let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

        let stdout_writer = tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                info!("{}", line)
            }
        });

        let stderr_writer = tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                info!("{}", line)
            }
        });

        tokio::select! {
            e = cmd.wait() => {
                let exit_status = e.map_err(Error::Spawn)?;
                if !exit_status.success() {
                    return Err(Error::NonZeroExitCode(exit_status));
                }
            }
            _ = stdout_writer => {},
            _ = stderr_writer => {},
        }

        Ok(())
    }
}
