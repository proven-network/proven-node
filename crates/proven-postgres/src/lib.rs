mod error;

pub use error::{Error, Result};

use std::process::Stdio;

// use regex::Regex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

pub struct Postgres {
    store_dir: String,
    username: String,
    password: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Postgres {
    pub fn new(store_dir: String, username: String, password: String) -> Self {
        Self {
            store_dir,
            username,
            password,
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

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let store_dir = self.store_dir.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the postgres process
            let mut cmd = Command::new("/usr/local/pgsql/bin/postgres")
                .arg("-D")
                .arg(&store_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stdout output of the postgres process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();

                // let re = Regex::new(r"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]) (\[[A-Z]+\]) (.*)")
                //     .unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    info!("{}", line);
                    // if let Some(caps) = re.captures(&line) {
                    //     let label = caps.get(2).unwrap().as_str();
                    //     let message = caps.get(3).unwrap().as_str();
                    //     match label {
                    //         "[INFO]" => info!("{}", message),
                    //         "[NOTICE]" => info!("{}", message),
                    //         "[DEBUG]" => debug!("{}", message),
                    //         "[WARNING]" => warn!("{}", message),
                    //         "[CRITICAL]" => warn!("{}", message),
                    //         "[ERROR]" => error!("{}", message),
                    //         "[FATAL]" => error!("{}", message),
                    //         _ => error!("{}", line),
                    //     }
                    // } else {
                    //     error!("{}", line);
                    // }
                }
            });

            // Spawn a task to read and process the stderr output of the postgres process
            task_tracker.spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                // let re = Regex::new(r"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]) (\[[A-Z]+\]) (.*)")
                //     .unwrap();

                while let Ok(Some(line)) = lines.next_line().await {
                    warn!("{}", line);
                    // if let Some(caps) = re.captures(&line) {
                    //     let label = caps.get(2).unwrap().as_str();
                    //     let message = caps.get(3).unwrap().as_str();
                    //     match label {
                    //         "[INFO]" => info!("{}", message),
                    //         "[NOTICE]" => info!("{}", message),
                    //         "[DEBUG]" => debug!("{}", message),
                    //         "[WARNING]" => warn!("{}", message),
                    //         "[CRITICAL]" => warn!("{}", message),
                    //         "[ERROR]" => error!("{}", message),
                    //         "[FATAL]" => error!("{}", message),
                    //         _ => error!("{}", line),
                    //     }
                    // } else {
                    //     error!("{}", line);
                    // }
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
}
