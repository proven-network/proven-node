mod error;

pub use error::{Error, Result};

use std::process::Stdio;

use radix_common::network::NetworkDefinition;
// use regex::Regex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

static CONFIG_PATH: &str = "/var/lib/proven-node/babylon-node.config";
static KEYSTORE_PATH: &str = "/var/lib/proven-node/babylon-keystore.ks";
static KEYSTORE_PASS: &str = "notarealpassword"; // Irrelevant as keyfile never leaves TEE

static MAINNET_SEED_NODES: &[&str] = &[
    "radix://node_rdx1qf2x63qx4jdaxj83kkw2yytehvvmu6r2xll5gcp6c9rancmrfsgfw0vnc65@babylon-mainnet-eu-west-1-node0.radixdlt.com",
    "radix://node_rdx1qgxn3eeldj33kd98ha6wkjgk4k77z6xm0dv7mwnrkefknjcqsvhuu4gc609@babylon-mainnet-ap-southeast-2-node0.radixdlt.com",
    "radix://node_rdx1qwrrnhzfu99fg3yqgk3ut9vev2pdssv7hxhff80msjmmcj968487uugc0t2@babylon-mainnet-ap-south-1-node0.radixdlt.com",
    "radix://node_rdx1q0gnmwv0fmcp7ecq0znff7yzrt7ggwrp47sa9pssgyvrnl75tvxmvj78u7t@babylon-mainnet-us-east-1-node0.radixdlt.com",
];

static STOKENET_SEED_NODES: &[&str] = &[
    "radix://node_tdx_2_1qv89yg0la2jt429vqp8sxtpg95hj637gards67gpgqy2vuvwe4s5ss0va2y@babylon-stokenet-ap-south-1-node0.radixdlt.com",
    "radix://node_tdx_2_1qvtd9ffdhxyg7meqggr2ezsdfgjre5aqs6jwk5amdhjg86xhurgn5c79t9t@babylon-stokenet-ap-southeast-2-node0.radixdlt.com",
    "radix://node_tdx_2_1qwfh2nn0zx8cut5fqfz6n7pau2f7vdyl89mypldnn4fwlhaeg2tvunp8s8h@babylon-stokenet-eu-west-1-node0.radixdlt.com",
    "radix://node_tdx_2_1qwz237kqdpct5l3yjhmna66uxja2ymrf3x6hh528ng3gtvnwndtn5rsrad4@babylon-stokenet-us-east-1-node1.radixdlt.com"
];

static JAVA_OPTS: &[&str] = &[
    "-Djava.library.path=/bin/babylon-node",
    "--enable-preview",
    "-server",
    "-Xms2g",
    "-Xmx12g",
    "-XX:MaxDirectMemorySize=2048m",
    "-XX:+HeapDumpOnOutOfMemoryError",
    "-XX:+UseCompressedOops",
    "-Djavax.net.ssl.trustStore=/etc/ssl/certs/java/cacerts",
    "-Djavax.net.ssl.trustStoreType=jks",
    "-Djava.security.egd=file:/dev/urandom",
    "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
];

pub struct BabylonNode {
    network_definition: NetworkDefinition,
    store_dir: String,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl BabylonNode {
    pub fn new(network_definition: NetworkDefinition, store_dir: String) -> Self {
        Self {
            network_definition,
            store_dir,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        self.generate_node_key().await?;
        info!("generated node key");

        self.update_node_config().await?;
        info!("updated node config");

        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the babylon-node process
            let mut cmd = Command::new("/bin/babylon-node/core-v1.2.3/bin/core")
                .env("RADIX_NODE_KEYSTORE_PASSWORD", KEYSTORE_PASS)
                .env("JAVA_OPTS", JAVA_OPTS.join(" "))
                .env("LD_PRELOAD", "/bin/babylon-node/libcorerust.so")
                .arg("-config")
                .arg(CONFIG_PATH)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(Error::Spawn)?;

            let stdout = cmd.stdout.take().ok_or(Error::OutputParse)?;
            let stderr = cmd.stderr.take().ok_or(Error::OutputParse)?;

            // Spawn a task to read and process the stdout output of the babylon-node process
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

            // Spawn a task to read and process the stderr output of the babylon-node process
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

            // Wait for the babylon-node process to exit or for the shutdown token to be cancelled
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
        info!("babylon-node shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("babylon-node shutdown");
    }

    async fn generate_node_key(&self) -> Result<()> {
        let output = Command::new("/bin/babylon-node/core-v1.2.3/bin/keygen")
            .arg("-k")
            .arg(KEYSTORE_PATH)
            .arg("-p")
            .arg(KEYSTORE_PASS)
            .output()
            .await
            .map_err(Error::Spawn)?;

        if !output.status.success() {
            return Err(Error::NonZeroExitCode(output.status));
        }

        Ok(())
    }

    async fn update_node_config(&self) -> Result<()> {
        let seed_nodes = match self.network_definition.id {
            1 => MAINNET_SEED_NODES,
            2 => STOKENET_SEED_NODES,
            _ => unreachable!(),
        }
        .join(",");

        let config = format!(
            r#"
            network.id={}
            network.p2p.seed_nodes={}
            node.key.path={}
            db.location={}
        "#,
            self.network_definition.id, seed_nodes, KEYSTORE_PATH, self.store_dir
        );

        let mut config_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(CONFIG_PATH)
            .unwrap();

        std::io::Write::write_all(&mut config_file, config.as_bytes())
            .map_err(Error::ConfigWrite)?;

        Ok(())
    }
}
