use super::error::{Error, Result};
use crate::Args;
use crate::hosts::check_hostname_resolution;
use crate::net::fetch_external_ip;
use crate::node::{LocalNode, LocalNodeCore, Services};

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation_mock::MockAttestor;
use proven_bitcoin_core::{BitcoinNetwork, BitcoinNode, BitcoinNodeOptions};
use proven_core::{Core, CoreOptions, LightCore, LightCoreOptions};
use proven_ethereum_lighthouse::{
    EthereumNetwork as LighthouseNetwork, LighthouseNode, LighthouseNodeOptions,
};
use proven_ethereum_reth::{EthereumNetwork as RethNetwork, RethNode, RethNodeOptions};
use proven_governance::NodeSpecialization;
use proven_governance_mock::MockGovernance;
use proven_http_insecure::InsecureHttpServer;
use proven_messaging_nats::client::NatsClientOptions;
use proven_messaging_nats::service::NatsServiceOptions;
use proven_messaging_nats::stream::{NatsStream1, NatsStream2, NatsStream3, NatsStreamOptions};
use proven_nats_server::{NatsServer, NatsServerOptions};
use proven_network::{ProvenNetwork, ProvenNetworkOptions};
use proven_postgres::{Postgres, PostgresOptions};
use proven_radix_aggregator::{RadixAggregator, RadixAggregatorOptions};
use proven_radix_gateway::{RadixGateway, RadixGatewayOptions};
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_radix_node::{RadixNode, RadixNodeOptions};
use proven_runtime::{RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_streamed::{StreamedSqlStore1, StreamedSqlStore2, StreamedSqlStore3};
use proven_store_fs::{FsStore, FsStore1, FsStore2, FsStore3};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3, NatsStoreOptions};
use radix_common::prelude::NetworkDefinition;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

static GATEWAY_URL: &str = "http://127.0.0.1:8081";

static POSTGRES_USERNAME: &str = "your-username";
static POSTGRES_PASSWORD: &str = "your-password";
static POSTGRES_RADIX_STOKENET_DATABASE: &str = "radix-stokenet-db";

// TODO: This is in dire need of refactoring.
pub struct Bootstrap {
    args: Args,
    attestor: MockAttestor,
    external_ip: IpAddr,

    // added during initialization
    governance: Option<MockGovernance>,
    network: Option<ProvenNetwork<MockGovernance, MockAttestor>>,
    light_core: Option<LightCore<MockAttestor, MockGovernance>>,

    radix_mainnet_node: Option<RadixNode>,
    radix_mainnet_node_handle: Option<JoinHandle<()>>,

    radix_stokenet_node: Option<RadixNode>,
    radix_stokenet_node_handle: Option<JoinHandle<()>>,

    ethereum_mainnet_reth_node: Option<proven_ethereum_reth::RethNode>,
    ethereum_mainnet_reth_node_handle: Option<JoinHandle<()>>,

    ethereum_mainnet_lighthouse_node: Option<proven_ethereum_lighthouse::LighthouseNode>,
    ethereum_mainnet_lighthouse_node_handle: Option<JoinHandle<()>>,

    ethereum_holesky_reth_node: Option<proven_ethereum_reth::RethNode>,
    ethereum_holesky_reth_node_handle: Option<JoinHandle<()>>,

    ethereum_holesky_lighthouse_node: Option<proven_ethereum_lighthouse::LighthouseNode>,
    ethereum_holesky_lighthouse_node_handle: Option<JoinHandle<()>>,

    ethereum_sepolia_reth_node: Option<proven_ethereum_reth::RethNode>,
    ethereum_sepolia_reth_node_handle: Option<JoinHandle<()>>,

    ethereum_sepolia_lighthouse_node: Option<proven_ethereum_lighthouse::LighthouseNode>,
    ethereum_sepolia_lighthouse_node_handle: Option<JoinHandle<()>>,

    bitcoin_node: Option<BitcoinNode>,
    bitcoin_node_handle: Option<JoinHandle<()>>,

    postgres: Option<Postgres>,
    postgres_handle: Option<JoinHandle<()>>,

    radix_aggregator: Option<RadixAggregator>,
    radix_aggregator_handle: Option<JoinHandle<()>>,

    radix_gateway: Option<RadixGateway>,
    radix_gateway_handle: Option<JoinHandle<()>>,

    nats_client: Option<NatsClient>,
    nats_server: Option<NatsServer<MockGovernance, MockAttestor>>,
    nats_server_handle: Option<JoinHandle<proven_nats_server::Result<()>>>,

    core: Option<LocalNodeCore>,
    core_handle: Option<JoinHandle<proven_core::Result<()>>>,

    // state
    started: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Bootstrap {
    pub async fn new(args: Args) -> Result<Self> {
        let external_ip = fetch_external_ip().await?;

        Ok(Self {
            args,
            attestor: MockAttestor,
            external_ip,

            governance: None,
            network: None,
            light_core: None,
            radix_mainnet_node: None,
            radix_mainnet_node_handle: None,

            radix_stokenet_node: None,
            radix_stokenet_node_handle: None,

            ethereum_mainnet_reth_node: None,
            ethereum_mainnet_reth_node_handle: None,

            ethereum_mainnet_lighthouse_node: None,
            ethereum_mainnet_lighthouse_node_handle: None,

            ethereum_holesky_reth_node: None,
            ethereum_holesky_reth_node_handle: None,

            ethereum_holesky_lighthouse_node: None,
            ethereum_holesky_lighthouse_node_handle: None,

            ethereum_sepolia_reth_node: None,
            ethereum_sepolia_reth_node_handle: None,

            ethereum_sepolia_lighthouse_node: None,
            ethereum_sepolia_lighthouse_node_handle: None,

            bitcoin_node: None,
            bitcoin_node_handle: None,

            postgres: None,
            postgres_handle: None,

            radix_aggregator: None,
            radix_aggregator_handle: None,

            radix_gateway: None,
            radix_gateway_handle: None,

            nats_client: None,
            nats_server: None,
            nats_server_handle: None,

            core: None,
            core_handle: None,

            started: false,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::large_stack_frames)]
    pub async fn initialize(mut self) -> Result<LocalNode> {
        if self.started {
            return Err(Error::AlreadyStarted);
        }

        self.started = true;

        if let Err(e) = self.start_network_cluster().await {
            error!("failed to get network topology: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_radix_node().await {
            error!("failed to start radix-node: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_ethereum_mainnet_node().await {
            error!("failed to start ethereum mainnet nodes: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_ethereum_holesky_node().await {
            error!("failed to start ethereum holesky nodes: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_ethereum_sepolia_node().await {
            error!("failed to start ethereum sepolia nodes: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_bitcoin_node().await {
            error!("failed to start bitcoin node: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_postgres().await {
            error!("failed to start postgres: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_radix_aggregator().await {
            error!("failed to start radix-aggregator: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_radix_gateway().await {
            error!("failed to start radix-gateway: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_nats_server().await {
            error!("failed to start nats server: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_core().await {
            error!("failed to start core: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        // Optional handles
        let radix_mainnet_node_handle = self.radix_mainnet_node_handle.take();
        let radix_stokenet_node_handle = self.radix_stokenet_node_handle.take();
        let ethereum_mainnet_reth_node_handle = self.ethereum_mainnet_reth_node_handle.take();
        let ethereum_mainnet_lighthouse_node_handle =
            self.ethereum_mainnet_lighthouse_node_handle.take();
        let ethereum_holesky_reth_node_handle = self.ethereum_holesky_reth_node_handle.take();
        let ethereum_holesky_lighthouse_node_handle =
            self.ethereum_holesky_lighthouse_node_handle.take();
        let ethereum_sepolia_reth_node_handle = self.ethereum_sepolia_reth_node_handle.take();
        let ethereum_sepolia_lighthouse_node_handle =
            self.ethereum_sepolia_lighthouse_node_handle.take();
        let bitcoin_node_handle = self.bitcoin_node_handle.take();
        let postgres_handle = self.postgres_handle.take();
        let radix_aggregator_handle = self.radix_aggregator_handle.take();
        let radix_gateway_handle = self.radix_gateway_handle.take();

        // Mandatory handles
        let nats_server_handle = self.nats_server_handle.take().unwrap();
        let core_handle = self.core_handle.take().unwrap();

        // Optional services
        let radix_mainnet_node_option = self
            .radix_mainnet_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let radix_stokenet_node_option = self
            .radix_stokenet_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let ethereum_mainnet_reth_node_option = self
            .ethereum_mainnet_reth_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let ethereum_mainnet_lighthouse_node_option = self
            .ethereum_mainnet_lighthouse_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let ethereum_holesky_reth_node_option = self
            .ethereum_holesky_reth_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let ethereum_holesky_lighthouse_node_option = self
            .ethereum_holesky_lighthouse_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let ethereum_sepolia_reth_node_option = self
            .ethereum_sepolia_reth_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let ethereum_sepolia_lighthouse_node_option = self
            .ethereum_sepolia_lighthouse_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let bitcoin_node_option = self
            .bitcoin_node
            .take()
            .map(|node| Arc::new(Mutex::new(node)));
        let postgres = self
            .postgres
            .take()
            .map(|postgres| Arc::new(Mutex::new(postgres)));
        let radix_aggregator = self
            .radix_aggregator
            .take()
            .map(|aggregator| Arc::new(Mutex::new(aggregator)));
        let radix_gateway = self
            .radix_gateway
            .take()
            .map(|gateway| Arc::new(Mutex::new(gateway)));

        // Mandatory services
        let nats_server = Arc::new(Mutex::new(self.nats_server.take().unwrap()));
        let core = Arc::new(Mutex::new(self.core.take().unwrap()));

        let node_services = Services {
            radix_mainnet_node: radix_mainnet_node_option.clone(),
            radix_stokenet_node: radix_stokenet_node_option.clone(),
            ethereum_holesky_reth_node: ethereum_holesky_reth_node_option.clone(),
            ethereum_holesky_lighthouse_node: ethereum_holesky_lighthouse_node_option.clone(),
            ethereum_mainnet_reth_node: ethereum_mainnet_reth_node_option.clone(),
            ethereum_mainnet_lighthouse_node: ethereum_mainnet_lighthouse_node_option.clone(),
            ethereum_sepolia_reth_node: ethereum_sepolia_reth_node_option.clone(),
            ethereum_sepolia_lighthouse_node: ethereum_sepolia_lighthouse_node_option.clone(),
            bitcoin_node: bitcoin_node_option.clone(),
            postgres: postgres.clone(),
            radix_aggregator: radix_aggregator.clone(),
            radix_gateway: radix_gateway.clone(),
            nats_server: nats_server.clone(),
            core: core.clone(),
        };

        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            // Tasks that must be running for the enclave to function
            let critical_tasks = tokio::spawn(async move {
                tokio::select! {
                    _ = async {
                        if let Some(handle) = radix_mainnet_node_handle {
                            if let Ok(e) = handle.await {
                                error!("radix mainnet node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = radix_stokenet_node_handle {
                            if let Ok(e) = handle.await {
                                error!("radix stokenet node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = ethereum_mainnet_reth_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum mainnet reth node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = ethereum_mainnet_lighthouse_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum mainnet lighthouse node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = ethereum_holesky_reth_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum holesky reth node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = ethereum_holesky_lighthouse_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum holesky lighthouse node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = ethereum_sepolia_reth_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum sepolia reth node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = ethereum_sepolia_lighthouse_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum sepolia lighthouse node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = bitcoin_node_handle {
                            if let Ok(e) = handle.await {
                                error!("bitcoin node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = postgres_handle {
                            if let Ok(e) = handle.await {
                                error!("postgres exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = radix_aggregator_handle {
                            if let Ok(e) = handle.await {
                                error!("radix_aggregator exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(handle) = radix_gateway_handle {
                            if let Ok(e) = handle.await {
                                error!("radix_gateway exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    Ok(Err(e)) = nats_server_handle => {
                        error!("nats_server exited: {:?}", e);
                    }
                    Ok(Err(e)) = core_handle => {
                        error!("core exited: {:?}", e);
                    }
                    else => {
                        info!("enclave shutdown cleanly. goodbye.");
                    }
                }
            });

            tokio::select! {
                () = shutdown_token.cancelled() => info!("shutdown command received. shutting down..."),
                _ = critical_tasks => error!("critical task failed - exiting")
            }

            // Shutdown services in reverse order
            core.lock().await.shutdown().await;
            nats_server.lock().await.shutdown().await;

            if let Some(gateway) = &radix_gateway {
                let _ = gateway.lock().await.shutdown().await;
            }

            if let Some(aggregator) = &radix_aggregator {
                let _ = aggregator.lock().await.shutdown().await;
            }

            if let Some(postgres) = &postgres {
                let _ = postgres.lock().await.shutdown().await;
            }

            if let Some(mainnet_node) = &radix_mainnet_node_option {
                let _ = mainnet_node.lock().await.shutdown().await;
            }

            if let Some(stokenet_node) = &radix_stokenet_node_option {
                let _ = stokenet_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_mainnet_reth_node) = &ethereum_mainnet_reth_node_option {
                ethereum_mainnet_reth_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_mainnet_lighthouse_node) = &ethereum_mainnet_lighthouse_node_option {
                ethereum_mainnet_lighthouse_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_holesky_reth_node) = &ethereum_holesky_reth_node_option {
                ethereum_holesky_reth_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_holesky_lighthouse_node) = &ethereum_holesky_lighthouse_node_option {
                ethereum_holesky_lighthouse_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_sepolia_reth_node) = &ethereum_sepolia_reth_node_option {
                ethereum_sepolia_reth_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_sepolia_lighthouse_node) = &ethereum_sepolia_lighthouse_node_option {
                ethereum_sepolia_lighthouse_node.lock().await.shutdown().await;
            }

            if let Some(bitcoin_node) = &bitcoin_node_option {
                let _ = bitcoin_node.lock().await.shutdown().await;
            }
        });

        self.task_tracker.close();

        let enclave = LocalNode::new(
            self.attestor.clone(),
            node_services,
            self.shutdown_token.clone(),
            self.task_tracker.clone(),
        );

        Ok(enclave)
    }

    async fn unwind_services(self) {
        // shutdown in reverse order

        if let Some(core) = self.core {
            core.shutdown().await;
        }

        if let Some(nats_server) = self.nats_server {
            nats_server.shutdown().await;
        }

        if let Some(mut radix_gateway) = self.radix_gateway {
            let _ = radix_gateway.shutdown().await;
        }

        if let Some(mut radix_aggregator) = self.radix_aggregator {
            let _ = radix_aggregator.shutdown().await;
        }

        if let Some(mut postgres) = self.postgres {
            let _ = postgres.shutdown().await;
        }

        if let Some(ethereum_sepolia_lighthouse_node) = self.ethereum_sepolia_lighthouse_node {
            ethereum_sepolia_lighthouse_node.shutdown().await;
        }

        if let Some(ethereum_sepolia_reth_node) = self.ethereum_sepolia_reth_node {
            ethereum_sepolia_reth_node.shutdown().await;
        }

        if let Some(ethereum_holesky_lighthouse_node) = self.ethereum_holesky_lighthouse_node {
            ethereum_holesky_lighthouse_node.shutdown().await;
        }

        if let Some(ethereum_holesky_reth_node) = self.ethereum_holesky_reth_node {
            ethereum_holesky_reth_node.shutdown().await;
        }

        if let Some(ethereum_mainnet_lighthouse_node) = self.ethereum_mainnet_lighthouse_node {
            ethereum_mainnet_lighthouse_node.shutdown().await;
        }

        if let Some(ethereum_mainnet_reth_node) = self.ethereum_mainnet_reth_node {
            ethereum_mainnet_reth_node.shutdown().await;
        }

        if let Some(mut bitcoin_node) = self.bitcoin_node {
            let _ = bitcoin_node.shutdown().await;
        }

        if let Some(mut radix_mainnet_node) = self.radix_mainnet_node {
            let _ = radix_mainnet_node.shutdown().await;
        }

        if let Some(mut radix_stokenet_node) = self.radix_stokenet_node {
            let _ = radix_stokenet_node.shutdown().await;
        }

        if let Some(light_core) = self.light_core {
            light_core.shutdown().await;
        }
    }

    async fn start_network_cluster(&mut self) -> Result<()> {
        let governance =
            MockGovernance::from_topology_file(self.args.topology_file.clone(), vec![])
                .map_err(|e| Error::Io(format!("Failed to load topology: {}", e)))?;

        let network = ProvenNetwork::new(ProvenNetworkOptions {
            governance: governance.clone(),
            attestor: self.attestor.clone(),
            nats_cluster_port: self.args.nats_cluster_port,
            private_key_hex: self.args.node_key.clone(),
        })
        .await?;

        // Check /etc/hosts to ensure the node's FQDN is properly configured
        check_hostname_resolution(network.fqdn().await?.as_str()).await?;

        let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.args.port));
        let http_server = InsecureHttpServer::new(http_sock_addr);

        let light_core = LightCore::new(LightCoreOptions {
            network: network.clone(),
        });
        let _light_core_handle = light_core.start(http_server).await?;

        self.governance = Some(governance);
        self.network = Some(network);
        self.light_core = Some(light_core);

        // TODO: Wait for at least one other node to be started so NATS can boot in cluster mode
        // Just sleep to simulate for now
        tokio::time::sleep(Duration::from_secs(20)).await;

        Ok(())
    }

    async fn start_radix_node(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before radix node step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::RadixMainnet)
        {
            let mut radix_mainnet_node = RadixNode::new(RadixNodeOptions {
                config_dir: "/tmp/radix-node-mainnet".to_string(),
                host_ip: self.external_ip.to_string(),
                http_port: self.args.radix_mainnet_http_port,
                network_definition: NetworkDefinition::mainnet(),
                p2p_port: self.args.radix_mainnet_p2p_port,
                store_dir: self
                    .args
                    .radix_mainnet_store_dir
                    .to_string_lossy()
                    .to_string(),
            });

            let radix_mainnet_node_handle = radix_mainnet_node.start().await?;

            self.radix_mainnet_node = Some(radix_mainnet_node);
            self.radix_mainnet_node_handle = Some(radix_mainnet_node_handle);

            info!("radix mainnet node started");
        }

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::RadixStokenet)
        {
            let mut radix_stokenet_node = RadixNode::new(RadixNodeOptions {
                config_dir: "/tmp/radix-node-stokenet".to_string(),
                host_ip: self.external_ip.to_string(),
                http_port: self.args.radix_stokenet_http_port,
                network_definition: NetworkDefinition::stokenet(),
                p2p_port: self.args.radix_stokenet_p2p_port,
                store_dir: self
                    .args
                    .radix_stokenet_store_dir
                    .to_string_lossy()
                    .to_string(),
            });

            let radix_stokenet_node_handle = radix_stokenet_node.start().await?;

            self.radix_stokenet_node = Some(radix_stokenet_node);
            self.radix_stokenet_node_handle = Some(radix_stokenet_node_handle);

            info!("radix stokenet node started");
        }

        Ok(())
    }

    async fn start_ethereum_holesky_node(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before ethereum nodes step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::EthereumHolesky)
        {
            // Start Reth execution client
            let ethereum_reth_node = RethNode::new(RethNodeOptions {
                discovery_addr: self.args.ethereum_holesky_execution_discovery_addr,
                http_addr: self.args.ethereum_holesky_execution_http_addr,
                metrics_addr: self.args.ethereum_holesky_execution_metrics_addr,
                network: RethNetwork::Holesky,
                rpc_addr: self.args.ethereum_holesky_execution_rpc_addr,
                store_dir: self.args.ethereum_holesky_execution_store_dir.clone(),
            });

            let ethereum_reth_node_handle = ethereum_reth_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Reth node: {}", e)))?;

            self.ethereum_holesky_reth_node = Some(ethereum_reth_node);
            self.ethereum_holesky_reth_node_handle = Some(ethereum_reth_node_handle);

            info!("ethereum reth node (holesky) started");

            // Start Lighthouse consensus client
            let ethereum_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
                execution_rpc_addr: self.args.ethereum_holesky_execution_rpc_addr,
                host_ip: self.external_ip.to_string(),
                http_addr: self.args.ethereum_holesky_consensus_http_addr,
                metrics_addr: self.args.ethereum_holesky_consensus_metrics_addr,
                network: LighthouseNetwork::Holesky,
                p2p_addr: self.args.ethereum_holesky_consensus_p2p_addr,
                store_dir: self.args.ethereum_holesky_consensus_store_dir.clone(),
            });

            let ethereum_lighthouse_node_handle = ethereum_lighthouse_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Lighthouse node: {}", e)))?;

            self.ethereum_holesky_lighthouse_node = Some(ethereum_lighthouse_node);
            self.ethereum_holesky_lighthouse_node_handle = Some(ethereum_lighthouse_node_handle);

            info!("ethereum lighthouse node (holesky) started");
        }

        Ok(())
    }

    async fn start_ethereum_mainnet_node(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before ethereum nodes step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::EthereumMainnet)
        {
            // Start Reth execution client
            let ethereum_reth_node = RethNode::new(RethNodeOptions {
                discovery_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 30303),
                http_addr: self.args.ethereum_mainnet_execution_http_addr,
                metrics_addr: self.args.ethereum_mainnet_execution_metrics_addr,
                network: RethNetwork::Mainnet,
                rpc_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8545),
                store_dir: self.args.ethereum_mainnet_execution_store_dir.clone(),
            });

            let ethereum_reth_node_handle = ethereum_reth_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Reth node: {}", e)))?;

            self.ethereum_mainnet_reth_node = Some(ethereum_reth_node);
            self.ethereum_mainnet_reth_node_handle = Some(ethereum_reth_node_handle);

            info!("ethereum reth node (mainnet) started");

            // Start Lighthouse consensus client
            let ethereum_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
                execution_rpc_addr: self.args.ethereum_mainnet_execution_rpc_addr,
                host_ip: self.external_ip.to_string(),
                http_addr: self.args.ethereum_mainnet_consensus_http_addr,
                metrics_addr: self.args.ethereum_mainnet_consensus_metrics_addr,
                network: LighthouseNetwork::Mainnet,
                p2p_addr: self.args.ethereum_mainnet_consensus_p2p_addr,
                store_dir: self.args.ethereum_mainnet_consensus_store_dir.clone(),
            });

            let ethereum_lighthouse_node_handle = ethereum_lighthouse_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Lighthouse node: {}", e)))?;

            self.ethereum_mainnet_lighthouse_node = Some(ethereum_lighthouse_node);
            self.ethereum_mainnet_lighthouse_node_handle = Some(ethereum_lighthouse_node_handle);

            info!("ethereum lighthouse node (mainnet) started");
        }

        Ok(())
    }

    async fn start_ethereum_sepolia_node(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before ethereum nodes step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::EthereumSepolia)
        {
            // Start Reth execution client
            let ethereum_reth_node = RethNode::new(RethNodeOptions {
                discovery_addr: self.args.ethereum_sepolia_execution_discovery_addr,
                http_addr: self.args.ethereum_sepolia_execution_http_addr,
                metrics_addr: self.args.ethereum_sepolia_execution_metrics_addr,
                network: RethNetwork::Sepolia,
                rpc_addr: self.args.ethereum_sepolia_execution_rpc_addr,
                store_dir: self.args.ethereum_sepolia_execution_store_dir.clone(),
            });

            let ethereum_reth_node_handle = ethereum_reth_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Reth node: {}", e)))?;

            self.ethereum_sepolia_reth_node = Some(ethereum_reth_node);
            self.ethereum_sepolia_reth_node_handle = Some(ethereum_reth_node_handle);

            info!("ethereum reth node (sepolia) started");

            // Start Lighthouse consensus client
            let ethereum_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
                execution_rpc_addr: self.args.ethereum_sepolia_execution_rpc_addr,
                host_ip: self.external_ip.to_string(),
                http_addr: self.args.ethereum_sepolia_consensus_http_addr,
                metrics_addr: self.args.ethereum_sepolia_consensus_metrics_addr,
                network: LighthouseNetwork::Sepolia,
                p2p_addr: self.args.ethereum_sepolia_consensus_p2p_addr,
                store_dir: self.args.ethereum_sepolia_consensus_store_dir.clone(),
            });

            let ethereum_lighthouse_node_handle = ethereum_lighthouse_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Lighthouse node: {}", e)))?;

            self.ethereum_sepolia_lighthouse_node = Some(ethereum_lighthouse_node);
            self.ethereum_sepolia_lighthouse_node_handle = Some(ethereum_lighthouse_node_handle);

            info!("ethereum lighthouse node (sepolia) started");
        }

        Ok(())
    }

    async fn start_bitcoin_node(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before bitcoin node step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::BitcoinTestnet)
        {
            // Start Bitcoin testnet node
            let mut bitcoin_node = BitcoinNode::new(BitcoinNodeOptions {
                network: BitcoinNetwork::Testnet,
                store_dir: self
                    .args
                    .bitcoin_testnet_store_dir
                    .to_string_lossy()
                    .to_string(),
                rpc_port: None,
            });

            let bitcoin_node_handle = bitcoin_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Bitcoin testnet node: {}", e)))?;

            self.bitcoin_node = Some(bitcoin_node);
            self.bitcoin_node_handle = Some(bitcoin_node_handle);

            info!("bitcoin testnet node started");
        }

        Ok(())
    }

    async fn start_postgres(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before postgres step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::RadixMainnet)
            || network
                .specializations()
                .await?
                .contains(&NodeSpecialization::RadixStokenet)
        {
            let mut postgres = Postgres::new(PostgresOptions {
                password: POSTGRES_PASSWORD.to_string(),
                port: self.args.postgres_port,
                username: POSTGRES_USERNAME.to_string(),
                skip_vacuum: self.args.skip_vacuum,
                store_dir: self.args.postgres_store_dir.to_string_lossy().to_string(),
            });

            let postgres_handle = postgres.start().await?;

            self.postgres = Some(postgres);
            self.postgres_handle = Some(postgres_handle);

            info!("postgres for radix stokenet started");
        }

        Ok(())
    }

    async fn start_radix_aggregator(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before radix aggregator step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::RadixStokenet)
        {
            let postgres = self.postgres.as_ref().unwrap_or_else(|| {
                panic!("postgres not set before radix aggregator step");
            });

            let radix_stokenet_node = self.radix_stokenet_node.as_ref().unwrap_or_else(|| {
                panic!("radix node not set before radix aggregator step");
            });

            let mut radix_aggregator = RadixAggregator::new(RadixAggregatorOptions {
                postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
                postgres_ip_address: postgres.ip_address().to_string(),
                postgres_password: POSTGRES_PASSWORD.to_string(),
                postgres_port: postgres.port(),
                postgres_username: POSTGRES_USERNAME.to_string(),
                radix_node_ip_address: radix_stokenet_node.ip_address().to_string(),
                radix_node_port: radix_stokenet_node.http_port(),
            });

            let radix_aggregator_handle = radix_aggregator.start().await?;

            self.radix_aggregator = Some(radix_aggregator);
            self.radix_aggregator_handle = Some(radix_aggregator_handle);

            info!("radix-aggregator for radix stokenet started");
        }

        Ok(())
    }

    async fn start_radix_gateway(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before radix gateway step");
        });

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::RadixStokenet)
        {
            let postgres = self.postgres.as_ref().unwrap_or_else(|| {
                panic!("postgres not set before radix gateway step");
            });

            let radix_stokenet_node = self.radix_stokenet_node.as_ref().unwrap_or_else(|| {
                panic!("radix node not set before radix gateway step");
            });

            let mut radix_gateway = RadixGateway::new(RadixGatewayOptions {
                postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
                postgres_ip_address: postgres.ip_address().to_string(),
                postgres_password: POSTGRES_PASSWORD.to_string(),
                postgres_port: postgres.port(),
                postgres_username: POSTGRES_USERNAME.to_string(),
                radix_node_ip_address: radix_stokenet_node.ip_address().to_string(),
                radix_node_port: radix_stokenet_node.http_port(),
            });

            let radix_gateway_handle = radix_gateway.start().await?;

            self.radix_gateway = Some(radix_gateway);
            self.radix_gateway_handle = Some(radix_gateway_handle);

            info!("radix-gateway for radix stokenet started");
        }

        Ok(())
    }

    async fn start_nats_server(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before nats server step");
        });

        let nats_server = NatsServer::new(NatsServerOptions {
            client_listen_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.args.nats_client_port),
            debug: self.args.testnet,
            network: network.clone(),
            server_name: network.fqdn().await?,
            store_dir: "/var/lib/nats/nats".to_string(),
        });

        let nats_server_handle = nats_server.start().await?;
        let nats_client = nats_server.build_client().await?;

        self.nats_server = Some(nats_server);
        self.nats_server_handle = Some(nats_server_handle);
        self.nats_client = Some(nats_client);

        info!("nats server started");

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn start_core(&mut self) -> Result<()> {
        let nats_client = self.nats_client.as_ref().unwrap_or_else(|| {
            panic!("nats client not fetched before core");
        });

        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before core");
        });

        let light_core = self.light_core.as_ref().unwrap_or_else(|| {
            panic!("light core not fetched before core");
        });

        let challenge_store = NatsStore2::new(NatsStoreOptions {
            bucket: "challenges".to_string(),
            client: nats_client.clone(),
            max_age: Duration::from_secs(5 * 60),
            persist: false,
        });

        let sessions_store = NatsStore1::new(NatsStoreOptions {
            bucket: "sessions".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            persist: true,
        });

        let session_manager = SessionManager::new(SessionManagerOptions {
            attestor: self.attestor.clone(),
            challenge_store,
            sessions_store,
            radix_gateway_origin: GATEWAY_URL,
            radix_network_definition: &NetworkDefinition::stokenet(),
        });

        let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.args.port));
        let http_server = InsecureHttpServer::new(http_sock_addr);

        let application_manager = ApplicationManager::new(
            NatsStore::new(NatsStoreOptions {
                bucket: "APPLICATION_MANAGER_KV".to_string(),
                client: nats_client.clone(),
                max_age: Duration::ZERO,
                persist: true,
            }),
            StreamedSqlStore1::new(
                NatsStream1::new(
                    "APPLICATION_MANAGER_SQL",
                    NatsStreamOptions {
                        client: nats_client.clone(),
                    },
                ),
                NatsServiceOptions {
                    client: nats_client.clone(),
                    durable_name: None,
                    jetstream_context: async_nats::jetstream::new(nats_client.clone()),
                },
                NatsClientOptions {
                    client: nats_client.clone(),
                },
                FsStore1::new("/tmp/proven/application_manager_snapshots"),
            ),
        );

        let application_store = NatsStore2::new(NatsStoreOptions {
            bucket: "APPLICATION_KV".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            persist: true,
        });

        let application_sql_store = StreamedSqlStore2::new(
            NatsStream2::new(
                "APPLICATION_SQL",
                NatsStreamOptions {
                    client: nats_client.clone(),
                },
            ),
            NatsServiceOptions {
                client: nats_client.clone(),
                durable_name: None,
                jetstream_context: async_nats::jetstream::new(nats_client.clone()),
            },
            NatsClientOptions {
                client: nats_client.clone(),
            },
            FsStore2::new("/tmp/proven/application_snapshots"),
        );

        let personal_store = NatsStore3::new(NatsStoreOptions {
            bucket: "PERSONAL_KV".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            persist: true,
        });

        let personal_sql_store = StreamedSqlStore3::new(
            NatsStream3::new(
                "PERSONAL_SQL",
                NatsStreamOptions {
                    client: nats_client.clone(),
                },
            ),
            NatsServiceOptions {
                client: nats_client.clone(),
                durable_name: None,
                jetstream_context: async_nats::jetstream::new(nats_client.clone()),
            },
            NatsClientOptions {
                client: nats_client.clone(),
            },
            FsStore3::new("/tmp/proven/personal_snapshots"),
        );

        let nft_store = NatsStore3::new(NatsStoreOptions {
            bucket: "NFT_KV".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            persist: true,
        });

        let nft_sql_store = StreamedSqlStore3::new(
            NatsStream3::new(
                "NFT_SQL",
                NatsStreamOptions {
                    client: nats_client.clone(),
                },
            ),
            NatsServiceOptions {
                client: nats_client.clone(),
                durable_name: None,
                jetstream_context: async_nats::jetstream::new(nats_client.clone()),
            },
            NatsClientOptions {
                client: nats_client.clone(),
            },
            FsStore3::new("/tmp/proven/nft_snapshots"),
        );

        let file_system_store = FsStore::new("/tmp/proven/file_systems");

        let radix_nft_verifier = GatewayRadixNftVerifier::new(GATEWAY_URL);

        let runtime_pool_manager = RuntimePoolManager::new(RuntimePoolManagerOptions {
            application_sql_store,
            application_store,
            file_system_store,
            max_workers: 10,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_gateway_origin: GATEWAY_URL.to_string(),
            radix_network_definition: NetworkDefinition::stokenet(),
            radix_nft_verifier,
        })
        .await;

        let core = Core::new(CoreOptions {
            application_manager,
            attestor: self.attestor.clone(),
            network: network.clone(),
            runtime_pool_manager,
            session_manager,
        });

        // Shutdown the light core and free the port before starting the full core
        light_core.shutdown().await;
        self.light_core = None;

        let core_handle = core.start(http_server).await?;

        self.core = Some(core);
        self.core_handle = Some(core_handle);

        info!("core started");

        Ok(())
    }
}
