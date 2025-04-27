use super::error::{Error, Result};
use crate::Args;
use crate::hosts::check_hostname_resolution;
use crate::net::fetch_external_ip;
use crate::node::{LocalNode, LocalNodeCore, Services};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use axum::Router;
use axum::routing::any;
use ed25519_dalek::SigningKey;
use http::StatusCode;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation_mock::MockAttestor;
use proven_bitcoin_core::{BitcoinNetwork, BitcoinNode, BitcoinNodeOptions};
use proven_bootable::Bootable;
use proven_core::{Core, CoreOptions, LightCore, LightCoreOptions};
use proven_ethereum_lighthouse::{
    EthereumNetwork as LighthouseNetwork, LighthouseNode, LighthouseNodeOptions,
};
use proven_ethereum_reth::{EthereumNetwork as RethNetwork, RethNode, RethNodeOptions};
use proven_governance::NodeSpecialization;
use proven_governance_mock::MockGovernance;
use proven_http_insecure::InsecureHttpServer;
use proven_identity::{IdentityManagement, IdentityManager, IdentityManagerOptions};
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
use proven_sql_streamed::{StreamedSqlStore1, StreamedSqlStore2, StreamedSqlStore3};
use proven_store_fs::{FsStore, FsStore1, FsStore2, FsStore3};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3, NatsStoreOptions};
use radix_common::prelude::NetworkDefinition;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
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
    num_replicas: usize,
    governance: Option<MockGovernance>,
    network: Option<ProvenNetwork<MockGovernance, MockAttestor>>,
    light_core: Option<LightCore<MockAttestor, MockGovernance, InsecureHttpServer>>,

    radix_mainnet_node: Option<RadixNode>,

    radix_stokenet_node: Option<RadixNode>,

    ethereum_mainnet_reth_node: Option<proven_ethereum_reth::RethNode>,
    ethereum_mainnet_lighthouse_node: Option<proven_ethereum_lighthouse::LighthouseNode>,

    ethereum_holesky_reth_node: Option<proven_ethereum_reth::RethNode>,
    ethereum_holesky_lighthouse_node: Option<proven_ethereum_lighthouse::LighthouseNode>,

    ethereum_sepolia_reth_node: Option<proven_ethereum_reth::RethNode>,
    ethereum_sepolia_lighthouse_node: Option<proven_ethereum_lighthouse::LighthouseNode>,

    bitcoin_node: Option<BitcoinNode>,

    postgres: Option<Postgres>,

    radix_aggregator: Option<RadixAggregator>,

    radix_gateway: Option<RadixGateway>,

    nats_client: Option<NatsClient>,
    nats_server: Option<NatsServer<MockGovernance, MockAttestor>>,

    core: Option<LocalNodeCore>,

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
            attestor: MockAttestor::new(),
            external_ip,

            num_replicas: 3,
            governance: None,
            network: None,
            light_core: None,

            radix_mainnet_node: None,

            radix_stokenet_node: None,

            ethereum_mainnet_reth_node: None,
            ethereum_mainnet_lighthouse_node: None,

            ethereum_holesky_reth_node: None,
            ethereum_holesky_lighthouse_node: None,

            ethereum_sepolia_reth_node: None,
            ethereum_sepolia_lighthouse_node: None,

            bitcoin_node: None,

            postgres: None,

            radix_aggregator: None,

            radix_gateway: None,

            nats_client: None,
            nats_server: None,

            core: None,

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

        if let Err(e) = self.start_nats_server().await {
            error!("failed to start nats server: {:?}", e);
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

        if let Err(e) = self.start_core().await {
            error!("failed to start core: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

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
            nats_server: nats_server.clone(),
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
            core: core.clone(),
        };

        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            // Tasks that must be running for the enclave to function
            let critical_tasks = tokio::spawn(async move {
                tokio::select! {
                    _ = async {
                        if let Some(nats_server) = self.nats_server {
                            nats_server.wait().await;
                            error!("nats_server exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(radix_mainnet_node) = self.radix_mainnet_node {
                            radix_mainnet_node.wait().await;
                            error!("radix mainnet node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(radix_stokenet_node) = self.radix_stokenet_node {
                            radix_stokenet_node.wait().await;
                            error!("radix stokenet node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(ethereum_mainnet_reth_node) = self.ethereum_mainnet_reth_node {
                            ethereum_mainnet_reth_node.wait().await;
                            error!("ethereum mainnet reth node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(ethereum_mainnet_lighthouse_node) = self.ethereum_mainnet_lighthouse_node {
                            ethereum_mainnet_lighthouse_node.wait().await;
                            error!("ethereum mainnet lighthouse node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(ethereum_holesky_reth_node) = self.ethereum_holesky_reth_node {
                            ethereum_holesky_reth_node.wait().await;
                            error!("ethereum holesky reth node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(ethereum_holesky_lighthouse_node) = self.ethereum_holesky_lighthouse_node {
                            ethereum_holesky_lighthouse_node.wait().await;
                            error!("ethereum holesky lighthouse node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(ethereum_sepolia_reth_node) = self.ethereum_sepolia_reth_node {
                            ethereum_sepolia_reth_node.wait().await;
                            error!("ethereum sepolia reth node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(ethereum_sepolia_lighthouse_node) = self.ethereum_sepolia_lighthouse_node {
                            ethereum_sepolia_lighthouse_node.wait().await;
                            error!("ethereum sepolia lighthouse node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(bitcoin_node) = self.bitcoin_node {
                            bitcoin_node.wait().await;
                            error!("bitcoin node exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(postgres) = self.postgres {
                            postgres.wait().await;
                            error!("postgres exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(radix_aggregator) = self.radix_aggregator {
                            radix_aggregator.wait().await;
                            error!("radix_aggregator exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(radix_gateway) = self.radix_gateway {
                            radix_gateway.wait().await;
                            error!("radix_gateway exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async {
                        if let Some(core) = self.core {
                            core.wait().await;
                            error!("core exited");
                            return;
                        }
                        std::future::pending::<()>().await
                    } => {},
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
            let _ = core.lock().await.shutdown().await;
            let _ = nats_server.lock().await.shutdown().await;

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
                let _ = ethereum_mainnet_reth_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_mainnet_lighthouse_node) = &ethereum_mainnet_lighthouse_node_option {
                let _ = ethereum_mainnet_lighthouse_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_holesky_reth_node) = &ethereum_holesky_reth_node_option {
                let _ = ethereum_holesky_reth_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_holesky_lighthouse_node) = &ethereum_holesky_lighthouse_node_option {
                let _ = ethereum_holesky_lighthouse_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_sepolia_reth_node) = &ethereum_sepolia_reth_node_option {
                let _ = ethereum_sepolia_reth_node.lock().await.shutdown().await;
            }

            if let Some(ethereum_sepolia_lighthouse_node) = &ethereum_sepolia_lighthouse_node_option {
                let _ = ethereum_sepolia_lighthouse_node.lock().await.shutdown().await;
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
            let _ = core.shutdown().await;
        }

        if let Some(radix_gateway) = self.radix_gateway {
            let _ = radix_gateway.shutdown().await;
        }

        if let Some(radix_aggregator) = self.radix_aggregator {
            let _ = radix_aggregator.shutdown().await;
        }

        if let Some(postgres) = self.postgres {
            let _ = postgres.shutdown().await;
        }

        if let Some(ethereum_sepolia_lighthouse_node) = self.ethereum_sepolia_lighthouse_node {
            let _ = ethereum_sepolia_lighthouse_node.shutdown().await;
        }

        if let Some(ethereum_sepolia_reth_node) = self.ethereum_sepolia_reth_node {
            let _ = ethereum_sepolia_reth_node.shutdown().await;
        }

        if let Some(ethereum_holesky_lighthouse_node) = self.ethereum_holesky_lighthouse_node {
            let _ = ethereum_holesky_lighthouse_node.shutdown().await;
        }

        if let Some(ethereum_holesky_reth_node) = self.ethereum_holesky_reth_node {
            let _ = ethereum_holesky_reth_node.shutdown().await;
        }

        if let Some(ethereum_mainnet_lighthouse_node) = self.ethereum_mainnet_lighthouse_node {
            let _ = ethereum_mainnet_lighthouse_node.shutdown().await;
        }

        if let Some(ethereum_mainnet_reth_node) = self.ethereum_mainnet_reth_node {
            let _ = ethereum_mainnet_reth_node.shutdown().await;
        }

        if let Some(bitcoin_node) = self.bitcoin_node {
            let _ = bitcoin_node.shutdown().await;
        }

        if let Some(radix_mainnet_node) = self.radix_mainnet_node {
            let _ = radix_mainnet_node.shutdown().await;
        }

        if let Some(radix_stokenet_node) = self.radix_stokenet_node {
            let _ = radix_stokenet_node.shutdown().await;
        }

        if let Some(nats_server) = self.nats_server {
            let _ = nats_server.shutdown().await;
        }

        if let Some(light_core) = self.light_core {
            let _ = light_core.shutdown().await;
        }
    }

    async fn start_network_cluster(&mut self) -> Result<()> {
        // Parse the private key and calculate public key
        let private_key_bytes = hex::decode(self.args.node_key.trim()).map_err(|e| {
            Error::PrivateKey(format!("Failed to decode private key as hex: {}", e))
        })?;

        // We need exactly 32 bytes for ed25519 private key
        let private_key = SigningKey::try_from(private_key_bytes.as_slice()).map_err(|_| {
            Error::PrivateKey("Failed to create SigningKey: invalid key length".to_string())
        })?;

        let governance = match self.args.topology_file {
            Some(ref topology_file) => {
                info!(
                    "using replication factor 3 with topology from file: {}",
                    topology_file.display()
                );
                MockGovernance::from_topology_file(topology_file, vec![])
                    .map_err(|e| Error::Io(format!("Failed to load topology: {}", e)))?
            }
            None => {
                info!("using replication factor 1 as no topology file provided");
                self.num_replicas = 1;
                MockGovernance::for_single_node(
                    format!("http://localhost:{}", self.args.port),
                    private_key.clone(),
                )
            }
        };

        let network = ProvenNetwork::new(ProvenNetworkOptions {
            governance: governance.clone(),
            attestor: self.attestor.clone(),
            nats_cluster_port: self.args.nats_cluster_port,
            private_key,
        })
        .await?;

        let peer_count = network.get_peers().await?.len();

        // Check /etc/hosts to ensure the node's FQDN is properly configured
        check_hostname_resolution(network.fqdn().await?.as_str()).await?;

        let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.args.port));
        let http_server = InsecureHttpServer::new(
            http_sock_addr,
            Router::new()
                .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
                .layer(CorsLayer::very_permissive()),
        );

        let light_core = LightCore::new(LightCoreOptions {
            http_server,
            network: network.clone(),
        });
        light_core.start().await?;

        self.governance = Some(governance);
        self.network = Some(network);
        self.light_core = Some(light_core);

        if peer_count > 0 {
            // TODO: Wait for at least one other node to be started so NATS can boot in cluster mode
            // Just sleep to simulate for now
            tokio::time::sleep(Duration::from_secs(20)).await;
        }

        Ok(())
    }

    async fn start_nats_server(&mut self) -> Result<()> {
        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before nats server step");
        });

        let nats_server = NatsServer::new(NatsServerOptions {
            bin_dir: self.args.nats_bin_dir.clone(),
            client_port: self.args.nats_client_port,
            config_dir: PathBuf::from("/tmp/nats-config"),
            debug: self.args.testnet,
            http_port: self.args.nats_http_port,
            network: network.clone(),
            server_name: network.fqdn().await?,
            store_dir: self.args.nats_store_dir.clone(),
        })?;

        nats_server.start().await?;
        let nats_client = nats_server.build_client().await?;

        self.nats_server = Some(nats_server);
        self.nats_client = Some(nats_client);

        info!("nats server started");

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
            let radix_mainnet_node = RadixNode::new(RadixNodeOptions {
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

            radix_mainnet_node.start().await?;

            self.radix_mainnet_node = Some(radix_mainnet_node);

            info!("radix mainnet node started");
        }

        if network
            .specializations()
            .await?
            .contains(&NodeSpecialization::RadixStokenet)
        {
            let radix_stokenet_node = RadixNode::new(RadixNodeOptions {
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

            radix_stokenet_node.start().await?;

            self.radix_stokenet_node = Some(radix_stokenet_node);

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
                discovery_port: self.args.ethereum_holesky_execution_discovery_port,
                http_port: self.args.ethereum_holesky_execution_http_port,
                metrics_port: self.args.ethereum_holesky_execution_metrics_port,
                network: RethNetwork::Holesky,
                rpc_port: self.args.ethereum_holesky_execution_rpc_port,
                store_dir: self.args.ethereum_holesky_execution_store_dir.clone(),
            });

            ethereum_reth_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Reth node: {}", e)))?;

            let execution_rpc_jwt_hex = ethereum_reth_node.jwt_hex().await?;
            let execution_rpc_ip_address = ethereum_reth_node.ip_address().await.to_string();

            self.ethereum_holesky_reth_node = Some(ethereum_reth_node);

            info!("ethereum reth node (holesky) started");

            // Start Lighthouse consensus client
            let ethereum_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
                execution_rpc_ip_address,
                execution_rpc_jwt_hex,
                execution_rpc_port: self.args.ethereum_holesky_execution_rpc_port,
                host_ip: self.external_ip.to_string(),
                http_port: self.args.ethereum_holesky_consensus_http_port,
                metrics_port: self.args.ethereum_holesky_consensus_metrics_port,
                network: LighthouseNetwork::Holesky,
                p2p_port: self.args.ethereum_holesky_consensus_p2p_port,
                store_dir: self.args.ethereum_holesky_consensus_store_dir.clone(),
            });

            ethereum_lighthouse_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Lighthouse node: {}", e)))?;

            self.ethereum_holesky_lighthouse_node = Some(ethereum_lighthouse_node);

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
                discovery_port: self.args.ethereum_mainnet_execution_discovery_port,
                http_port: self.args.ethereum_mainnet_execution_http_port,
                metrics_port: self.args.ethereum_mainnet_execution_metrics_port,
                network: RethNetwork::Mainnet,
                rpc_port: self.args.ethereum_mainnet_execution_rpc_port,
                store_dir: self.args.ethereum_mainnet_execution_store_dir.clone(),
            });

            ethereum_reth_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Reth node: {}", e)))?;

            let execution_rpc_ip_address = ethereum_reth_node.ip_address().await.to_string();
            let execution_rpc_jwt_hex = ethereum_reth_node.jwt_hex().await?;

            self.ethereum_mainnet_reth_node = Some(ethereum_reth_node);

            info!("ethereum reth node (mainnet) started");

            // Start Lighthouse consensus client
            let ethereum_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
                execution_rpc_ip_address,
                execution_rpc_jwt_hex,
                execution_rpc_port: self.args.ethereum_mainnet_execution_rpc_port,
                host_ip: self.external_ip.to_string(),
                http_port: self.args.ethereum_mainnet_consensus_http_port,
                metrics_port: self.args.ethereum_mainnet_consensus_metrics_port,
                network: LighthouseNetwork::Mainnet,
                p2p_port: self.args.ethereum_mainnet_consensus_p2p_port,
                store_dir: self.args.ethereum_mainnet_consensus_store_dir.clone(),
            });

            ethereum_lighthouse_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Lighthouse node: {}", e)))?;

            self.ethereum_mainnet_lighthouse_node = Some(ethereum_lighthouse_node);

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
                discovery_port: self.args.ethereum_sepolia_execution_discovery_port,
                http_port: self.args.ethereum_sepolia_execution_http_port,
                metrics_port: self.args.ethereum_sepolia_execution_metrics_port,
                network: RethNetwork::Sepolia,
                rpc_port: self.args.ethereum_sepolia_execution_rpc_port,
                store_dir: self.args.ethereum_sepolia_execution_store_dir.clone(),
            });

            ethereum_reth_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Reth node: {}", e)))?;

            let execution_rpc_ip_address = ethereum_reth_node.ip_address().await.to_string();
            let execution_rpc_jwt_hex = ethereum_reth_node.jwt_hex().await?;
            self.ethereum_sepolia_reth_node = Some(ethereum_reth_node);

            info!("ethereum reth node (sepolia) started");

            // Start Lighthouse consensus client
            let ethereum_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
                execution_rpc_ip_address,
                execution_rpc_jwt_hex,
                execution_rpc_port: self.args.ethereum_sepolia_execution_rpc_port,
                host_ip: self.external_ip.to_string(),
                http_port: self.args.ethereum_sepolia_consensus_http_port,
                metrics_port: self.args.ethereum_sepolia_consensus_metrics_port,
                network: LighthouseNetwork::Sepolia,
                p2p_port: self.args.ethereum_sepolia_consensus_p2p_port,
                store_dir: self.args.ethereum_sepolia_consensus_store_dir.clone(),
            });

            ethereum_lighthouse_node
                .start()
                .await
                .map_err(|e| Error::Io(format!("Failed to start Lighthouse node: {}", e)))?;

            self.ethereum_sepolia_lighthouse_node = Some(ethereum_lighthouse_node);

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
            let bitcoin_node = BitcoinNode::new(BitcoinNodeOptions {
                network: BitcoinNetwork::Testnet,
                store_dir: self
                    .args
                    .bitcoin_testnet_store_dir
                    .to_string_lossy()
                    .to_string(),
                rpc_port: None,
            });

            bitcoin_node.start().await?;

            self.bitcoin_node = Some(bitcoin_node);

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
            let postgres = Postgres::new(PostgresOptions {
                password: POSTGRES_PASSWORD.to_string(),
                port: self.args.postgres_port,
                username: POSTGRES_USERNAME.to_string(),
                skip_vacuum: self.args.skip_vacuum,
                store_dir: self.args.postgres_store_dir.to_string_lossy().to_string(),
            });

            postgres.start().await?;

            self.postgres = Some(postgres);

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

            let radix_aggregator = RadixAggregator::new(RadixAggregatorOptions {
                postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
                postgres_ip_address: postgres.ip_address().await.to_string(),
                postgres_password: POSTGRES_PASSWORD.to_string(),
                postgres_port: postgres.port(),
                postgres_username: POSTGRES_USERNAME.to_string(),
                radix_node_ip_address: radix_stokenet_node.ip_address().await.to_string(),
                radix_node_port: radix_stokenet_node.http_port(),
            });

            radix_aggregator.start().await?;

            self.radix_aggregator = Some(radix_aggregator);

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

            let radix_gateway = RadixGateway::new(RadixGatewayOptions {
                postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
                postgres_ip_address: postgres.ip_address().await.to_string(),
                postgres_password: POSTGRES_PASSWORD.to_string(),
                postgres_port: postgres.port(),
                postgres_username: POSTGRES_USERNAME.to_string(),
                radix_node_ip_address: radix_stokenet_node.ip_address().await.to_string(),
                radix_node_port: radix_stokenet_node.http_port(),
            });

            radix_gateway.start().await?;

            self.radix_gateway = Some(radix_gateway);

            info!("radix-gateway for radix stokenet started");
        }

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

        let session_manager = IdentityManager::new(IdentityManagerOptions {
            attestor: self.attestor.clone(),
            challenge_store,
            sessions_store,
            radix_gateway_origin: GATEWAY_URL,
            radix_network_definition: &NetworkDefinition::stokenet(),
        });

        let http_sock_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.args.port));
        let http_server = InsecureHttpServer::new(
            http_sock_addr,
            Router::new()
                .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
                .layer(CorsLayer::very_permissive()),
        );

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
                        num_replicas: self.num_replicas,
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
                    num_replicas: self.num_replicas,
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
                    num_replicas: self.num_replicas,
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
                    num_replicas: self.num_replicas,
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
            http_server,
            network: network.clone(),
            runtime_pool_manager,
            session_manager,
        });

        // Shutdown the light core and free the port before starting the full core
        let _ = light_core.shutdown().await;
        self.light_core = None;

        core.start().await?;

        self.core = Some(core);

        info!("core started");

        Ok(())
    }
}
