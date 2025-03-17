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
use proven_attestation_dev::DevAttestor;
use proven_core::{Core, CoreOptions};
use proven_ethereum_geth::{GethNode, GethNodeOptions, EthereumNetwork as GethNetwork};
use proven_ethereum_lighthouse::{LighthouseNode, LighthouseNodeOptions, EthereumNetwork as LighthouseNetwork};
use proven_governance::{Governance, GovernanceError, Node};
use proven_governance_mock::MockGovernance;
use proven_http_insecure::InsecureHttpServer;
use proven_messaging_nats::client::NatsClientOptions;
use proven_messaging_nats::service::NatsServiceOptions;
use proven_messaging_nats::stream::{NatsStream1, NatsStream2, NatsStream3, NatsStreamOptions};
use proven_nats_server::{NatsServer, NatsServerOptions};
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
    attestor: DevAttestor,
    external_ip: IpAddr,

    // added during initialization
    governance: Option<MockGovernance>,
    node_config: Option<Node>,

    radix_mainnet_node: Option<RadixNode>,
    radix_mainnet_node_handle: Option<JoinHandle<proven_radix_node::Result<()>>>,

    radix_stokenet_node: Option<RadixNode>,
    radix_stokenet_node_handle: Option<JoinHandle<proven_radix_node::Result<()>>>,

    ethereum_geth_node: Option<proven_ethereum_geth::GethNode>,
    ethereum_geth_node_handle: Option<JoinHandle<()>>,

    ethereum_lighthouse_node: Option<proven_ethereum_lighthouse::LighthouseNode>,
    ethereum_lighthouse_node_handle: Option<JoinHandle<()>>,

    postgres: Option<Postgres>,
    postgres_handle: Option<JoinHandle<proven_postgres::Result<()>>>,

    radix_aggregator: Option<RadixAggregator>,
    radix_aggregator_handle: Option<JoinHandle<proven_radix_aggregator::Result<()>>>,

    radix_gateway: Option<RadixGateway>,
    radix_gateway_handle: Option<JoinHandle<proven_radix_gateway::Result<()>>>,

    nats_client: Option<NatsClient>,
    nats_server: Option<NatsServer>,
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
            attestor: DevAttestor,
            external_ip,

            governance: None,
            node_config: None,

            radix_mainnet_node: None,
            radix_mainnet_node_handle: None,

            radix_stokenet_node: None,
            radix_stokenet_node_handle: None,

            ethereum_geth_node: None,
            ethereum_geth_node_handle: None,

            ethereum_lighthouse_node: None,
            ethereum_lighthouse_node_handle: None,

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

        if let Err(e) = self.get_network_topology().await {
            error!("failed to get network topology: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_radix_node().await {
            error!("failed to start radix-node: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_ethereum_nodes().await {
            error!("failed to start ethereum nodes: {:?}", e);
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
        let ethereum_geth_node_handle = self.ethereum_geth_node_handle.take();
        let ethereum_lighthouse_node_handle = self.ethereum_lighthouse_node_handle.take();
        let postgres_handle = self.postgres_handle.take();
        let radix_aggregator_handle = self.radix_aggregator_handle.take();
        let radix_gateway_handle = self.radix_gateway_handle.take();

        // Mandatory handles
        let nats_server_handle = self.nats_server_handle.take().unwrap();
        let core_handle = self.core_handle.take().unwrap();

        // Optional services
        let radix_mainnet_node_option = self.radix_mainnet_node.take().map(|node| Arc::new(Mutex::new(node)));
        let radix_stokenet_node_option = self.radix_stokenet_node.take().map(|node| Arc::new(Mutex::new(node)));
        let ethereum_geth_node_option = self.ethereum_geth_node.take().map(|node| Arc::new(Mutex::new(node)));
        let ethereum_lighthouse_node_option = self.ethereum_lighthouse_node.take().map(|node| Arc::new(Mutex::new(node)));
        let postgres = self.postgres.take().map(|postgres| Arc::new(Mutex::new(postgres)));
        let radix_aggregator = self.radix_aggregator.take().map(|aggregator| Arc::new(Mutex::new(aggregator)));
        let radix_gateway = self.radix_gateway.take().map(|gateway| Arc::new(Mutex::new(gateway)));

        // Mandatory services
        let nats_server = Arc::new(Mutex::new(self.nats_server.take().unwrap()));
        let core = Arc::new(Mutex::new(self.core.take().unwrap()));

        let node_services = Services {
            radix_mainnet_node: radix_mainnet_node_option.clone(),
            radix_stokenet_node: radix_stokenet_node_option.clone(),
            ethereum_geth_node: ethereum_geth_node_option.clone(),
            ethereum_lighthouse_node: ethereum_lighthouse_node_option.clone(),
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
                            if let Ok(Err(e)) = handle.await {
                                error!("radix mainnet node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async { 
                        if let Some(handle) = radix_stokenet_node_handle {
                            if let Ok(Err(e)) = handle.await {
                                error!("radix stokenet node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async { 
                        if let Some(handle) = ethereum_geth_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum geth node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async { 
                        if let Some(handle) = ethereum_lighthouse_node_handle {
                            if let Ok(e) = handle.await {
                                error!("ethereum lighthouse node exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async { 
                        if let Some(handle) = postgres_handle {
                            if let Ok(Err(e)) = handle.await {
                                error!("postgres exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async { 
                        if let Some(handle) = radix_aggregator_handle {
                            if let Ok(Err(e)) = handle.await {
                                error!("radix_aggregator exited: {:?}", e);
                                return;
                            }
                        }
                        std::future::pending::<()>().await
                    } => {},
                    _ = async { 
                        if let Some(handle) = radix_gateway_handle {
                            if let Ok(Err(e)) = handle.await {
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
                gateway.lock().await.shutdown().await;
            }
            
            if let Some(aggregator) = &radix_aggregator {
                aggregator.lock().await.shutdown().await;
            }
            
            if let Some(pg) = &postgres {
                pg.lock().await.shutdown().await;
            }
            
            if let Some(mainnet_node) = &radix_mainnet_node_option {
                mainnet_node.lock().await.shutdown().await;
            }
            
            if let Some(stokenet_node) = &radix_stokenet_node_option {
                stokenet_node.lock().await.shutdown().await;
            }

            if let Some(geth_node) = &ethereum_geth_node_option {
                geth_node.lock().await.shutdown().await;
            }
            
            if let Some(lighthouse_node) = &ethereum_lighthouse_node_option {
                lighthouse_node.lock().await.shutdown().await;
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

        if let Some(radix_gateway) = self.radix_gateway {
            radix_gateway.shutdown().await;
        }

        if let Some(radix_aggregator) = self.radix_aggregator {
            radix_aggregator.shutdown().await;
        }

        if let Some(postgres) = self.postgres {
            postgres.shutdown().await;
        }

        if let Some(ethereum_lighthouse_node) = self.ethereum_lighthouse_node {
            ethereum_lighthouse_node.shutdown().await;
        }

        if let Some(ethereum_geth_node) = self.ethereum_geth_node {
            ethereum_geth_node.shutdown().await;
        }

        if let Some(radix_node) = self.radix_mainnet_node {
            radix_node.shutdown().await;
        }
    }

    async fn get_network_topology(&mut self) -> Result<()> {
        let governance =
            MockGovernance::from_topology_file(self.args.topology_file.clone(), vec![])
                .map_err(|e| Error::Io(format!("Failed to load topology: {}", e)))?
                .with_private_key(&self.args.node_key)
                .map_err(|e| Error::Io(format!("Failed to initialize governance: {}", e)))?;

        let node_config = governance
            .get_self()
            .await
            .map_err(|e| Error::Governance(e.kind()))?;

        info!("node config: {:?}", node_config);

        // Check /etc/hosts to ensure the node's FQDN is properly configured
        check_hostname_resolution(&node_config.fqdn).await?;

        self.governance = Some(governance);
        self.node_config = Some(node_config);

        Ok(())
    }

    async fn start_radix_node(&mut self) -> Result<()> {
        let node_config = self.node_config.as_ref().unwrap_or_else(|| {
            panic!("node config not set before radix node step");
        });

        if node_config
            .specializations
            .contains(&proven_governance::NodeSpecialization::RadixMainnet)
        {
            let radix_mainnet_node = RadixNode::new(RadixNodeOptions {
                host_ip: self.external_ip.to_string(),
                network_definition: NetworkDefinition::mainnet(),
                port: self.args.radix_mainnet_port,
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

        if node_config
            .specializations
            .contains(&proven_governance::NodeSpecialization::RadixStokenet)
        {
            let radix_stokenet_node = RadixNode::new(RadixNodeOptions {
                host_ip: self.external_ip.to_string(),
                network_definition: NetworkDefinition::stokenet(),
                port: self.args.radix_stokenet_port,
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

    async fn start_ethereum_nodes(&mut self) -> Result<()> {
        let node_config = self.node_config.as_ref().unwrap_or_else(|| {
            panic!("node config not set before ethereum nodes step");
        });

        if node_config
            .specializations
            .contains(&proven_governance::NodeSpecialization::EthereumSepolia)
        {
            // Start Geth execution client
            let ethereum_geth_node = GethNode::new(GethNodeOptions {
                network: GethNetwork::Sepolia,
                store_dir: self.args.ethereum_sepolia_store_dir.to_string_lossy().to_string(),
            });

            let ethereum_geth_node_handle = ethereum_geth_node.start().await
                .map_err(|e| Error::Io(format!("Failed to start Geth node: {}", e)))?;

            self.ethereum_geth_node = Some(ethereum_geth_node);
            self.ethereum_geth_node_handle = Some(ethereum_geth_node_handle);

            info!("ethereum geth node (sepolia) started");

            // Start Lighthouse consensus client
            let ethereum_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
                host_ip: self.external_ip.to_string(),
                network: LighthouseNetwork::Sepolia,
                store_dir: self.args.ethereum_sepolia_store_dir.to_string_lossy().to_string(),
            });

            let ethereum_lighthouse_node_handle = ethereum_lighthouse_node.start().await
                .map_err(|e| Error::Io(format!("Failed to start Lighthouse node: {}", e)))?;

            self.ethereum_lighthouse_node = Some(ethereum_lighthouse_node);
            self.ethereum_lighthouse_node_handle = Some(ethereum_lighthouse_node_handle);

            info!("ethereum lighthouse node (sepolia) started");
        }

        Ok(())
    }

    async fn start_postgres(&mut self) -> Result<()> {
        let node_config = self.node_config.as_ref().unwrap_or_else(|| {
            panic!("node config not set before postgres step");
        });

        if node_config
            .specializations
            .contains(&proven_governance::NodeSpecialization::RadixMainnet)
            || node_config
                .specializations
                .contains(&proven_governance::NodeSpecialization::RadixStokenet)
        {
            let postgres = Postgres::new(PostgresOptions {
                bin_path: self.args.postgres_bin_path.clone(),
                password: POSTGRES_PASSWORD.to_string(),
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
        let node_config = self.node_config.as_ref().unwrap_or_else(|| {
            panic!("node config not fetched before core");
        });

        if node_config
            .specializations
            .contains(&proven_governance::NodeSpecialization::RadixStokenet)
        {
            let radix_aggregator = RadixAggregator::new(RadixAggregatorOptions {
                postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
                postgres_password: POSTGRES_PASSWORD.to_string(),
                postgres_username: POSTGRES_USERNAME.to_string(),
            });

            let radix_aggregator_handle = radix_aggregator.start().await?;

            self.radix_aggregator = Some(radix_aggregator);
            self.radix_aggregator_handle = Some(radix_aggregator_handle);

            info!("radix-aggregator for radix stokenet started");
        }

        Ok(())
    }

    async fn start_radix_gateway(&mut self) -> Result<()> {
        let node_config = self.node_config.as_ref().unwrap_or_else(|| {
            panic!("node config not fetched before core");
        });

        if node_config
            .specializations
            .contains(&proven_governance::NodeSpecialization::RadixStokenet)
        {
            let radix_gateway = RadixGateway::new(RadixGatewayOptions {
                postgres_database: POSTGRES_RADIX_STOKENET_DATABASE.to_string(),
                postgres_password: POSTGRES_PASSWORD.to_string(),
                postgres_username: POSTGRES_USERNAME.to_string(),
            });

            let radix_gateway_handle = radix_gateway.start().await?;

            self.radix_gateway = Some(radix_gateway);
            self.radix_gateway_handle = Some(radix_gateway_handle);

            info!("radix-gateway for radix stokenet started");
        }

        Ok(())
    }

    async fn start_nats_server(&mut self) -> Result<()> {
        let nats_server = NatsServer::new(NatsServerOptions {
            debug: self.args.testnet,
            listen_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.args.nats_port),
            server_name: self.node_config.as_ref().unwrap().fqdn.clone(),
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

        let node_config = self.node_config.as_ref().unwrap_or_else(|| {
            panic!("node config not fetched before core");
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
            governance: MockGovernance::new(Vec::new(), Vec::new()),
            primary_hostnames: vec![node_config.fqdn.clone()].into_iter().collect(),
            runtime_pool_manager,
            session_manager,
        });
        let core_handle = core.start(http_server).await?;

        self.core = Some(core);
        self.core_handle = Some(core_handle);

        info!("core started");

        Ok(())
    }
}
