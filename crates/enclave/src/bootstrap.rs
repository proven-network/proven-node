#![allow(clippy::significant_drop_tightening)]

use super::error::{Error, Result};
use super::net::{bring_up_loopback, setup_default_gateway, write_dns_resolv};
use super::node::{EnclaveNode, EnclaveNodeGateway, Services};
use super::speedtest::SpeedTest;

use std::convert::TryInto;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::any;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use http::StatusCode;
use proven_applications::{ApplicationManager, ApplicationManagerConfig};
use proven_attestation::Attestor;
use proven_attestation_nsm::NsmAttestor;
use proven_bootable::Bootable;
use proven_cert_store::CertStore;
use proven_dnscrypt_proxy::{DnscryptProxy, DnscryptProxyOptions};
use proven_engine::config::{
    ConsensusConfig, GlobalConsensusConfig, GroupConsensusConfig, NetworkConfig, ServiceConfig,
    StorageConfig,
};
use proven_engine::{EngineBuilder, EngineConfig};
use proven_external_fs::{ExternalFs, ExternalFsOptions};
use proven_gateway::{BootstrapUpgrade, Gateway, GatewayOptions};
use proven_http_letsencrypt::{LetsEncryptHttpServer, LetsEncryptHttpServerOptions};
use proven_identity::{IdentityManager, IdentityManagerConfig};
use proven_imds::{IdentityDocument, Imds};
use proven_instance_details::{Instance, InstanceDetailsFetcher};
use proven_kms::{Kms, KmsOptions};
use proven_messaging_memory::client::MemoryClientOptions;
use proven_messaging_memory::service::MemoryServiceOptions;
use proven_messaging_memory::stream::{MemoryStream2, MemoryStream3, MemoryStreamOptions};
use proven_network::NetworkManager;
use proven_passkeys::{PasskeyManagement, PasskeyManager, PasskeyManagerOptions};
use proven_postgres::{Postgres, PostgresOptions};
use proven_radix_aggregator::{RadixAggregator, RadixAggregatorOptions};
use proven_radix_gateway::{RadixGateway, RadixGatewayOptions};
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_radix_node::{RadixNode, RadixNodeOptions};
use proven_runtime::{
    RpcEndpoints, RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions,
};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_streamed::{StreamedSqlStore2, StreamedSqlStore3};
use proven_storage::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_store::Store;
use proven_store_asm::{AsmStore, AsmStoreOptions};
use proven_store_memory::{MemoryStore, MemoryStore1, MemoryStore2, MemoryStore3};
use proven_store_s3::{S3Store, S3Store2, S3Store3, S3StoreOptions};
use proven_topology::{NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport::HttpIntegratedTransport;
use proven_transport_ws::{WebsocketConfig, WebsocketTransport};
use proven_vsock_cac::InitializeRequest;
use proven_vsock_proxy::Proxy;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

static GATEWAY_URL: &str = "http://127.0.0.1:8081";

static POSTGRES_USERNAME: &str = "your-username";
static POSTGRES_PASSWORD: &str = "your-password";
static POSTGRES_DATABASE: &str = "babylon-db";
static POSTGRES_STORE_DIR: &str = "/var/lib/postgres";

static RADIX_NODE_STORE_DIR: &str = "/var/lib/babylon";

// let nats_monitor = NatsMonitor::new(8222);
// let varz = nats_monitor.get_varz().await?;
// info!("nats varz: {:?}", varz);
// let connz = nats_monitor.get_connz().await?;
// info!("nats connz: {:?}", connz);

// TODO: This is in dire need of refactoring.
pub struct Bootstrap {
    args: InitializeRequest,
    attestor: NsmAttestor,

    // added during initialization
    governance: Option<MockTopologyAdaptor>,
    imds_identity: Option<IdentityDocument>,
    instance_details: Option<Instance>,
    transport: Option<Arc<WebsocketTransport<MockTopologyAdaptor, NsmAttestor>>>,
    network: Option<
        Arc<
            NetworkManager<
                WebsocketTransport<MockTopologyAdaptor, NsmAttestor>,
                MockTopologyAdaptor,
            >,
        >,
    >,

    proxy: Option<Proxy>,
    proxy_handle: Option<JoinHandle<proven_vsock_proxy::Result<()>>>,

    dnscrypt_proxy: Option<DnscryptProxy>,
    dnscrypt_proxy_handle: Option<JoinHandle<proven_dnscrypt_proxy::Result<()>>>,

    radix_node_fs: Option<ExternalFs>,
    radix_node_fs_handle: Option<JoinHandle<proven_external_fs::Result<()>>>,

    radix_node: Option<RadixNode>,

    postgres_fs: Option<ExternalFs>,
    postgres_fs_handle: Option<JoinHandle<proven_external_fs::Result<()>>>,

    postgres: Option<Postgres>,

    radix_aggregator: Option<RadixAggregator>,
    radix_gateway: Option<RadixGateway>,

    gateway: Option<EnclaveNodeGateway>,

    // state
    started: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Bootstrap {
    pub fn new(args: InitializeRequest) -> Result<Self> {
        let attestor = NsmAttestor::new()?;

        Ok(Self {
            args,
            attestor,

            governance: None,
            imds_identity: None,
            instance_details: None,
            transport: None,
            network: None,

            proxy: None,
            proxy_handle: None,

            dnscrypt_proxy: None,
            dnscrypt_proxy_handle: None,

            radix_node_fs: None,
            radix_node_fs_handle: None,

            radix_node: None,

            postgres_fs: None,
            postgres_fs_handle: None,

            postgres: None,

            radix_aggregator: None,

            radix_gateway: None,

            gateway: None,

            started: false,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::large_stack_frames)]
    #[allow(clippy::cognitive_complexity)]
    pub async fn initialize(mut self) -> Result<EnclaveNode> {
        if self.started {
            return Err(Error::AlreadyStarted);
        }

        self.started = true;

        if let Err(e) = self.remount_tmp_with_exec().await {
            error!("failed to remount tmp with exec: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.configure_temp_dns_resolv() {
            error!("failed to configure temp dns resolv: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.bring_up_loopback().await {
            error!("failed to bring up loopback: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_network_proxy().await {
            error!("failed to start network proxy: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.seed_entropy().await {
            error!("failed to seed entropy: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.fetch_imds_identity().await {
            error!("failed to fetch imds identity: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.fetch_instance_details().await {
            error!("failed to fetch instance details: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.perform_speedtest().await {
            error!("failed to perform speedtest: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_dnscrypt_proxy().await {
            error!("failed to start dnscrypt-proxy: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.get_network_topology().await {
            error!("failed to get network topology: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_radix_node_fs().await {
            error!("failed to start radix-node filesystem: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_radix_node().await {
            error!("failed to start radix-node: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        if let Err(e) = self.start_postgres_fs().await {
            error!("failed to start postgres filesystem: {:?}", e);
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

        if let Err(e) = self.start_gateway().await {
            error!("failed to start gateway: {:?}", e);
            self.unwind_services().await;
            return Err(e);
        }

        let proxy_handle = self.proxy_handle.take().unwrap();
        let dnscrypt_proxy_handle = self.dnscrypt_proxy_handle.take().unwrap();
        let radix_node_fs_handle = self.radix_node_fs_handle.take().unwrap();
        let postgres_fs_handle = self.postgres_fs_handle.take().unwrap();

        let proxy = Arc::new(Mutex::new(self.proxy.take().unwrap()));
        let dnscrypt_proxy = Arc::new(Mutex::new(self.dnscrypt_proxy.take().unwrap()));
        let radix_node_fs = Arc::new(Mutex::new(self.radix_node_fs.take().unwrap()));
        let radix_node = Arc::new(Mutex::new(self.radix_node.take().unwrap()));
        let postgres_fs = Arc::new(Mutex::new(self.postgres_fs.take().unwrap()));
        let postgres = Arc::new(Mutex::new(self.postgres.take().unwrap()));
        let radix_aggregator = Arc::new(Mutex::new(self.radix_aggregator.take().unwrap()));
        let radix_gateway = Arc::new(Mutex::new(self.radix_gateway.take().unwrap()));
        let gateway = Arc::new(Mutex::new(self.gateway.take().unwrap()));

        let node_services = Services {
            proxy: proxy.clone(),
            dnscrypt_proxy: dnscrypt_proxy.clone(),
            radix_node_fs: radix_node_fs.clone(),
            radix_node: radix_node.clone(),
            postgres_fs: postgres_fs.clone(),
            postgres: postgres.clone(),
            radix_aggregator: radix_aggregator.clone(),
            radix_gateway: radix_gateway.clone(),
            gateway: gateway.clone(),
        };

        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            let critical_tasks = {
                let radix_node = radix_node.clone();
                let radix_aggregator = radix_aggregator.clone();
                let radix_gateway = radix_gateway.clone();
                let postgres = postgres.clone();
                let gateway = gateway.clone();

                // Tasks that must be running for the enclave to function
                tokio::spawn(async move {
                    let radix_node = radix_node.clone();
                    let radix_node_guard = radix_node.lock().await;

                    let radix_aggregator = radix_aggregator.clone();
                    let radix_aggregator_guard = radix_aggregator.lock().await;

                    let radix_gateway = radix_gateway.clone();
                    let radix_gateway_guard = radix_gateway.lock().await;

                    let postgres = postgres.clone();
                    let postgres_guard = postgres.lock().await;

                    let gateway = gateway.clone();
                    let gateway_guard = gateway.lock().await;

                    tokio::select! {
                        Ok(Err(e)) = proxy_handle => {
                            error!("proxy exited: {:?}", e);
                        }
                        Ok(Err(e)) = dnscrypt_proxy_handle => {
                            error!("dnscrypt_proxy exited: {:?}", e);
                        }
                        Ok(Err(e)) = radix_node_fs_handle => {
                            error!("radix_external_fs exited: {:?}", e);
                        }
                        () = radix_node_guard.wait() => {
                            error!("radix_node exited");
                        }
                        Ok(Err(e)) = postgres_fs_handle => {
                            error!("postgres_external_fs exited: {:?}", e);
                        }
                        () = postgres_guard.wait() => {
                            error!("postgres exited");
                        }
                        () = radix_aggregator_guard.wait() => {
                            error!("radix_aggregator exited");
                        }
                        () = radix_gateway_guard.wait() => {
                            error!("radix_gateway exited");
                        }
                        () = gateway_guard.wait() => {
                            error!("gateway exited");
                        }
                        else => {
                            info!("enclave shutdown cleanly. goodbye.");
                        }
                    }
                })
            };

            tokio::select! {
                () = shutdown_token.cancelled() => info!("shutdown command received. shutting down..."),
                _ = critical_tasks => error!("critical task failed - exiting")
            }

            let _ = gateway.lock().await.shutdown().await;
            let _ = radix_gateway.lock().await.shutdown().await;
            let _ = radix_aggregator.lock().await.shutdown().await;
            let _ = radix_node.lock().await.shutdown().await;
            radix_node_fs.lock().await.shutdown().await;
            let _ = postgres.lock().await.shutdown().await;
            postgres_fs.lock().await.shutdown().await;
            dnscrypt_proxy.lock().await.shutdown().await;
            proxy.lock().await.shutdown().await;
        });

        self.task_tracker.close();

        let enclave = EnclaveNode::new(
            self.attestor.clone(),
            self.imds_identity.take().unwrap(),
            self.instance_details.take().unwrap(),
            node_services,
            self.shutdown_token.clone(),
            self.task_tracker.clone(),
        );

        Ok(enclave)
    }

    async fn unwind_services(self) {
        // shutdown in reverse order

        if let Some(gateway) = self.gateway {
            let _ = gateway.shutdown().await;
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

        if let Some(postgres_fs) = self.postgres_fs {
            postgres_fs.shutdown().await;
        }

        if let Some(radix_node) = self.radix_node {
            let _ = radix_node.shutdown().await;
        }

        if let Some(radix_node_fs) = self.radix_node_fs {
            radix_node_fs.shutdown().await;
        }

        if let Some(dnscrypt_proxy) = self.dnscrypt_proxy {
            dnscrypt_proxy.shutdown().await;
        }

        if let Some(proxy) = self.proxy {
            proxy.shutdown().await;
        }
    }

    async fn remount_tmp_with_exec(&self) -> Result<()> {
        tokio::process::Command::new("mount")
            .arg("-o")
            .arg("remount,exec")
            .arg("tmpfs")
            .arg("/tmp")
            .output()
            .await
            .map_err(|e| Error::Io("failed to remount tmp with exec", e))?;

        info!("tmp remounted with exec (babylon snappy java fix)");

        Ok(())
    }

    fn configure_temp_dns_resolv(&self) -> Result<()> {
        write_dns_resolv(self.args.host_dns_resolv.clone())?;

        Ok(())
    }

    async fn bring_up_loopback(&self) -> Result<()> {
        bring_up_loopback().await?;

        info!("loopback up");

        Ok(())
    }

    async fn start_network_proxy(&mut self) -> Result<()> {
        // Calculate TUN mask from CIDR
        let tun_mask = self.args.cidr.network_length();

        let proxy = Proxy::new(self.args.enclave_ip, tun_mask, self.args.proxy_port, false);
        let proxy_handle = proxy.start().await?;

        setup_default_gateway("tun0", self.args.host_ip, self.args.cidr).await?;

        self.proxy = Some(proxy);
        self.proxy_handle = Some(proxy_handle);

        info!("network proxy configured");

        Ok(())
    }

    async fn seed_entropy(&self) -> Result<()> {
        let secured_random_bytes = self.attestor.secure_random().await?;
        let mut dev_random = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/random")
            .map_err(|e| Error::Io("failed to open /dev/random", e))?;
        std::io::Write::write_all(&mut dev_random, secured_random_bytes.as_ref())
            .map_err(|e| Error::Io("failed to write to /dev/random", e))?;

        info!("entropy seeded");

        Ok(())
    }

    async fn fetch_imds_identity(&mut self) -> Result<()> {
        let imds = Imds::new().await?;
        let identity = imds.get_verified_identity_document().await?;

        info!("identity: {:?}", identity);

        self.imds_identity = Some(identity);

        Ok(())
    }

    async fn fetch_instance_details(&mut self) -> Result<()> {
        let id = self.imds_identity.as_ref().unwrap_or_else(|| {
            panic!("imds identity not fetched before instance details");
        });

        let fetcher = InstanceDetailsFetcher::new(id.region.clone()).await;
        let instance = fetcher.get_instance_details(id.instance_id.clone()).await?;

        info!("instance: {:?}", instance);

        self.instance_details = Some(instance);

        Ok(())
    }

    async fn perform_speedtest(&self) -> Result<()> {
        if self.args.skip_speedtest {
            info!("skipping speedtest...");
        } else {
            info!("running speedtest...");

            let results = SpeedTest::run().await?;

            info!("speedtest results: {:?}", results);
        }

        Ok(())
    }

    async fn start_dnscrypt_proxy(&mut self) -> Result<()> {
        let id = self.imds_identity.as_ref().unwrap_or_else(|| {
            panic!("imds identity not fetched before dnscrypt-proxy");
        });

        let instance_details = self.instance_details.as_ref().unwrap_or_else(|| {
            panic!("instance details not fetched before dnscrypt-proxy");
        });

        let dnscrypt_proxy = DnscryptProxy::new(DnscryptProxyOptions {
            availability_zone: instance_details.availability_zone.clone(),
            region: id.region.clone(),
            subnet_id: instance_details.subnet_id.clone(),
            vpc_id: instance_details.vpc_id.clone(),
        });
        let dnscrypt_proxy_handle = dnscrypt_proxy.start().await?;

        self.dnscrypt_proxy = Some(dnscrypt_proxy);
        self.dnscrypt_proxy_handle = Some(dnscrypt_proxy_handle);

        // Switch to dnscrypt-proxy's DNS resolver
        write_dns_resolv("nameserver 127.0.0.1".to_string())?;

        info!("dnscrypt-proxy started");

        Ok(())
    }

    async fn get_network_topology(&mut self) -> Result<()> {
        // Parse the private key and calculate public key
        let private_key_bytes = hex::decode(self.args.node_key.trim())
            .map_err(|e| Error::PrivateKey(format!("Failed to decode private key as hex: {e}")))?;

        // We need exactly 32 bytes for ed25519 private key
        let private_key = SigningKey::try_from(private_key_bytes.as_slice()).map_err(|_| {
            Error::PrivateKey("Failed to create SigningKey: invalid key length".to_string())
        })?;

        // TODO: use helios in production
        let governance = MockTopologyAdaptor::new(
            Vec::new(),
            Vec::new(),
            "http://localhost:3200".to_string(),
            vec![],
        );

        // Create node ID from private key
        let node_id = NodeId::from(private_key.verifying_key());

        // Create topology manager
        let topology_manager = Arc::new(TopologyManager::new(
            Arc::new(governance.clone()),
            node_id.clone(),
        ));
        topology_manager
            .start()
            .await
            .map_err(|e| Error::Consensus(format!("Failed to start topology manager: {e}")))?;

        // Create websocket transport
        let websocket_config = WebsocketConfig::default();
        let transport = Arc::new(WebsocketTransport::new(
            websocket_config,
            Arc::new(self.attestor.clone()),
            Arc::new(governance.clone()),
            private_key.clone(),
            topology_manager.clone(),
        ));

        // Create network manager
        let network = Arc::new(NetworkManager::new(
            node_id.clone(),
            transport.clone(),
            topology_manager.clone(),
        ));
        network.start().await.map_err(Error::Bootable)?;

        self.governance = Some(governance);
        self.transport = Some(transport);
        self.network = Some(network);

        Ok(())
    }

    async fn start_radix_node_fs(&mut self) -> Result<()> {
        let radix_node_fs = ExternalFs::new(ExternalFsOptions {
            encryption_key: "your-password".to_string(),
            nfs_mount_point_dir: PathBuf::from(format!("{}/babylon/", self.args.nfs_mount_point)),
            mount_dir: PathBuf::from(RADIX_NODE_STORE_DIR),
            skip_fsck: self.args.skip_fsck,
        })?;

        let radix_node_fs_handle = radix_node_fs.start().await?;

        self.radix_node_fs = Some(radix_node_fs);
        self.radix_node_fs_handle = Some(radix_node_fs_handle);

        info!("radix-node filesystem started");

        Ok(())
    }

    async fn start_radix_node(&mut self) -> Result<()> {
        let instance_details = self.instance_details.as_ref().unwrap_or_else(|| {
            panic!("instance details not fetched before radix-node");
        });

        let host_ip = instance_details.public_ip.unwrap().to_string();
        let radix_node = RadixNode::new(RadixNodeOptions {
            config_dir: PathBuf::from("/tmp/radix-node-stokenet"),
            host_ip,
            http_port: 3333,
            network_definition: radix_common::network::NetworkDefinition::stokenet(),
            p2p_port: self.args.radix_stokenet_port,
            store_dir: PathBuf::from(RADIX_NODE_STORE_DIR),
        });

        radix_node.start().await.map_err(Error::Bootable)?;

        self.radix_node = Some(radix_node);

        info!("radix-node started");

        Ok(())
    }

    async fn start_postgres_fs(&mut self) -> Result<()> {
        let postgres_fs = ExternalFs::new(ExternalFsOptions {
            encryption_key: "your-password".to_string(),
            nfs_mount_point_dir: PathBuf::from(format!("{}/postgres/", self.args.nfs_mount_point)),
            mount_dir: PathBuf::from(POSTGRES_STORE_DIR),
            skip_fsck: self.args.skip_fsck,
        })?;

        let postgres_fs_handle = postgres_fs.start().await?;

        self.postgres_fs = Some(postgres_fs);
        self.postgres_fs_handle = Some(postgres_fs_handle);

        info!("postgres filesystem started");

        Ok(())
    }

    async fn start_postgres(&mut self) -> Result<()> {
        let postgres = Postgres::new(PostgresOptions {
            password: POSTGRES_PASSWORD.to_string(),
            port: 5432,
            username: POSTGRES_USERNAME.to_string(),
            skip_vacuum: self.args.skip_vacuum,
            store_dir: PathBuf::from(POSTGRES_STORE_DIR),
        });

        postgres.start().await.map_err(Error::Bootable)?;

        self.postgres = Some(postgres);

        info!("postgres started");

        Ok(())
    }

    async fn start_radix_aggregator(&mut self) -> Result<()> {
        let postgres = self.postgres.as_ref().unwrap_or_else(|| {
            panic!("postgres not set before radix aggregator step");
        });

        let radix_node = self.radix_node.as_ref().unwrap_or_else(|| {
            panic!("radix node not set before radix aggregator step");
        });

        let radix_aggregator = RadixAggregator::new(RadixAggregatorOptions {
            postgres_database: POSTGRES_DATABASE.to_string(),
            postgres_ip_address: postgres.ip_address().await.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_port: postgres.port(),
            postgres_username: POSTGRES_USERNAME.to_string(),
            radix_node_ip_address: radix_node.ip_address().await.to_string(),
            radix_node_port: radix_node.http_port(),
        });

        radix_aggregator.start().await.map_err(Error::Bootable)?;

        self.radix_aggregator = Some(radix_aggregator);

        info!("radix-aggregator started");

        Ok(())
    }

    async fn start_radix_gateway(&mut self) -> Result<()> {
        let postgres = self.postgres.as_ref().unwrap_or_else(|| {
            panic!("postgres not set before radix gateway step");
        });

        let radix_node = self.radix_node.as_ref().unwrap_or_else(|| {
            panic!("radix node not set before radix gateway step");
        });

        let radix_gateway = RadixGateway::new(RadixGatewayOptions {
            postgres_database: POSTGRES_DATABASE.to_string(),
            postgres_ip_address: postgres.ip_address().await.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_port: postgres.port(),
            postgres_username: POSTGRES_USERNAME.to_string(),
            radix_node_ip_address: radix_node.ip_address().await.to_string(),
            radix_node_port: radix_node.http_port(),
        });

        radix_gateway.start().await.map_err(Error::Bootable)?;

        self.radix_gateway = Some(radix_gateway);

        info!("radix-gateway started");

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::large_stack_frames)]
    #[allow(clippy::cognitive_complexity)]
    async fn start_gateway(&mut self) -> Result<()> {
        let id = self.imds_identity.as_ref().unwrap_or_else(|| {
            panic!("imds identity not fetched before gateway");
        });

        let _network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before gateway");
        });

        let passkey_manager = PasskeyManager::new(PasskeyManagerOptions {
            passkeys_store: MemoryStore::new(),
        });

        let sessions_manager = SessionManager::new(SessionManagerOptions {
            attestor: self.attestor.clone(),
            sessions_store: MemoryStore1::new(),
        });

        let cert_store = CertStore::new(
            S3Store::new(S3StoreOptions {
                bucket: self.args.certificates_bucket.clone(),
                prefix: None,
                region: id.region.clone(),
                secret_key: get_or_init_encrypted_key(
                    id.region.clone(),
                    self.args.kms_key_id.clone(),
                    "CERTIFICATES_KEY".to_string(),
                )
                .await?,
            })
            .await,
        );

        // Extract domain from email or use a default
        let domain = self
            .args
            .email
            .first()
            .and_then(|email| email.split('@').next_back())
            .unwrap_or("enclave.local");

        let http_sock_addr = SocketAddr::from((self.args.enclave_ip, self.args.https_port));
        let http_server = LetsEncryptHttpServer::new(LetsEncryptHttpServerOptions {
            cert_store,
            cname_domain: domain.to_string(),
            domains: vec![domain.to_string()],
            emails: self.args.email.clone(),
            fallback_router: Router::new()
                .fallback(any(|| async { (StatusCode::NOT_FOUND, "") }))
                .layer(CorsLayer::very_permissive()),
            listen_addr: http_sock_addr,
        });

        let application_store = MemoryStore2::new();

        let application_sql_store = StreamedSqlStore2::new(
            MemoryStream2::new("APPLICATION_SQL", MemoryStreamOptions),
            MemoryServiceOptions,
            MemoryClientOptions,
            S3Store2::new(S3StoreOptions {
                bucket: self.args.certificates_bucket.clone(),
                prefix: Some("application".to_string()),
                region: id.region.clone(),
                secret_key: get_or_init_encrypted_key(
                    id.region.clone(),
                    self.args.kms_key_id.clone(),
                    "APPLICATION_SNAPSHOTS_KEY".to_string(),
                )
                .await?,
            })
            .await,
        );

        let personal_store = MemoryStore3::new();

        let personal_sql_store = StreamedSqlStore3::new(
            MemoryStream3::new("PERSONAL_SQL", MemoryStreamOptions),
            MemoryServiceOptions,
            MemoryClientOptions,
            S3Store3::new(S3StoreOptions {
                bucket: self.args.certificates_bucket.clone(),
                prefix: Some("personal".to_string()),
                region: id.region.clone(),
                secret_key: get_or_init_encrypted_key(
                    id.region.clone(),
                    self.args.kms_key_id.clone(),
                    "PERSONAL_SNAPSHOTS_KEY".to_string(),
                )
                .await?,
            })
            .await,
        );

        let nft_store = MemoryStore3::new();

        let nft_sql_store = StreamedSqlStore3::new(
            MemoryStream3::new("NFT_SQL", MemoryStreamOptions),
            MemoryServiceOptions,
            MemoryClientOptions,
            S3Store3::new(S3StoreOptions {
                bucket: self.args.certificates_bucket.clone(),
                prefix: Some("nft".to_string()),
                region: id.region.clone(),
                secret_key: get_or_init_encrypted_key(
                    id.region.clone(),
                    self.args.kms_key_id.clone(),
                    "NFT_SNAPSHOTS_KEY".to_string(),
                )
                .await?,
            })
            .await,
        );

        let file_system_store = S3Store::new(S3StoreOptions {
            bucket: self.args.file_systems_bucket.clone(),
            prefix: None,
            region: id.region.clone(),
            secret_key: get_or_init_encrypted_key(
                id.region.clone(),
                self.args.kms_key_id.clone(),
                "FILE_SYSTEMS_KEY".to_string(),
            )
            .await?,
        })
        .await;

        let radix_nft_verifier = GatewayRadixNftVerifier::new(GATEWAY_URL);

        let runtime_pool_manager = RuntimePoolManager::new(RuntimePoolManagerOptions {
            application_sql_store,
            application_store,
            file_system_store,
            max_workers: self.args.max_runtime_workers,
            nft_sql_store,
            nft_store,
            personal_sql_store,
            personal_store,
            radix_nft_verifier,
            rpc_endpoints: RpcEndpoints::external(), // TODO: build this from network
        })
        .await;

        let governance = self.governance.as_ref().unwrap_or_else(|| {
            panic!("governance not set before gateway");
        });

        let _attestor = self.attestor.clone();

        let node_key = hex::decode(self.args.node_key.clone()).unwrap();
        let node_key: [u8; 32] = node_key.try_into().unwrap();
        let node_key = SigningKey::from_bytes(&node_key);

        // Get node ID from key
        let node_id = NodeId::from(node_key.verifying_key());

        // Configure the engine
        let engine_config = EngineConfig {
            consensus: ConsensusConfig {
                global: GlobalConsensusConfig {
                    election_timeout_min: Duration::from_millis(1500),
                    election_timeout_max: Duration::from_millis(3000),
                    heartbeat_interval: Duration::from_millis(500),
                    snapshot_interval: 10000,
                    max_entries_per_append: 100,
                },
                group: GroupConsensusConfig {
                    election_timeout_min: Duration::from_millis(1500),
                    election_timeout_max: Duration::from_millis(3000),
                    heartbeat_interval: Duration::from_millis(500),
                    snapshot_interval: 10000,
                    max_entries_per_append: 100,
                },
            },
            network: NetworkConfig {
                listen_addr: format!("0.0.0.0:{}", self.args.https_port),
                public_addr: format!(
                    "https://enclave-{}.local",
                    self.args.enclave_ip.to_string().replace('.', "-")
                ),
                connection_timeout: Duration::from_secs(10),
                request_timeout: Duration::from_secs(30),
            },
            node_name: node_id.to_string(),
            services: ServiceConfig::default(),
            storage: StorageConfig {
                path: "/tmp/engine-storage".to_string(),
                max_log_size: 1_000_000,
                compaction_interval: Duration::from_secs(300),
                cache_size: 1_000_000,
            },
        };

        // Get network and topology managers
        let network_manager = self
            .network
            .as_ref()
            .ok_or_else(|| Error::Consensus("Network not initialized".to_string()))?;

        // Extract topology manager from network - we'll need to store it separately
        // For now, create a new one (this is a limitation we'll need to fix)
        let topology_manager = Arc::new(TopologyManager::new(
            Arc::new(governance.clone()),
            node_id.clone(),
        ));
        topology_manager
            .start()
            .await
            .map_err(|e| Error::Consensus(format!("Failed to start topology manager: {e}")))?;

        // Create memory storage
        let storage_adaptor = MemoryStorage::new();
        let storage_manager = Arc::new(StorageManager::new(storage_adaptor));

        // Build the engine
        let mut engine = EngineBuilder::new(node_id)
            .with_config(engine_config)
            .with_network(network_manager.clone())
            .with_topology(topology_manager)
            .with_storage(storage_manager)
            .build()
            .await
            .map_err(|e| Error::Consensus(format!("Failed to build engine: {e}")))?;

        // Start the engine
        engine
            .start()
            .await
            .map_err(|e| Error::Consensus(format!("Failed to start engine: {e}")))?;

        // Get the client for Gateway to use
        let _consensus = Arc::new(engine.client());

        // Create ApplicationManager with engine client
        let engine_client = Arc::new(engine.client());

        let application_manager_config = ApplicationManagerConfig {
            stream_prefix: "applications".to_string(),
            leadership_lease_duration: std::time::Duration::from_secs(30),
            leadership_renewal_interval: std::time::Duration::from_secs(10),
            command_timeout: std::time::Duration::from_secs(30),
        };

        let application_manager =
            ApplicationManager::new(Arc::clone(&engine_client), application_manager_config).await?;

        // Create IdentityManager with engine client
        let identity_manager_config = IdentityManagerConfig {
            stream_prefix: "identity".to_string(),
            leadership_lease_duration: std::time::Duration::from_secs(30),
            leadership_renewal_interval: std::time::Duration::from_secs(10),
            command_timeout: std::time::Duration::from_secs(30),
        };

        let identity_manager = IdentityManager::new(engine_client.clone(), identity_manager_config)
            .await
            .map_err(|e| Error::Consensus(format!("Failed to create IdentityManager: {e}")))?;

        // Get engine router from transport
        let transport = self
            .transport
            .as_ref()
            .ok_or_else(|| Error::Consensus("Transport not initialized".to_string()))?;
        let engine_router = transport
            .create_router_integration()
            .map_err(|e| Error::Consensus(format!("Failed to create router integration: {e}")))?;

        let gateway = Gateway::new(GatewayOptions {
            attestor: self.attestor.clone(),
            engine_router,
            governance: governance.clone(),
            http_server,
            origin: format!(
                "https://enclave-{}.local",
                self.args.enclave_ip.to_string().replace('.', "-")
            ),
        });

        gateway
            .bootstrap(BootstrapUpgrade {
                application_manager,
                runtime_pool_manager,
                identity_manager,
                passkey_manager,
                sessions_manager,
            })
            .await?;

        gateway.start().await.map_err(Error::Bootable)?;

        self.gateway = Some(gateway);

        info!("gateway started");

        Ok(())
    }
}

async fn get_or_init_encrypted_key(
    region: String,
    key_id: String,
    key_name: String,
) -> Result<[u8; 32]> {
    let secret_id = format!("proven-{}", region.clone());
    let store = AsmStore::new(AsmStoreOptions {
        region: region.clone(),
        secret_name: secret_id,
    })
    .await;
    let kms = Kms::new(KmsOptions {
        attestor: NsmAttestor::new()?,
        key_id,
        region,
    })
    .await;

    let key_opt = store.get(key_name.clone()).await?;
    let key: [u8; 32] = if let Some(encrypted_key) = key_opt {
        kms.decrypt(encrypted_key)
            .await?
            .to_vec()
            .try_into()
            .map_err(|_| Error::BadKey)?
    } else {
        let unencrypted_key = rand::random::<[u8; 32]>();
        let encrypted_key = kms.encrypt(Bytes::from(unencrypted_key.to_vec())).await?;
        store.put(key_name, encrypted_key).await?;
        unencrypted_key
    };

    Ok(key)
}
