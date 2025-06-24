use super::error::{Error, Result};
use super::net::{bring_up_loopback, setup_default_gateway, write_dns_resolv};
use super::node::{EnclaveNode, EnclaveNodeCore, Services};
use super::speedtest::SpeedTest;

use std::convert::Infallible;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use axum::Router;
use axum::routing::any;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use http::StatusCode;
use proven_applications::ApplicationManager;
use proven_attestation::Attestor;
use proven_attestation_nsm::NsmAttestor;
use proven_bootable::Bootable;
use proven_cert_store::CertStore;
use proven_core::{Core, CoreOptions};
use proven_dnscrypt_proxy::{DnscryptProxy, DnscryptProxyOptions};
use proven_external_fs::{ExternalFs, ExternalFsOptions};
use proven_governance_mock::MockGovernance;
use proven_http_letsencrypt::{LetsEncryptHttpServer, LetsEncryptHttpServerOptions};
use proven_imds::{IdentityDocument, Imds};
use proven_instance_details::{Instance, InstanceDetailsFetcher};
use proven_kms::{Kms, KmsOptions};
use proven_messaging::stream::Stream;
use proven_messaging_nats::client::NatsClientOptions;
use proven_messaging_nats::consumer::NatsConsumerOptions;
use proven_messaging_nats::service::NatsServiceOptions;
// use proven_nats_monitor::NatsMonitor;
use proven_identity::{IdentityManagement, IdentityManager, IdentityManagerOptions};
use proven_messaging_nats::stream::{NatsStream, NatsStream2, NatsStream3, NatsStreamOptions};
use proven_nats_server::{NatsServer, NatsServerOptions};
use proven_network::{Peer, ProvenNetwork, ProvenNetworkOptions};
use proven_postgres::{Postgres, PostgresOptions};
use proven_radix_aggregator::{RadixAggregator, RadixAggregatorOptions};
use proven_radix_gateway::{RadixGateway, RadixGatewayOptions};
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_radix_node::{RadixNode, RadixNodeOptions};
use proven_runtime::{
    RpcEndpoints, RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions,
};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_streamed::{StreamedSqlStore, StreamedSqlStore2, StreamedSqlStore3};
use proven_store::Store;
use proven_store_asm::{AsmStore, AsmStoreOptions};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3, NatsStoreOptions};
use proven_store_s3::{S3Store, S3Store2, S3Store3, S3StoreOptions};
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::InitializeRequest;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockStream};
use tower_http::cors::CorsLayer;
use tracing::{error, info};

static VMADDR_CID_EC2_HOST: u32 = 3;

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
    governance: Option<MockGovernance>,
    imds_identity: Option<IdentityDocument>,
    instance_details: Option<Instance>,
    network: Option<ProvenNetwork<MockGovernance, NsmAttestor>>,
    node_config: Option<Peer<NsmAttestor>>,

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

    nats_server_fs: Option<ExternalFs>,
    nats_server_fs_handle: Option<JoinHandle<proven_external_fs::Result<()>>>,

    nats_client: Option<NatsClient>,
    nats_server:
        Option<NatsServer<MockGovernance, NsmAttestor, S3Store<Bytes, Infallible, Infallible>>>,

    core: Option<EnclaveNodeCore>,

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
            network: None,
            node_config: None,

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

            nats_server_fs: None,
            nats_server_fs_handle: None,

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

        if let Err(e) = self.start_nats_fs().await {
            error!("failed to start nats filesystem: {:?}", e);
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

        let proxy_handle = self.proxy_handle.take().unwrap();
        let dnscrypt_proxy_handle = self.dnscrypt_proxy_handle.take().unwrap();
        let radix_node_fs_handle = self.radix_node_fs_handle.take().unwrap();
        let postgres_fs_handle = self.postgres_fs_handle.take().unwrap();
        let nats_server_fs_handle = self.nats_server_fs_handle.take().unwrap();

        let proxy = Arc::new(Mutex::new(self.proxy.take().unwrap()));
        let dnscrypt_proxy = Arc::new(Mutex::new(self.dnscrypt_proxy.take().unwrap()));
        let radix_node_fs = Arc::new(Mutex::new(self.radix_node_fs.take().unwrap()));
        let radix_node = Arc::new(Mutex::new(self.radix_node.take().unwrap()));
        let postgres_fs = Arc::new(Mutex::new(self.postgres_fs.take().unwrap()));
        let postgres = Arc::new(Mutex::new(self.postgres.take().unwrap()));
        let radix_aggregator = Arc::new(Mutex::new(self.radix_aggregator.take().unwrap()));
        let radix_gateway = Arc::new(Mutex::new(self.radix_gateway.take().unwrap()));
        let nats_server_fs = Arc::new(Mutex::new(self.nats_server_fs.take().unwrap()));
        let nats_server = Arc::new(Mutex::new(self.nats_server.take().unwrap()));
        let core = Arc::new(Mutex::new(self.core.take().unwrap()));

        let node_services = Services {
            proxy: proxy.clone(),
            dnscrypt_proxy: dnscrypt_proxy.clone(),
            radix_node_fs: radix_node_fs.clone(),
            radix_node: radix_node.clone(),
            postgres_fs: postgres_fs.clone(),
            postgres: postgres.clone(),
            radix_aggregator: radix_aggregator.clone(),
            radix_gateway: radix_gateway.clone(),
            nats_server_fs: nats_server_fs.clone(),
            nats_server: nats_server.clone(),
            core: core.clone(),
        };

        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            let critical_tasks = {
                let radix_node = radix_node.clone();
                let radix_aggregator = radix_aggregator.clone();
                let radix_gateway = radix_gateway.clone();
                let nats_server = nats_server.clone();
                let postgres = postgres.clone();
                let core = core.clone();

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

                    let nats_server = nats_server.clone();
                    let nats_server_guard = nats_server.lock().await;

                    let core = core.clone();
                    let core_guard = core.lock().await;

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
                        Ok(Err(e)) = nats_server_fs_handle => {
                            error!("nats_external_fs exited: {:?}", e);
                        }
                        () = nats_server_guard.wait() => {
                            error!("nats_server exited");
                        }
                        () = core_guard.wait() => {
                            error!("core exited");
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

            let _ = core.lock().await.shutdown().await;
            let _ = nats_server.lock().await.shutdown().await;
            nats_server_fs.lock().await.shutdown().await;
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

        if let Some(core) = self.core {
            let _ = core.shutdown().await;
        }

        if let Some(nats_server) = self.nats_server {
            let _ = nats_server.shutdown().await;
        }

        if let Some(nats_server_fs) = self.nats_server_fs {
            nats_server_fs.shutdown().await;
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
        let vsock_stream =
            VsockStream::connect(VsockAddr::new(VMADDR_CID_EC2_HOST, self.args.proxy_port))
                .await
                .unwrap();

        let proxy = Proxy::new(
            self.args.enclave_ip,
            self.args.host_ip,
            self.args.cidr,
            "tun0",
        )
        .await?;

        setup_default_gateway("tun0", self.args.host_ip, self.args.cidr).await?;

        let proxy_handle = proxy.start(vsock_stream);

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
        let private_key_bytes = hex::decode(self.args.node_key.trim()).map_err(|e| {
            Error::PrivateKey(format!("Failed to decode private key as hex: {}", e))
        })?;

        // We need exactly 32 bytes for ed25519 private key
        let private_key = SigningKey::try_from(private_key_bytes.as_slice()).map_err(|_| {
            Error::PrivateKey("Failed to create SigningKey: invalid key length".to_string())
        })?;

        // TODO: use helios in production
        let governance = MockGovernance::new(
            Vec::new(),
            Vec::new(),
            "http://localhost:3200".to_string(),
            vec![],
        );

        let network = ProvenNetwork::new(ProvenNetworkOptions {
            attestor: self.attestor.clone(),
            governance: governance.clone(),
            nats_cluster_port: self.args.nats_cluster_port,
            private_key,
        })
        .await?;

        self.governance = Some(governance);
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

        radix_node.start().await?;

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

        postgres.start().await?;

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

        radix_aggregator.start().await?;

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

        radix_gateway.start().await?;

        self.radix_gateway = Some(radix_gateway);

        info!("radix-gateway started");

        Ok(())
    }

    async fn start_nats_fs(&mut self) -> Result<()> {
        let nats_server_fs = ExternalFs::new(ExternalFsOptions {
            encryption_key: "your-password".to_string(),
            nfs_mount_point_dir: PathBuf::from(format!("{}/nats/", self.args.nfs_mount_point)),
            mount_dir: PathBuf::from("/var/lib/nats"),
            skip_fsck: self.args.skip_fsck,
        })?;

        let nats_server_fs_handle = nats_server_fs.start().await?;

        self.nats_server_fs = Some(nats_server_fs);
        self.nats_server_fs_handle = Some(nats_server_fs_handle);

        info!("nats filesystem started");

        Ok(())
    }

    async fn start_nats_server(&mut self) -> Result<()> {
        let instance_details = self.instance_details.as_ref().unwrap_or_else(|| {
            panic!("instance details not fetched before nats-server");
        });

        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("governance not set before nats server step");
        });

        let nats_server = NatsServer::new(NatsServerOptions {
            bin_dir: Some(PathBuf::from("/apps/nats/v2.11.4")),
            cert_store: None,
            client_port: 4222,
            config_dir: PathBuf::from("/tmp/nats-config"),
            debug: self.args.testnet,
            http_port: 8222,
            network: network.clone(),
            server_name: instance_details.instance_id.clone(),
            store_dir: PathBuf::from("/var/lib/nats/nats"),
        })?;

        nats_server.start().await?;
        let nats_client = nats_server.build_client().await?;

        self.nats_server = Some(nats_server);
        self.nats_client = Some(nats_client);

        info!("nats server started");

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn start_core(&mut self) -> Result<()> {
        let id = self.imds_identity.as_ref().unwrap_or_else(|| {
            panic!("imds identity not fetched before core");
        });

        let nats_client = self.nats_client.as_ref().unwrap_or_else(|| {
            panic!("nats client not fetched before core");
        });

        let network = self.network.as_ref().unwrap_or_else(|| {
            panic!("network not set before core");
        });

        let node_config = self.node_config.as_ref().unwrap_or_else(|| {
            panic!("node config not set before core");
        });

        let identity_manager = IdentityManager::new(IdentityManagerOptions {
            identity_store: StreamedSqlStore::new(
                NatsStream::new(
                    "IDENTITY_MANAGER_SQL",
                    NatsStreamOptions {
                        client: nats_client.clone(),
                        num_replicas: 1,
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
                S3Store::new(S3StoreOptions {
                    bucket: self.args.certificates_bucket.clone(),
                    prefix: Some("identity_manager".to_string()),
                    region: id.region.clone(),
                    secret_key: get_or_init_encrypted_key(
                        id.region.clone(),
                        self.args.kms_key_id.clone(),
                        "IDENTITY_MANAGER_SNAPSHOTS_KEY".to_string(),
                    )
                    .await?,
                })
                .await,
            ),
            passkeys_store: NatsStore::new(NatsStoreOptions {
                bucket: "passkeys".to_string(),
                client: nats_client.clone(),
                max_age: Duration::ZERO,
                num_replicas: 1,
                persist: true,
            }),
        });

        let sessions_manager = SessionManager::new(SessionManagerOptions {
            attestor: self.attestor.clone(),
            sessions_store: NatsStore1::new(NatsStoreOptions {
                bucket: "sessions".to_string(),
                client: nats_client.clone(),
                max_age: Duration::ZERO,
                num_replicas: 1,
                persist: true,
            }),
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

        // Parse the domain name from the origin in node config
        let domain = node_config.fqdn()?;

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

        let application_manager = ApplicationManager::new(
            // Command stream for processing application commands
            NatsStream::new(
                "APPLICATION_COMMANDS",
                NatsStreamOptions {
                    client: nats_client.clone(),
                    num_replicas: 1,
                },
            ),
            // Event stream for publishing and consuming application events
            NatsStream::new(
                "APPLICATION_EVENTS",
                NatsStreamOptions {
                    client: nats_client.clone(),
                    num_replicas: 1,
                },
            ),
            // Service options for command processing
            NatsServiceOptions {
                client: nats_client.clone(),
                durable_name: Some("APPLICATION_SERVICE".to_string()),
                jetstream_context: async_nats::jetstream::new(nats_client.clone()),
            },
            // Client options for sending commands
            NatsClientOptions {
                client: nats_client.clone(),
            },
            // Consumer options for event processing
            NatsConsumerOptions {
                client: nats_client.clone(),
                durable_name: Some("APPLICATION_VIEW_CONSUMER".to_string()),
                jetstream_context: async_nats::jetstream::new(nats_client.clone()),
            },
        );

        let application_store = NatsStore2::new(NatsStoreOptions {
            bucket: "APPLICATION_KV".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            num_replicas: 1,
            persist: true,
        });

        let application_sql_store = StreamedSqlStore2::new(
            NatsStream2::new(
                "APPLICATION_SQL",
                NatsStreamOptions {
                    client: nats_client.clone(),
                    num_replicas: 1,
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

        let personal_store = NatsStore3::new(NatsStoreOptions {
            bucket: "PERSONAL_KV".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            num_replicas: 1,
            persist: true,
        });

        let personal_sql_store = StreamedSqlStore3::new(
            NatsStream3::new(
                "PERSONAL_SQL",
                NatsStreamOptions {
                    client: nats_client.clone(),
                    num_replicas: 1,
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

        let nft_store = NatsStore3::new(NatsStoreOptions {
            bucket: "NFT_KV".to_string(),
            client: nats_client.clone(),
            max_age: Duration::ZERO,
            num_replicas: 1,
            persist: true,
        });

        let nft_sql_store = StreamedSqlStore3::new(
            NatsStream3::new(
                "NFT_SQL",
                NatsStreamOptions {
                    client: nats_client.clone(),
                    num_replicas: 1,
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

        let core = Core::new(CoreOptions {
            application_manager,
            attestor: self.attestor.clone(),
            identity_manager,
            sessions_manager,
            http_server,
            network: network.clone(),
            runtime_pool_manager,
        });
        core.start().await?;

        self.core = Some(core);

        info!("core started");

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
