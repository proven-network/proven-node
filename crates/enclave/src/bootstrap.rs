use super::enclave::{Enclave, EnclaveCore, Services};
use super::error::{Error, Result};
use super::net::{bring_up_loopback, setup_default_gateway, write_dns_resolv};
use super::speedtest::SpeedTest;

use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use bytes::Bytes;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation::Attestor;
use proven_attestation_nsm::NsmAttestor;
use proven_core::{Core, CoreOptions};
use proven_dnscrypt_proxy::{DnscryptProxy, DnscryptProxyOptions};
use proven_external_fs::{ExternalFs, ExternalFsOptions};
use proven_governance_mock::MockGovernance;
use proven_http_letsencrypt::{LetsEncryptHttpServer, LetsEncryptHttpServerOptions};
use proven_imds::{IdentityDocument, Imds};
use proven_instance_details::{Instance, InstanceDetailsFetcher};
use proven_kms::Kms;
use proven_messaging_nats::client::NatsClientOptions;
use proven_messaging_nats::service::NatsServiceOptions;
// use proven_nats_monitor::NatsMonitor;
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
use proven_store::Store;
use proven_store_asm::{AsmStore, AsmStoreOptions};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3, NatsStoreOptions};
use proven_store_s3::{S3Store, S3Store1, S3Store2, S3Store3, S3StoreOptions};
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::InitializeRequest;
use radix_common::network::NetworkDefinition;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockStream};
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
    nsm: NsmAttestor,
    network_definition: NetworkDefinition,

    // added during initialization
    imds_identity: Option<IdentityDocument>,
    instance_details: Option<Instance>,

    proxy: Option<Proxy>,
    proxy_handle: Option<JoinHandle<proven_vsock_proxy::Result<()>>>,

    dnscrypt_proxy: Option<DnscryptProxy>,
    dnscrypt_proxy_handle: Option<JoinHandle<proven_dnscrypt_proxy::Result<()>>>,

    radix_node_fs: Option<ExternalFs>,
    radix_node_fs_handle: Option<JoinHandle<proven_external_fs::Result<()>>>,

    radix_node: Option<RadixNode>,
    radix_node_handle: Option<JoinHandle<proven_radix_node::Result<()>>>,

    postgres_fs: Option<ExternalFs>,
    postgres_fs_handle: Option<JoinHandle<proven_external_fs::Result<()>>>,

    postgres: Option<Postgres>,
    postgres_handle: Option<JoinHandle<proven_postgres::Result<()>>>,

    radix_aggregator: Option<RadixAggregator>,
    radix_aggregator_handle: Option<JoinHandle<proven_radix_aggregator::Result<()>>>,

    radix_gateway: Option<RadixGateway>,
    radix_gateway_handle: Option<JoinHandle<proven_radix_gateway::Result<()>>>,

    nats_server_fs: Option<ExternalFs>,
    nats_server_fs_handle: Option<JoinHandle<proven_external_fs::Result<()>>>,

    nats_client: Option<NatsClient>,
    nats_server: Option<NatsServer>,
    nats_server_handle: Option<JoinHandle<proven_nats_server::Result<()>>>,

    core: Option<EnclaveCore>,
    core_handle: Option<JoinHandle<proven_core::Result<()>>>,

    // state
    started: bool,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Bootstrap {
    pub fn new(args: InitializeRequest) -> Self {
        let nsm = NsmAttestor::new();
        let network_definition = if args.stokenet {
            NetworkDefinition::stokenet()
        } else {
            NetworkDefinition::mainnet()
        };

        Self {
            args,
            nsm,
            network_definition,

            imds_identity: None,
            instance_details: None,

            proxy: None,
            proxy_handle: None,

            dnscrypt_proxy: None,
            dnscrypt_proxy_handle: None,

            radix_node_fs: None,
            radix_node_fs_handle: None,

            radix_node: None,
            radix_node_handle: None,

            postgres_fs: None,
            postgres_fs_handle: None,

            postgres: None,
            postgres_handle: None,

            radix_aggregator: None,
            radix_aggregator_handle: None,

            radix_gateway: None,
            radix_gateway_handle: None,

            nats_server_fs: None,
            nats_server_fs_handle: None,

            nats_client: None,
            nats_server: None,
            nats_server_handle: None,

            core: None,
            core_handle: None,

            started: false,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::large_stack_frames)]
    pub async fn initialize(mut self) -> Result<Enclave> {
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
        let radix_node_handle = self.radix_node_handle.take().unwrap();
        let postgres_fs_handle = self.postgres_fs_handle.take().unwrap();
        let postgres_handle = self.postgres_handle.take().unwrap();
        let radix_aggregator_handle = self.radix_aggregator_handle.take().unwrap();
        let radix_gateway_handle = self.radix_gateway_handle.take().unwrap();
        let nats_server_fs_handle = self.nats_server_fs_handle.take().unwrap();
        let nats_server_handle = self.nats_server_handle.take().unwrap();
        let core_handle = self.core_handle.take().unwrap();

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

        let enclave_services = Services {
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
            // Tasks that must be running for the enclave to function
            let critical_tasks = tokio::spawn(async move {
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
                    Ok(Err(e)) = radix_node_handle => {
                        error!("radix_node exited: {:?}", e);
                    }
                    Ok(Err(e)) = postgres_fs_handle => {
                        error!("postgres_external_fs exited: {:?}", e);
                    }
                    Ok(Err(e)) = postgres_handle => {
                        error!("postgres exited: {:?}", e);
                    }
                    Ok(Err(e)) = radix_aggregator_handle => {
                        error!("radix_aggregator exited: {:?}", e);
                    }
                    Ok(Err(e)) = radix_gateway_handle => {
                        error!("radix_gateway exited: {:?}", e);
                    }
                    Ok(Err(e)) = nats_server_fs_handle => {
                        error!("nats_external_fs exited: {:?}", e);
                    }
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

            core.lock().await.shutdown().await;
            nats_server.lock().await.shutdown().await;
            nats_server_fs.lock().await.shutdown().await;
            radix_gateway.lock().await.shutdown().await;
            radix_aggregator.lock().await.shutdown().await;
            radix_node.lock().await.shutdown().await;
            radix_node_fs.lock().await.shutdown().await;
            postgres.lock().await.shutdown().await;
            postgres_fs.lock().await.shutdown().await;
            dnscrypt_proxy.lock().await.shutdown().await;
            proxy.lock().await.shutdown().await;
        });

        self.task_tracker.close();

        let enclave = Enclave::new(
            self.nsm.clone(),
            self.network_definition.clone(),
            self.imds_identity.take().unwrap(),
            self.instance_details.take().unwrap(),
            enclave_services,
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

        if let Some(nats_server_fs) = self.nats_server_fs {
            nats_server_fs.shutdown().await;
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

        if let Some(postgres_fs) = self.postgres_fs {
            postgres_fs.shutdown().await;
        }

        if let Some(radix_node) = self.radix_node {
            radix_node.shutdown().await;
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
        let secured_random_bytes = self.nsm.secure_random().await?;
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

    async fn start_radix_node_fs(&mut self) -> Result<()> {
        let radix_node_fs = ExternalFs::new(ExternalFsOptions {
            encryption_key: "your-password".to_string(),
            nfs_mount_point: format!("{}/babylon/", self.args.nfs_mount_point),
            mount_dir: RADIX_NODE_STORE_DIR.to_string(),
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
            host_ip,
            network_definition: self.network_definition.clone(),
            store_dir: RADIX_NODE_STORE_DIR.to_string(),
        });

        let radix_node_handle = radix_node.start().await?;

        self.radix_node = Some(radix_node);
        self.radix_node_handle = Some(radix_node_handle);

        info!("radix-node started");

        Ok(())
    }

    async fn start_postgres_fs(&mut self) -> Result<()> {
        let postgres_fs = ExternalFs::new(ExternalFsOptions {
            encryption_key: "your-password".to_string(),
            nfs_mount_point: format!("{}/postgres/", self.args.nfs_mount_point),
            mount_dir: POSTGRES_STORE_DIR.to_string(),
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
            username: POSTGRES_USERNAME.to_string(),
            skip_vacuum: self.args.skip_vacuum,
            store_dir: POSTGRES_STORE_DIR.to_string(),
        });

        let postgres_handle = postgres.start().await?;

        self.postgres = Some(postgres);
        self.postgres_handle = Some(postgres_handle);

        info!("postgres started");

        Ok(())
    }

    async fn start_radix_aggregator(&mut self) -> Result<()> {
        let radix_aggregator = RadixAggregator::new(RadixAggregatorOptions {
            postgres_database: POSTGRES_DATABASE.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_username: POSTGRES_USERNAME.to_string(),
        });

        let radix_aggregator_handle = radix_aggregator.start().await?;

        self.radix_aggregator = Some(radix_aggregator);
        self.radix_aggregator_handle = Some(radix_aggregator_handle);

        info!("radix-aggregator started");

        Ok(())
    }

    async fn start_radix_gateway(&mut self) -> Result<()> {
        let radix_gateway = RadixGateway::new(RadixGatewayOptions {
            postgres_database: POSTGRES_DATABASE.to_string(),
            postgres_password: POSTGRES_PASSWORD.to_string(),
            postgres_username: POSTGRES_USERNAME.to_string(),
        });

        let radix_gateway_handle = radix_gateway.start().await?;

        self.radix_gateway = Some(radix_gateway);
        self.radix_gateway_handle = Some(radix_gateway_handle);

        info!("radix-gateway started");

        Ok(())
    }

    async fn start_nats_fs(&mut self) -> Result<()> {
        let nats_server_fs = ExternalFs::new(ExternalFsOptions {
            encryption_key: "your-password".to_string(),
            nfs_mount_point: format!("{}/nats/", self.args.nfs_mount_point),
            mount_dir: "/var/lib/nats".to_string(),
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

        let nats_server = NatsServer::new(NatsServerOptions {
            debug: self.args.stokenet,
            listen_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.args.nats_port),
            server_name: instance_details.instance_id.clone(),
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
        let id = self.imds_identity.as_ref().unwrap_or_else(|| {
            panic!("imds identity not fetched before core");
        });

        let instance_details = self.instance_details.as_ref().unwrap_or_else(|| {
            panic!("instance details not fetched before core");
        });

        let nats_client = self.nats_client.as_ref().unwrap_or_else(|| {
            panic!("nats client not fetched before core");
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
            attestor: self.nsm.clone(),
            challenge_store,
            sessions_store,
            radix_gateway_origin: GATEWAY_URL,
            radix_network_definition: &self.network_definition,
        });

        let cert_store = S3Store::new(S3StoreOptions {
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
        .await;

        let cluster_fqdn = format!("{}.{}", id.region.clone(), self.args.fqdn.clone());
        let node_fqdn = format!("{}.{}", instance_details.instance_id.clone(), cluster_fqdn);
        let domains = vec![node_fqdn, cluster_fqdn, self.args.fqdn.clone()];

        let http_sock_addr = SocketAddr::from((self.args.enclave_ip, self.args.https_port));
        let http_server = LetsEncryptHttpServer::new(LetsEncryptHttpServerOptions {
            cert_store,
            cname_domain: self.args.fqdn.clone(),
            domains: domains.clone(),
            emails: self.args.email.clone(),
            listen_addr: http_sock_addr,
        });

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
                S3Store1::new(S3StoreOptions {
                    bucket: self.args.certificates_bucket.clone(),
                    prefix: Some("application_manager".to_string()),
                    region: id.region.clone(),
                    secret_key: get_or_init_encrypted_key(
                        id.region.clone(),
                        self.args.kms_key_id.clone(),
                        "APPLICATION_MANAGER_SNAPSHOTS_KEY".to_string(),
                    )
                    .await?,
                })
                .await,
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
            radix_gateway_origin: GATEWAY_URL.to_string(),
            radix_network_definition: self.network_definition.clone(),
            radix_nft_verifier,
        })
        .await;

        let core = Core::new(CoreOptions {
            application_manager,
            attestor: self.nsm.clone(),
            governance: MockGovernance::new(Vec::new(), Vec::new()),
            primary_hostnames: domains
                .iter()
                .map(|domain| format!("https://{domain}"))
                .collect(),
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
    let kms = Kms::new(key_id, region.clone()).await;

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
