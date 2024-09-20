use crate::enclave::Enclave;
use crate::error::{Error, Result};
use crate::net::{bring_up_loopback, setup_default_gateway, write_dns_resolv};

use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use proven_attestation::Attestor;
use proven_attestation_nsm::NsmAttestor;
use proven_babylon_aggregator::BabylonAggregator;
use proven_babylon_gateway::BabylonGateway;
use proven_babylon_node::BabylonNode;
use proven_core::{Core, NewCoreArguments};
use proven_dnscrypt_proxy::DnscryptProxy;
use proven_external_fs::ExternalFs;
use proven_http_letsencrypt::LetsEncryptHttpServer;
use proven_imds::{IdentityDocument, Imds};
use proven_instance_details::{Instance, InstanceDetailsFetcher};
use proven_kms::Kms;
// use proven_nats_monitor::NatsMonitor;
use proven_nats_server::NatsServer;
use proven_postgres::Postgres;
use proven_sessions::{SessionManagement, SessionManager};
use proven_store::Store;
use proven_store_asm::AsmStore;
use proven_store_nats::{NatsKeyValueConfig, NatsStore};
use proven_store_s3_sse_c::S3Store;
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::InitializeRequest;
use proven_vsock_tracing::configure_logging_to_vsock;
use radix_common::network::NetworkDefinition;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockStream};
use tracing::{error, info};
use tracing_panic::panic_hook;

static VMADDR_CID_EC2_HOST: u32 = 3;

static POSTGRES_USERNAME: &str = "your-username";
static POSTGRES_PASSWORD: &str = "your-password";
static POSTGRES_DATABASE: &str = "babylon-db";
pub struct EnclaveBootstrap {
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl EnclaveBootstrap {
    pub fn new() -> Self {
        Self {
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start(&self, args: InitializeRequest) -> Result<Arc<Mutex<Enclave>>> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyStarted);
        }

        // Configure tracing
        std::panic::set_hook(Box::new(panic_hook));
        configure_logging_to_vsock(VsockAddr::new(VMADDR_CID_EC2_HOST, args.log_port)).await?;

        info!("tracing configured");

        remount_tmp_with_exec().await?;

        info!("tmp remounted with exec (babylon snappy java fix)");

        // Configure network
        write_dns_resolv(args.host_dns_resolv)?; // Use host's DNS resolver until dnscrypt-proxy is up
        bring_up_loopback().await?;

        let vsock_stream =
            VsockStream::connect(VsockAddr::new(VMADDR_CID_EC2_HOST, args.proxy_port))
                .await
                .unwrap();

        let proxy = Proxy::new(
            args.enclave_ip,
            args.host_ip,
            args.cidr,
            args.tun_device.clone(),
        )
        .await?;
        setup_default_gateway(args.tun_device.as_str(), args.host_ip, args.cidr).await?;
        let proxy_handle = proxy.start(vsock_stream);

        info!("network configured");

        // Seed entropy
        let nsm = NsmAttestor::new();
        let secured_random_bytes = nsm.secure_random().await?;
        let mut rng = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/random")?;
        std::io::Write::write_all(&mut rng, &secured_random_bytes)?;

        info!("entropy seeded");

        // Fetch validated identity from IMDS
        let identity = fetch_imds_identity().await?;
        info!("identity: {:?}", identity);

        let instance =
            fetch_instance_details(identity.region.clone(), identity.instance_id.clone()).await?;
        info!("instance: {:?}", instance);
        let server_name = instance.instance_id.clone();

        if args.skip_speedtest {
            info!("skipping speedtest...");
        } else {
            info!("running speedtest...");
            run_speedtest().await?;
        }

        // Boot dnscrypt-proxy
        let dnscrypt_proxy = DnscryptProxy::new(
            identity.region.clone(),
            instance.vpc_id,
            instance.availability_zone,
            instance.subnet_id,
        );
        let dnscrypt_proxy_handle = dnscrypt_proxy.start().await?;
        write_dns_resolv("nameserver 127.0.0.1".to_string())?; // Switch to dnscrypt-proxy's DNS resolver

        // Boot postgres
        let postgres_store_dir = "/var/lib/postgres".to_string();
        let postgres_external_fs = ExternalFs::new(
            "your-password".to_string(),
            "fs-035b691e876c20f4c.fsx.us-east-2.amazonaws.com:/fsx/postgres/".to_string(),
            postgres_store_dir.clone(),
            args.skip_fsck,
        );
        let postgres_external_fs_handle = postgres_external_fs.start().await?;

        let postgres = Postgres::new(
            postgres_store_dir,
            POSTGRES_USERNAME.to_string(),
            POSTGRES_PASSWORD.to_string(),
        );
        let postgres_handle = postgres.start().await?;

        // Boot babylon node
        let babylon_node_store_dir = "/var/lib/babylon".to_string();
        let babylon_node_external_fs = ExternalFs::new(
            "your-password".to_string(),
            "fs-035b691e876c20f4c.fsx.us-east-2.amazonaws.com:/fsx/babylon/".to_string(),
            babylon_node_store_dir.clone(),
            args.skip_fsck,
        );
        let babylon_external_fs_handle = babylon_node_external_fs.start().await?;

        let network_definition = match args.stokenet {
            true => NetworkDefinition::stokenet(),
            false => NetworkDefinition::mainnet(),
        };

        let babylon_node = BabylonNode::new(network_definition.clone(), babylon_node_store_dir);
        let babylon_node_handle = babylon_node.start().await?;

        // Boot babylon aggregator
        let babylon_aggregator = BabylonAggregator::new(
            POSTGRES_DATABASE.to_string(),
            POSTGRES_USERNAME.to_string(),
            POSTGRES_PASSWORD.to_string(),
        );
        let babylon_aggregator_handle = babylon_aggregator.start().await?;

        // Boot babylon gateway
        let babylon_gateway = BabylonGateway::new(
            POSTGRES_DATABASE.to_string(),
            POSTGRES_USERNAME.to_string(),
            POSTGRES_PASSWORD.to_string(),
        );
        let babylon_gateway_handle = babylon_gateway.start().await?;

        // Boot NATS server
        let nats_store_dir = "/var/lib/nats".to_string();
        let nats_external_fs = ExternalFs::new(
            "your-password".to_string(),
            "fs-035b691e876c20f4c.fsx.us-east-2.amazonaws.com:/fsx/nats/".to_string(),
            nats_store_dir.clone(),
            args.skip_fsck,
        );
        let nats_external_fs_handle = nats_external_fs.start().await?;
        let nats_server = NatsServer::new(
            server_name.clone(),
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, args.nats_port),
            format!("{}/nats", nats_store_dir),
        );
        let nats_server_handle = nats_server.start().await?;
        let nats_client = nats_server.build_client().await?;

        // let nats_monitor = NatsMonitor::new(8222);
        // let varz = nats_monitor.get_varz().await?;
        // info!("nats varz: {:?}", varz);
        // let connz = nats_monitor.get_connz().await?;
        // info!("nats connz: {:?}", connz);

        let challenge_store = NatsStore::new(
            nats_client.clone(),
            NatsKeyValueConfig {
                bucket: "challenges".to_string(),
                max_age: Duration::from_secs(5 * 60), // 5 minutes
                ..Default::default()
            },
        )
        .await?;
        let sessions_store = NatsStore::new(
            nats_client.clone(),
            NatsKeyValueConfig {
                bucket: "sessions".to_string(),
                ..Default::default()
            },
        )
        .await?;

        let session_manager =
            SessionManager::new(nsm, challenge_store, sessions_store, network_definition);

        let cert_store = S3Store::new(
            args.certificates_bucket,
            identity.region.clone(),
            get_or_init_encrypted_key(identity.region.clone(), "CERTIFICATES_KEY".to_string())
                .await?,
        )
        .await;
        let cluster_fqdn = format!("{}.{}", identity.region, args.fqdn.clone());
        let node_fqdn = format!("{}.{}", identity.instance_id, cluster_fqdn);
        let domains = vec![node_fqdn, cluster_fqdn, args.fqdn.clone()];
        let http_sock_addr = SocketAddr::from((args.enclave_ip, args.https_port));
        let http_server =
            LetsEncryptHttpServer::new(http_sock_addr, domains, args.email, cert_store);

        let core = Core::new(NewCoreArguments { session_manager });
        let core_handle = core.start(http_server).await?;

        let enclave = Arc::new(Mutex::new(Enclave::new(core, nats_server)));

        let shutdown_token = self.shutdown_token.clone();
        let enclave_clone = enclave.clone();
        self.task_tracker.spawn(async move {
            // Tasks that must be running for the enclave to function
            let critical_tasks = tokio::spawn(async move {
                tokio::select! {
                    Ok(Err(e)) = babylon_aggregator_handle => {
                        error!("babylon_aggregator exited: {:?}", e);
                    }
                    Ok(Err(e)) = babylon_external_fs_handle => {
                        error!("babylon_external_fs exited: {:?}", e);
                    }
                    Ok(Err(e)) = babylon_gateway_handle => {
                        error!("babylon_gateway exited: {:?}", e);
                    }
                    Ok(Err(e)) = babylon_node_handle => {
                        error!("babylon_node exited: {:?}", e);
                    }
                    Ok(Err(e)) = dnscrypt_proxy_handle => {
                        error!("dnscrypt_proxy exited: {:?}", e);
                    }
                    Ok(Err(e)) = nats_external_fs_handle => {
                        error!("nats_external_fs exited: {:?}", e);
                    }
                    Ok(Err(e)) = nats_server_handle => {
                        error!("nats_server exited: {:?}", e);
                    }
                    Ok(Err(e)) = postgres_external_fs_handle => {
                        error!("postgres_external_fs exited: {:?}", e);
                    }
                    Ok(Err(e)) = postgres_handle => {
                        error!("postgres exited: {:?}", e);
                    }
                    Ok(Err(e)) = proxy_handle => {
                        error!("proxy exited: {:?}", e);
                    }
                    else => {
                        info!("enclave shutdown cleanly. goodbye.");
                    }
                }
            });

            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("shutdown command received. shutting down...");
                    enclave_clone.lock().await.shutdown().await;
                    nats_external_fs.shutdown().await;
                    babylon_gateway.shutdown().await;
                    babylon_aggregator.shutdown().await;
                    babylon_node.shutdown().await;
                    babylon_node_external_fs.shutdown().await;
                    postgres.shutdown().await;
                    dnscrypt_proxy.shutdown().await;
                    proxy.shutdown().await;
                }
                _ = critical_tasks => {
                    error!("critical task failed - exiting");
                    enclave_clone.lock().await.shutdown().await;
                    nats_external_fs.shutdown().await;
                    babylon_gateway.shutdown().await;
                    babylon_aggregator.shutdown().await;
                    babylon_node.shutdown().await;
                    babylon_node_external_fs.shutdown().await;
                    postgres.shutdown().await;
                    dnscrypt_proxy.shutdown().await;
                    proxy.shutdown().await;
                }
                _ = core_handle => {
                    error!("core exited unexpectedly - exiting");
                }
            }
        });

        self.task_tracker.close();

        Ok(enclave)
    }

    pub async fn shutdown(&self) {
        info!("enclave shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("enclave shutdown");
    }
}

async fn get_or_init_encrypted_key(region: String, key_name: String) -> Result<[u8; 32]> {
    let secret_id = format!("proven-{}", region.clone());
    let store = AsmStore::new(region.clone(), secret_id).await;
    let kms = Kms::new(
        "2aae0800-75c8-4ca1-aff4-8b1fc885a8ce".to_string(),
        region.clone(),
    )
    .await;

    let key_opt = store.get(key_name.clone()).await?;
    let key: [u8; 32] = match key_opt {
        Some(encrypted_key) => kms
            .decrypt(encrypted_key)
            .await?
            .try_into()
            .map_err(|_| Error::BadKey)?,
        None => {
            let unencrypted_key = rand::random::<[u8; 32]>();
            let encrypted_key = kms.encrypt(unencrypted_key.to_vec()).await?;
            store.put(key_name, encrypted_key).await?;
            unencrypted_key
        }
    };

    Ok(key)
}

async fn fetch_instance_details(region: String, instance_id: String) -> Result<Instance> {
    let fetcher = InstanceDetailsFetcher::new(region).await;
    let instance = fetcher.get_instance_details(instance_id).await?;

    Ok(instance)
}

async fn fetch_imds_identity() -> Result<IdentityDocument> {
    let imds = Imds::new().await?;
    let identity = imds.get_verified_identity_document().await?;

    Ok(identity)
}

async fn remount_tmp_with_exec() -> Result<()> {
    tokio::process::Command::new("mount")
        .arg("-o")
        .arg("remount,exec")
        .arg("tmpfs")
        .arg("/tmp")
        .output()
        .await?;

    Ok(())
}

// Run speedtest and log the results
async fn run_speedtest() -> Result<()> {
    let cmd = tokio::process::Command::new("librespeed-cli")
        .arg("--json")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .await?;

    info!("speedtest results: {:?}", cmd);

    Ok(())
}
