#![allow(clippy::result_large_err)]
mod error;
mod net;

use error::{Error, Result};
use net::{bring_up_loopback, setup_default_gateway, write_dns_resolv};

use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use proven_attestation::Attestor;
use proven_attestation_nsm::NsmAttestor;
use proven_core::{Core, NewCoreArguments};
use proven_dnscrypt_proxy::DnscryptProxy;
use proven_http_letsencrypt::LetsEncryptHttpServer;
use proven_imds::{IdentityDocument, Imds};
use proven_instance_details::{Instance, InstanceDetailsFetcher};
use proven_kms::Kms;
use proven_nats_server::NatsServer;
use proven_sessions::{SessionManagement, SessionManager};
use proven_store::Store;
use proven_store_asm::AsmStore;
use proven_store_memory::MemoryStore;
use proven_store_s3_sse_c::S3Store;
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::{listen_for_commands, Command, InitializeArgs};
use proven_vsock_tracing::configure_logging_to_vsock;
use radix_common::network::NetworkDefinition;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockStream, VMADDR_CID_ANY};
use tracing::{error, info};
use tracing_panic::panic_hook;

static VMADDR_CID_EC2_HOST: u32 = 3;

#[tokio::main(worker_threads = 10)]
async fn main() -> Result<()> {
    let shutdown_token = CancellationToken::new();
    let task_tracker = TaskTracker::new();

    listen_for_commands(
        VsockAddr::new(VMADDR_CID_ANY, 1024), // Control port is always at 1024
        move |command| {
            let shutdown_token = shutdown_token.clone();
            let task_tracker = task_tracker.clone();

            async move {
                match command {
                    Command::Initialize(args) => {
                        if task_tracker.is_closed() {
                            return;
                        }

                        task_tracker.spawn(async move {
                            if let Err(e) = initialize(args, shutdown_token).await {
                                error!("initialize failed: {:?}", e);
                            }
                        });

                        task_tracker.close();
                    }
                    Command::Shutdown => {
                        shutdown_token.cancel();

                        task_tracker.wait().await;
                    }
                }
            }
        },
    )
    .await?;

    Ok(())
}

async fn initialize(args: InitializeArgs, shutdown_token: CancellationToken) -> Result<()> {
    // Configure tracing
    std::panic::set_hook(Box::new(panic_hook));
    configure_logging_to_vsock(VsockAddr::new(VMADDR_CID_EC2_HOST, args.log_port)).await?;

    info!("tracing configured");

    // Configure network
    write_dns_resolv(args.host_dns_resolv)?; // Use host's DNS resolver until dnscrypt-proxy is up
    bring_up_loopback().await?;

    let vsock_stream = VsockStream::connect(VsockAddr::new(VMADDR_CID_EC2_HOST, args.proxy_port))
        .await
        .unwrap();

    let proxy = Proxy::new(
        args.enclave_ip,
        args.host_ip,
        args.cidr,
        args.tun_device.clone(),
    );

    let connection_handler = proxy
        .start(async {
            setup_default_gateway(args.tun_device.as_str(), args.host_ip, args.cidr).await?;

            Ok(())
        })
        .await
        .unwrap();

    let proxy_ct = CancellationToken::new();
    let proxy_ct_clone = proxy_ct.clone();
    let proxy_handle =
        tokio::spawn(async move { connection_handler.proxy(vsock_stream, proxy_ct_clone).await });

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

    let instance = fetch_instance_details(identity.region.clone(), identity.instance_id).await?;
    info!("instance: {:?}", instance);
    let server_name = instance.instance_id.clone();

    // Boot dnscrypt-proxy
    let dnscrypt_proxy = DnscryptProxy::new(
        identity.region.clone(),
        instance.vpc_id,
        instance.availability_zone,
        instance.subnet_id,
    );
    let dnscrypt_proxy_handle = dnscrypt_proxy.start().await?;
    write_dns_resolv("nameserver 127.0.0.1".to_string())?; // Switch to dnscrypt-proxy's DNS resolver

    // Boot NATS server
    let nats_server = NatsServer::new(
        server_name,
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, args.nats_port),
    );
    let nats_server_handle = nats_server.start().await?;

    // Get secret from ASM and get or init sse base key
    let secret_id = format!("proven-{}", identity.region.clone());
    let store = AsmStore::new(identity.region.clone(), secret_id).await;
    let kms = Kms::new(
        "2aae0800-75c8-4ca1-aff4-8b1fc885a8ce".to_string(),
        identity.region.clone(),
    )
    .await;

    let s3_sse_c_base_key_opt = store.get("S3_SSE_C_BASE_KEY".to_string()).await?;
    let s3_sse_c_base_key: [u8; 32] = match s3_sse_c_base_key_opt {
        Some(encrypted_key) => kms
            .decrypt(encrypted_key)
            .await?
            .try_into()
            .map_err(|_| Error::Custom("bad value for S3_SSE_C_BASE_KEY".to_string()))?,
        None => {
            let unencrypted_key = rand::random::<[u8; 32]>();
            let encrypted_key = kms.encrypt(unencrypted_key.to_vec()).await?;
            store
                .put("S3_SSE_C_BASE_KEY".to_string(), encrypted_key)
                .await?;
            unencrypted_key
        }
    };

    let challenge_store = MemoryStore::new();
    let sessions_store = S3Store::new(
        "myduperprovenbucket".to_string(),
        identity.region,
        s3_sse_c_base_key,
    )
    .await;
    let network_definition = NetworkDefinition::stokenet();

    let session_manager =
        SessionManager::new(nsm, challenge_store, sessions_store, network_definition);

    let http_sock_addr = SocketAddr::from((args.enclave_ip, args.https_port));
    let http_server = LetsEncryptHttpServer::new(http_sock_addr, args.fqdn, args.email, store);

    let core = Core::new(NewCoreArguments { session_manager });
    let core_handle = core.start(http_server).await?;

    // Tasks that must be running for the enclave to function
    let critical_tasks = tokio::spawn(async move {
        tokio::select! {
            Ok(Err(e)) = dnscrypt_proxy_handle => {
                error!("dnscrypt_proxy exited: {:?}", e);
            }
            Ok(Err(e)) = nats_server_handle => {
                error!("nats_server exited: {:?}", e);
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
            core.shutdown().await;
            nats_server.shutdown().await;
            proxy_ct.cancel();
            dnscrypt_proxy.shutdown().await;
        }
        _ = critical_tasks => {
            error!("critical task failed - exiting");
            core.shutdown().await;
        }
        _ = core_handle => {
            error!("core exited unexpectedly - exiting");
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(())
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
