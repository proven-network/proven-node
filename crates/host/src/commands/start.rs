use crate::StartArgs;
use crate::error::{Error, Result};
use crate::net::{configure_nat, configure_port_forwarding, configure_route};
use crate::nitro::NitroCli;
use crate::systemctl;

use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;

use axum::Router;
use axum::http::Uri;
use axum::response::Redirect;
use axum::routing::any;
use nix::unistd::Uid;
use proven_bootable::Bootable;
use proven_http_insecure::InsecureHttpServer;
use proven_logger_vsock::server::{StdoutLogProcessor, VsockLogServerBuilder};
use proven_vsock_cac::{CacClient, InitializeRequest};
use proven_vsock_proxy::Proxy;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info};

static ALLOCATOR_CONFIG_TEMPLATE: &str = include_str!("../../templates/allocator.yaml");

#[allow(clippy::cognitive_complexity)]
pub async fn start(args: StartArgs) -> Result<()> {
    if !Uid::effective().is_root() {
        return Err(Error::NotRoot);
    }

    if !Path::new(&args.eif_path).exists() {
        return Err(Error::EifDoesNotExist(args.eif_path.clone()));
    }

    if NitroCli::is_enclave_running().await? {
        return Err(Error::EnclaveAlreadyRunning);
    }

    info!("allocating enclave resources...");
    allocate_enclave_resources(args.enclave_cpus, args.enclave_memory)?;

    // Calculate TUN mask from CIDR
    let tun_mask = args.cidr.network_length();

    let proxy = Proxy::new(args.host_ip, tun_mask, args.proxy_port, true);
    let proxy_handle = proxy.start().await?;

    NitroCli::run_enclave(
        args.enclave_cpus,
        args.enclave_memory,
        args.enclave_cid,
        args.eif_path.clone(),
    )
    .await?;

    // sleep for a bit to allow everything to start
    // TODO: replace with a more robust mechanism
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    // Enclave (vsock-based) logging
    // The new logger-vsock server listens on the host side
    #[cfg(target_os = "linux")]
    let log_addr = tokio_vsock::VsockAddr::new(tokio_vsock::VMADDR_CID_ANY, 5555);

    #[cfg(not(target_os = "linux"))]
    let log_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 5555));

    let log_server = VsockLogServerBuilder::new(log_addr)
        .processor(std::sync::Arc::new(StdoutLogProcessor))
        .build()
        .await?;

    let log_server_handle = tokio::spawn(async move {
        if let Err(e) = log_server.serve().await {
            error!("Log server error: {}", e);
        }
    });

    configure_nat(&args.outbound_device, args.cidr).await?;
    configure_route(&args.tun_device, args.cidr, args.enclave_ip).await?;
    configure_port_forwarding(args.host_ip, args.enclave_ip, &args.outbound_device).await?;

    let fqdn = args.fqdn.clone();
    let fallback_router = Router::new().fallback(any(move |uri: Uri| {
        let fqdn = fqdn.clone();
        async move {
            let https_uri = format!("https://{fqdn}{uri}");
            Redirect::permanent(&https_uri)
        }
    }));

    let http_server = InsecureHttpServer::new(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 80)),
        fallback_router,
    );

    http_server.start().await.map_err(Error::Bootable)?;

    // Tasks that must be running for the host to function
    let http_server_clone = http_server.clone();
    let critical_tasks = tokio::spawn(async move {
        tokio::select! {
            () = http_server_clone.wait() => {
                error!("http_server exited");
            }
            result = proxy_handle => {
                match result {
                    Ok(Ok(())) => info!("proxy exited normally"),
                    Ok(Err(e)) => error!("proxy error: {}", e),
                    Err(e) => error!("proxy task error: {}", e),
                }
            }
            else => {
                info!("all critical tasks exited normally");
                log_server_handle.abort();
            }
        }
    });

    initialize_enclave(&args).await?;

    info!("enclave initialized successfully");

    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| Error::Io("failed to create SIGTERM signal", e))?;

    tokio::select! {
        _ = sigterm.recv() => {
            info!("received SIGTERM, initiating shutdown");
            shutdown_enclave(#[cfg(target_os = "linux")] &args).await?;
            let _ = http_server.shutdown().await;
            proxy.shutdown().await;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received SIGINT, initiating shutdown");
            shutdown_enclave(#[cfg(target_os = "linux")] &args).await?;
            let _ = http_server.shutdown().await;
            proxy.shutdown().await;
        }
        _ = critical_tasks => {
            error!("critical task failed - exiting");
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    info!("deallocating enclave resources...");
    allocate_enclave_resources(1, 0)?;
    info!("host shutdown cleanly. goodbye.");

    Ok(())
}

fn allocate_enclave_resources(enclave_cpus: u8, enclave_memory: u32) -> Result<()> {
    let existing_allocator_config = std::fs::read_to_string("/etc/nitro_enclaves/allocator.yaml")
        .map_err(|e| Error::Io("failed to read allocator config", e))?;
    if existing_allocator_config.contains(&format!("cpu_count: {enclave_cpus}"))
        && existing_allocator_config.contains(&format!("memory_mib: {enclave_memory}"))
    {
        return Ok(());
    }

    let allocator_config = ALLOCATOR_CONFIG_TEMPLATE
        .replace("{memory_mib}", &enclave_memory.to_string())
        .replace("{cpu_count}", &enclave_cpus.to_string());

    std::fs::write("/etc/nitro_enclaves/allocator.yaml", allocator_config)
        .map_err(|e| Error::Io("failed to write allocator config", e))?;

    systemctl::restart_allocator_service()?;

    Ok(())
}

async fn initialize_enclave(args: &StartArgs) -> Result<()> {
    let host_dns_resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap();

    let client = CacClient::new(
        #[cfg(target_os = "linux")]
        VsockAddr::new(args.enclave_cid, 1024),
        #[cfg(not(target_os = "linux"))]
        SocketAddr::from((Ipv4Addr::LOCALHOST, 1024)),
    )?;
    let res = client
        .initialize(InitializeRequest {
            certificates_bucket: args.certificates_bucket.clone(),
            cidr: args.cidr,
            email: args.email.clone(),
            enclave_ip: args.enclave_ip,
            file_systems_bucket: args.file_systems_bucket.clone(),
            host_dns_resolv,
            host_ip: args.host_ip,
            https_port: args.https_port,
            kms_key_id: args.kms_key_id.clone(),
            max_runtime_workers: args.max_runtime_workers,
            nfs_mount_point: args.nfs_mount_point.clone(),
            node_key: args.node_key.clone(),
            proxy_port: args.proxy_port,
            radix_mainnet_port: args.radix_mainnet_port,
            radix_stokenet_port: args.radix_stokenet_port,
            skip_fsck: args.skip_fsck,
            skip_speedtest: args.skip_speedtest,
            skip_vacuum: args.skip_vacuum,
            sql_snapshots_bucket: args.sql_snapshots_bucket.clone(),
            testnet: args.testnet,
        })
        .await;

    info!("initialize response: {:?}", res);

    Ok(())
}

async fn shutdown_enclave(#[cfg(target_os = "linux")] args: &StartArgs) -> Result<()> {
    let client = CacClient::new(
        #[cfg(target_os = "linux")]
        tokio_vsock::VsockAddr::new(args.enclave_cid, 1024),
        #[cfg(not(target_os = "linux"))]
        SocketAddr::from((Ipv4Addr::LOCALHOST, 1024)),
    )?;
    let res = client.shutdown().await;

    info!("shutdown response: {:?}", res);

    Ok(())
}
