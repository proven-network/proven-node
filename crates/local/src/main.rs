mod error;

use error::Result;
use std::net::{Ipv4Addr, SocketAddr};

use clap::Parser;
use proven_attestation_dev::DevAttestor;
use proven_core::{Core, NewCoreArguments};
use proven_http_insecure::InsecureHttpServer;
use proven_sessions::{SessionManagement, SessionManager};
use proven_store_fs::FsStore;
use proven_store_memory::MemoryStore;
use radix_common::network::NetworkDefinition;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = 3200)]
    port: u16,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish(),
    )
    .unwrap();

    let args = Args::parse();
    let challenge_store = MemoryStore::new();
    let sessions_store = FsStore::new("/tmp/proven/sessions");
    let network_definition = NetworkDefinition::stokenet();
    let dev_attestor = DevAttestor::new();

    let gateway_origin = "https://stokenet.radixdlt.com".to_string();

    let session_manager = SessionManager::new(
        dev_attestor,
        challenge_store,
        gateway_origin.clone(),
        sessions_store,
        network_definition,
    );

    let core = Core::new(NewCoreArguments { session_manager });

    let http_sock_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));
    let http_server = InsecureHttpServer::new(http_sock_addr);

    let application_store = FsStore::new("/tmp/proven/application");
    let personal_store = FsStore::new("/tmp/proven/personal");
    let nft_store = FsStore::new("/tmp/proven/nft");
    let core_handle = core
        .start(
            http_server,
            application_store,
            personal_store,
            nft_store,
            gateway_origin,
        )
        .await?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down");
            let _ = core.shutdown().await;
        }
        _ = core_handle => {
            error!("Core exited");
        }
    }

    Ok(())
}
