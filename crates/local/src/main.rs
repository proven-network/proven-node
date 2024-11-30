mod error;

use error::Result;
use std::net::{Ipv4Addr, SocketAddr};

use clap::Parser;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation_dev::DevAttestor;
use proven_core::{Core, CoreOptions, CoreStartOptions};
use proven_http_insecure::InsecureHttpServer;
use proven_sessions::{SessionManagement, SessionManager};
use proven_sql_direct::DirectSqlStore;
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
    let radix_network_definition = NetworkDefinition::stokenet();
    let dev_attestor = DevAttestor::new();

    let radix_gateway_origin = "https://stokenet.radixdlt.com".to_string();

    let session_manager = SessionManager::new(
        dev_attestor,
        challenge_store,
        sessions_store,
        radix_gateway_origin.clone(),
        radix_network_definition.clone(),
    );

    let application_manager_sql_store = DirectSqlStore::new("/tmp/proven/app_data");
    let application_manager = ApplicationManager::new(application_manager_sql_store).await?;

    let core = Core::new(CoreOptions {
        application_manager,
        session_manager,
    });

    let http_sock_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));
    let http_server = InsecureHttpServer::new(http_sock_addr);

    let application_store = FsStore::new("/tmp/proven/kv/application");
    let personal_store = FsStore::new("/tmp/proven/kv/personal");
    let nft_store = FsStore::new("/tmp/proven/kv/nft");

    let application_sql_store = DirectSqlStore::new("/tmp/proven/sql/application");
    let personal_sql_store = DirectSqlStore::new("/tmp/proven/sql/personal");
    let nft_sql_store = DirectSqlStore::new("/tmp/proven/sql/nft");

    let core_handle = core
        .start(CoreStartOptions {
            application_sql_store,
            application_store,
            http_server,
            personal_sql_store,
            personal_store,
            nft_sql_store,
            nft_store,
            radix_gateway_origin,
            radix_network_definition,
        })
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
