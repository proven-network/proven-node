//! Binary to bootstrap other components locally.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

use error::Result;
use std::net::{Ipv4Addr, SocketAddr};

use clap::Parser;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation_dev::DevAttestor;
use proven_core::{Core, CoreOptions};
use proven_http_insecure::InsecureHttpServer;
use proven_runtime::{RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
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
    let attestor = DevAttestor;

    let radix_gateway_origin = "https://stokenet.radixdlt.com".to_string();

    let session_manager = SessionManager::new(SessionManagerOptions {
        attestor,
        challenge_store,
        sessions_store,
        radix_gateway_origin: radix_gateway_origin.clone(),
        radix_network_definition: radix_network_definition.clone(),
    });

    let application_manager_sql_store = DirectSqlStore::new("/tmp/proven/application_manager.db");
    let application_manager = ApplicationManager::new(application_manager_sql_store).await?;

    let application_store = FsStore::new("/tmp/proven/kv/application");
    let personal_store = FsStore::new("/tmp/proven/kv/personal");
    let nft_store = FsStore::new("/tmp/proven/kv/nft");

    let application_sql_store = DirectSqlStore::new("/tmp/proven/sql/application");
    let personal_sql_store = DirectSqlStore::new("/tmp/proven/sql/personal");
    let nft_sql_store = DirectSqlStore::new("/tmp/proven/sql/nft");

    let runtime_pool_manager = RuntimePoolManager::new(RuntimePoolManagerOptions {
        application_sql_store,
        application_store,
        max_workers: 10,
        nft_sql_store,
        nft_store,
        personal_sql_store,
        personal_store,
        radix_gateway_origin,
        radix_network_definition,
    })
    .await;

    let core = Core::new(CoreOptions {
        application_manager,
        runtime_pool_manager,
        session_manager,
    });

    let http_sock_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));
    let http_server = InsecureHttpServer::new(http_sock_addr);

    let core_handle = core.start(http_server)?;

    info!("listening on http://{http_sock_addr}");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down");
            let () = core.shutdown().await;
        }
        _ = core_handle => {
            error!("Core exited");
        }
    }

    Ok(())
}
