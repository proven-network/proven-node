//! Binary to bootstrap other components locally.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;
mod hosts;

use error::Result;
use hosts::check_hosts_file;

use std::net::{Ipv4Addr, SocketAddr};

use clap::Parser;
use console::style;
use proven_applications::{ApplicationManagement, ApplicationManager};
use proven_attestation_dev::DevAttestor;
use proven_core::{Core, CoreOptions};
use proven_http_insecure::InsecureHttpServer;
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::{RuntimePoolManagement, RuntimePoolManager, RuntimePoolManagerOptions};
use proven_sessions::{SessionManagement, SessionManager, SessionManagerOptions};
use proven_sql_direct::{DirectSqlStore1, DirectSqlStore2, DirectSqlStore3};
use proven_store_fs::{FsStore1, FsStore2, FsStore3};
use proven_store_memory::{MemoryStore, MemoryStore1};
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
    )?;

    // Check /etc/hosts to ensure proven.local is mapped to 127.0.0.1
    if !check_hosts_file("proven.local") {
        error!("proven.local is not configured in hosts file");
        #[cfg(target_family = "unix")]
        error!(
            "Please add {} to {}",
            style("127.0.0.1 proven.local").cyan(),
            style("/etc/hosts").blue(),
        );
        #[cfg(target_family = "windows")]
        error!(
            "Please add {} to {}",
            style("127.0.0.1 proven.local").cyan(),
            style(r"C:\Windows\System32\drivers\etc\hosts").blue()
        );
        std::process::exit(1);
    }

    let args = Args::parse();
    let challenge_store = MemoryStore1::new();
    let sessions_store = FsStore1::new("/tmp/proven/sessions");
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

    let application_manager = ApplicationManager::new(
        MemoryStore::new(),
        DirectSqlStore1::new("/tmp/proven/application_manager.db"),
    );

    let application_store = FsStore2::new("/tmp/proven/kv/application");
    let personal_store = FsStore3::new("/tmp/proven/kv/personal");
    let nft_store = FsStore3::new("/tmp/proven/kv/nft");

    let application_sql_store = DirectSqlStore2::new("/tmp/proven/sql/application");
    let personal_sql_store = DirectSqlStore3::new("/tmp/proven/sql/personal");
    let nft_sql_store = DirectSqlStore3::new("/tmp/proven/sql/nft");

    let radix_nft_verifier = GatewayRadixNftVerifier::new("https://stokenet.radixdlt.com");

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
        radix_nft_verifier,
    })
    .await;

    let core = Core::new(CoreOptions {
        application_manager,
        primary_hostnames: vec![format!("proven.local:{}", args.port)]
            .into_iter()
            .collect(),
        runtime_pool_manager,
        session_manager,
    });

    let http_sock_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));
    let http_server = InsecureHttpServer::new(http_sock_addr);

    let core_handle = core.start(http_server)?;

    info!("listening on http://proven.local:{}", args.port);

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
