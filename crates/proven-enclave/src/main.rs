#![allow(clippy::result_large_err)]
mod enclave;
mod enclave_bootstrap;
mod error;
mod net;

use enclave_bootstrap::EnclaveBootstrap;
use error::Result;

use std::sync::Arc;

use proven_vsock_rpc::{
    handle_commands, listen_for_command, Acknowledger, Command, InitializeArgs,
};
use tokio_vsock::{VsockAddr, VMADDR_CID_ANY};
use tracing::error;

#[tokio::main(worker_threads = 12)]
async fn main() -> Result<()> {
    let (command, acknowledger) = listen_for_command(VsockAddr::new(VMADDR_CID_ANY, 1024)).await?;

    match command {
        Command::Initialize(args) => {
            if let Err(error) = initialize(args, acknowledger).await {
                error!("failed to initialize: {:?}", error);
            }
        }
        _ => {
            error!("not initialized - cannot handle other commands");
        }
    }

    Ok(())
}

async fn initialize(args: InitializeArgs, acknowledger: Acknowledger) -> Result<()> {
    let enclave_bootstrap = Arc::new(EnclaveBootstrap::new());
    let enclave = enclave_bootstrap.start(args).await?;

    acknowledger(1).await?;

    handle_commands(
        VsockAddr::new(VMADDR_CID_ANY, 1024), // Control port is always at 1024
        move |command| {
            let enclave_bootstrap = enclave_bootstrap.clone();
            let enclave = enclave.clone();

            async move {
                match command {
                    Command::Initialize(_) => {
                        error!("already initialized");
                    }
                    Command::Shutdown => {
                        enclave_bootstrap.shutdown().await;
                    }
                    command => {
                        enclave.lock().await.handle_command(command).await;
                    }
                }
            }
        },
    )
    .await?;

    Ok(())
}
