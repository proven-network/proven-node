#![allow(clippy::result_large_err)]
mod enclave;
mod enclave_manager;
mod error;
mod net;

use enclave_manager::EnclaveManager;
use error::Result;

use std::sync::Arc;

use proven_vsock_rpc::{handle_commands, listen_for_command, Command, InitializeArgs};
use tokio_vsock::{VsockAddr, VMADDR_CID_ANY};
use tracing::error;

#[tokio::main(worker_threads = 10)]
async fn main() -> Result<()> {
    let initial_command = listen_for_command(VsockAddr::new(VMADDR_CID_ANY, 1024)).await?;

    match initial_command {
        Command::Initialize(args) => {
            initialize(args).await?;
        }
        _ => {
            error!("not initialized - cannot handle other commands");
        }
    }

    Ok(())
}

async fn initialize(args: InitializeArgs) -> Result<()> {
    let enclave_manager = Arc::new(EnclaveManager::new());
    let enclave = enclave_manager.start(args).await?;

    handle_commands(
        VsockAddr::new(VMADDR_CID_ANY, 1024), // Control port is always at 1024
        move |command| {
            let enclave_manager = enclave_manager.clone();
            let enclave = enclave.clone();

            async move {
                match command {
                    Command::Initialize(_) => {
                        error!("already initialized");
                    }
                    Command::Shutdown => {
                        enclave_manager.shutdown().await;
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
