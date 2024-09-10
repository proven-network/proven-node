#![allow(clippy::result_large_err)]
mod enclave;
mod enclave_manager;
mod error;
mod net;

use enclave::Enclave;
use enclave_manager::EnclaveManager;
use error::Result;

use std::sync::Arc;

use proven_vsock_rpc::{listen_for_commands, Command};
use tokio::sync::Mutex;
use tokio_vsock::{VsockAddr, VMADDR_CID_ANY};
use tracing::error;

#[tokio::main(worker_threads = 10)]
async fn main() -> Result<()> {
    let enclave_manager_opt: Arc<Mutex<Option<EnclaveManager>>> = Arc::new(Mutex::new(None));
    let enclave_opt: Arc<Mutex<Option<Arc<Mutex<Enclave>>>>> = Arc::new(Mutex::new(None));

    listen_for_commands(
        VsockAddr::new(VMADDR_CID_ANY, 1024), // Control port is always at 1024
        move |command| {
            let enclave_manager_opt = enclave_manager_opt.clone();
            let enclave_opt = enclave_opt.clone();

            async move {
                match (command, enclave_manager_opt.lock().await.as_ref()) {
                    (Command::Initialize(args), None) => {
                        let enclave_manager = EnclaveManager::new();

                        match enclave_manager.start(args).await {
                            Ok(enclave) => {
                                enclave_manager_opt.lock().await.replace(enclave_manager);
                                enclave_opt.lock().await.replace(enclave);
                            }
                            Err(e) => {
                                error!("initialize failed: {:?}", e);
                            }
                        }
                    }
                    (_, None) => {
                        error!("enclave not initialized");
                    }
                    (Command::Shutdown, Some(enclave_manager)) => {
                        enclave_manager.shutdown().await;
                    }
                    (command, Some(_)) => match enclave_opt.lock().await.as_ref() {
                        Some(enclave) => {
                            enclave.lock().await.handle_command(command).await;
                        }
                        None => {
                            error!("enclave not initialized");
                        }
                    },
                }
            }
        },
    )
    .await?;

    Ok(())
}
