use crate::commands::{start, stop};
use crate::error::{Error, Result};
use crate::{StartArgs, StopArgs};

use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

pub async fn daemon(args: StartArgs) -> Result<()> {
    // Start the enclave
    start(args.clone()).await?;

    info!("daemon running, waiting for shutdown signal");

    // Wait for SIGTERM
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| Error::Io("failed to create SIGTERM signal", e))?;

    tokio::select! {
        _ = sigterm.recv() => {
            info!("received SIGTERM, initiating shutdown");
        }
    }

    // Stop the enclave
    stop(StopArgs {
        enclave_cid: args.enclave_cid,
        force: false,
    })
    .await?;

    info!("daemon shutdown complete");
    Ok(())
}
