use crate::error::{Error, Result};
use crate::nitro::NitroCli;
use crate::ConnectArgs;

use proven_vsock_tracing::host::TracingService;
use tracing::info;

pub async fn connect(args: ConnectArgs) -> Result<()> {
    if !NitroCli::is_enclave_running().await? {
        return Err(Error::NoRunningEnclave);
    }

    let tracing_service = TracingService::new();
    let tracing_handle = tracing_service.start(args.log_port)?;

    info!("Connected to enclave logs. Press Ctrl+C to exit.");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down...");
            tracing_service.shutdown().await;
            tracing_handle.await.unwrap();
        }
    }

    Ok(())
}
