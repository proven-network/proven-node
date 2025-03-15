use crate::ConnectArgs;
use crate::error::{Error, Result};
use crate::nitro::NitroCli;

use proven_vsock_tracing::host::VsockTracingConsumer;
use tracing::info;

pub async fn connect(args: ConnectArgs) -> Result<()> {
    if !NitroCli::is_enclave_running().await? {
        return Err(Error::NoRunningEnclave);
    }

    let vsock_tracing_consumer = VsockTracingConsumer::new(args.enclave_cid);
    let vsock_tracing_consumer_handle = vsock_tracing_consumer.start()?;

    info!("Connected to enclave logs. Press Ctrl+C to exit.");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down...");
            vsock_tracing_consumer.shutdown().await;
            vsock_tracing_consumer_handle.await.unwrap();
        }
    }

    Ok(())
}
