mod command;
mod error;

pub use error::{Error, Result};
use tracing::debug;

use std::future::Future;
use std::net::Shutdown;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockListener, VsockStream};

pub use command::{AddPeerArgs, Command, InitializeArgs};

pub async fn send_command(vsock_addr: VsockAddr, command: Command) -> Result<()> {
    debug!("sending command: {:?}", command);

    let mut stream = VsockStream::connect(vsock_addr).await?;
    let encoded = serde_cbor::to_vec(&command)?;
    let length_prefix = (encoded.len() as u32).to_be_bytes();
    stream.write_all(&length_prefix).await?;
    stream.write_all(&encoded).await?;

    let ack = stream.read_u8().await?;
    if ack == 1 {
        debug!("received acknowledgment");
        stream.shutdown(Shutdown::Both)?;
    } else {
        debug!("failed to receive acknowledgment");
    }

    Ok(())
}

pub async fn listen_for_commands<F, Fut>(vsock_addr: VsockAddr, command_handler: F) -> Result<()>
where
    F: Fn(Command) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut listener = VsockListener::bind(vsock_addr)?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        let length = stream.read_u32().await?;
        let mut buffer = vec![0u8; length as usize];
        stream.read_exact(&mut buffer).await?;

        let command: Command = serde_cbor::from_slice(&buffer)?;
        debug!("received command: {:?}", command);

        command_handler(command.clone()).await;
        stream.write_u8(1).await?; // Send acknowledgment

        if let Command::Shutdown = command {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_vsock::VMADDR_CID_ANY;

    #[tokio::test]
    async fn test_send_and_receive_command() {
        let vsock_addr = VsockAddr::new(VMADDR_CID_ANY, 1300);
        let command = Command::Shutdown;
        let received_command = Arc::new(Mutex::new(None));

        let receiver = tokio::spawn({
            let received_command_clone = received_command.clone();
            async move {
                listen_for_commands(vsock_addr, move |command| {
                    let received_command_clone = received_command_clone.clone();
                    async move {
                        let mut received = received_command_clone.lock().await;
                        *received = Some(command);
                    }
                })
                .await
            }
        });

        // Simulate a delay to ensure the receiver is ready
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Send the command
        let _ = send_command(vsock_addr, command.clone()).await;

        // Wait for the receiver to finish
        let _ = receiver.await.unwrap();

        // Lock and clone the received command outside of the async block
        let received = received_command.lock().await.clone();

        assert_eq!(received, Some(command));
    }
}
