use proven_messaging::stream::Stream;
use proven_messaging_memory::stream::{MemoryStream, MemoryStreamOptions};
use proven_radix_stream::{RadixStream, RadixStreamOptions};

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let radix_stream = RadixStream::new(RadixStreamOptions {
        radix_gateway_origin: "https://mainnet.radixdlt.com",
        transaction_stream: MemoryStream::new("RADIX_TRANSACTIONS", MemoryStreamOptions)
            .await
            .unwrap(),
    })
    .await
    .unwrap();

    match radix_stream.start() {
        Ok(handle) => {
            let _ = handle.await.unwrap(); // This will keep running until the task completes
        }
        Err(e) => {
            eprintln!("Failed to start stream: {:?}", e);
        }
    }
}
