use proven_radix_stream::{RadixStream, RadixStreamOptions};
use proven_stream_memory::MemoryStream;

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .unwrap();

    let radix_stream = RadixStream::new(RadixStreamOptions {
        event_stream: MemoryStream::new(),
        radix_gateway_origin: "https://mainnet.radixdlt.com",
        transaction_stream: MemoryStream::new(),
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
