//! Simple test to debug streaming

use bytes::Bytes;
use proven_vsock_rpc::*;
use std::time::Duration;

#[tokio::test]
async fn test_simple_connection() {
    // Start server
    let port = proven_util::port_allocator::allocate_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    struct SimpleHandler;

    #[async_trait::async_trait]
    impl RpcHandler for SimpleHandler {
        async fn handle_message(
            &self,
            message_id: &str,
            _message: Bytes,
            pattern: MessagePattern,
        ) -> Result<HandlerResponse> {
            eprintln!(
                "Handler called with message_id: {}, pattern: {:?}",
                message_id, pattern
            );
            Ok(HandlerResponse::Single(Bytes::from("hello")))
        }
    }

    let server = RpcServer::new(addr, SimpleHandler, ServerConfig::default());
    eprintln!("Starting server on {addr}");

    let server_handle = tokio::spawn(async move {
        match server.serve().await {
            Ok(_) => eprintln!("Server exited normally"),
            Err(e) => eprintln!("Server error: {e:?}"),
        }
    });

    // Give server time to bind
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Try to connect
    eprintln!("Creating client...");
    match RpcClient::builder().vsock_addr(addr).build() {
        Ok(client) => {
            eprintln!("Client created successfully");
            client.shutdown().unwrap();
        }
        Err(e) => {
            eprintln!("Failed to create client: {e:?}");
        }
    }

    server_handle.abort();
}
