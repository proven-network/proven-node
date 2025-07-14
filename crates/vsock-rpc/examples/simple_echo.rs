//! Simple echo server/client example.
//!
//! This example demonstrates basic RPC usage with a simple echo service.
//! Note: This is a conceptual example as VSOCK requires specific setup.

use async_trait::async_trait;
use bytes::Bytes;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, Result, RpcHandler, RpcMessage, RpcServer, ServerConfig,
    VsockAddr,
};
use serde::{Deserialize, Serialize};

/// Echo request message.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoRequest {
    message: String,
}

/// Echo response message.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoResponse {
    message: String,
    timestamp: u64,
}

impl RpcMessage for EchoRequest {
    type Response = EchoResponse;

    fn message_id(&self) -> &'static str {
        "echo.request"
    }
}

/// Echo service handler.
struct EchoHandler;

#[async_trait]
impl RpcHandler for EchoHandler {
    async fn handle_message(
        &self,
        message: Bytes,
        _pattern: MessagePattern,
    ) -> Result<HandlerResponse> {
        // Decode the request
        let request: EchoRequest = proven_vsock_rpc::protocol::codec::decode(&message)?;

        println!("Received echo request: {}", request.message);

        // Create response
        let response = EchoResponse {
            message: request.message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Encode response
        let response_bytes = proven_vsock_rpc::protocol::codec::encode(&response)?;

        Ok(HandlerResponse::Single(response_bytes))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Note: This example shows the API usage but won't actually run
    // without a proper VSOCK setup (e.g., in a VM or container).

    println!("VSOCK RPC Echo Example");
    println!("This is a conceptual example showing API usage.");
    println!();

    // Server example
    println!("Server would listen on CID 2, port 5000:");
    let server_addr = VsockAddr::new(2, 5000);
    let handler = EchoHandler;
    let config = ServerConfig::default();
    let _server = RpcServer::new(server_addr, handler, config);
    println!("  Server created at {server_addr:?}");

    // Client example
    println!("\nClient would connect and send request:");
    println!("  let client = RpcClient::builder()");
    println!("      .vsock_addr(server_addr)");
    println!("      .build()");
    println!("      .await?;");
    println!();
    println!("  let request = EchoRequest {{");
    println!("      message: \"Hello, VSOCK!\".to_string(),");
    println!("  }};");
    println!();
    println!("  let response = client.request(request).await?;");
    println!("  println!(\"Response: {{:?}}\", response);");

    Ok(())
}
