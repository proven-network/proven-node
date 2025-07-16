//! Integration test for vsock-rpc

use async_trait::async_trait;
use bytes::Bytes;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, Result, RpcClient, RpcHandler, RpcMessage, RpcServer,
    ServerConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestRequest {
    id: u32,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestResponse {
    id: u32,
    reply: String,
}

impl TryFrom<Bytes> for TestRequest {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_from(bytes: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&bytes)
            .map_err(|e| proven_vsock_rpc::error::CodecError::DeserializationFailed(e.to_string()))
    }
}

impl TryInto<Bytes> for TestRequest {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        bincode::serialize(&self)
            .map(Bytes::from)
            .map_err(|e| proven_vsock_rpc::error::CodecError::SerializationFailed(e.to_string()))
    }
}

impl TryFrom<Bytes> for TestResponse {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_from(bytes: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&bytes)
            .map_err(|e| proven_vsock_rpc::error::CodecError::DeserializationFailed(e.to_string()))
    }
}

impl TryInto<Bytes> for TestResponse {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        bincode::serialize(&self)
            .map(Bytes::from)
            .map_err(|e| proven_vsock_rpc::error::CodecError::SerializationFailed(e.to_string()))
    }
}

impl RpcMessage for TestRequest {
    type Response = TestResponse;

    fn message_id(&self) -> &'static str {
        "test.request"
    }
}

struct TestHandler;

#[async_trait]
impl RpcHandler for TestHandler {
    async fn handle_message(
        &self,
        _message_id: &str,
        message: Bytes,
        _pattern: MessagePattern,
    ) -> Result<HandlerResponse> {
        let request = TestRequest::try_from(message)?;

        let response = TestResponse {
            id: request.id,
            reply: format!("Echo: {}", request.message),
        };

        let response_bytes: Bytes = response.try_into()?;
        Ok(HandlerResponse::Single(response_bytes))
    }
}

#[tokio::test]
async fn test_client_server_communication() -> Result<()> {
    // Start server
    let port = proven_util::port_allocator::allocate_port();

    #[cfg(target_os = "linux")]
    let server_addr = tokio_vsock::VsockAddr::new(3, port);
    #[cfg(not(target_os = "linux"))]
    let server_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    let handler = TestHandler;
    let config = ServerConfig::default();
    let server = RpcServer::new(server_addr, handler, config);

    // Run server in background
    let server_handle = tokio::spawn(async move { server.serve().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create client
    let client = RpcClient::builder()
        .vsock_addr(
            #[cfg(target_os = "linux")]
            tokio_vsock::VsockAddr::new(3, port),
            #[cfg(not(target_os = "linux"))]
            std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        )
        .pool_size(1)
        .default_timeout(Duration::from_secs(5))
        .build()?;

    // Send request
    let request = TestRequest {
        id: 42,
        message: "Hello, RPC!".to_string(),
    };

    let response = client.request(request.clone()).await?;
    assert_eq!(response.id, 42);
    assert_eq!(response.reply, "Echo: Hello, RPC!");

    // Cleanup
    client.shutdown()?;
    server_handle.abort();

    Ok(())
}
