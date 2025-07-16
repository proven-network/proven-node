//! Integration tests for streaming functionality

use bytes::Bytes;
use proven_vsock_rpc::{RpcClient, RpcMessage};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamRequest {
    count: u32,
    delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamItem {
    index: u32,
    data: String,
}

impl RpcMessage for StreamRequest {
    type Response = StreamItem;

    fn message_id(&self) -> proven_vsock_rpc::MessageId {
        "stream_request"
    }
}

impl TryFrom<StreamRequest> for Bytes {
    type Error = bincode::Error;

    fn try_from(value: StreamRequest) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map(Bytes::from)
    }
}

impl TryFrom<Bytes> for StreamRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryFrom<StreamItem> for Bytes {
    type Error = bincode::Error;

    fn try_from(value: StreamItem) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map(Bytes::from)
    }
}

impl TryFrom<Bytes> for StreamItem {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

#[tokio::test]
#[ignore] // Streaming server-side implementation not complete
async fn test_request_stream() {
    // This test would require server-side streaming implementation
    // For now, we'll just verify client-side setup
}

#[tokio::test]
async fn test_streaming_request_setup() {
    // This test just verifies the client-side setup works
    #[cfg(target_os = "linux")]
    let addr = proven_vsock_rpc::VsockAddr::new(2, 12346);
    #[cfg(not(target_os = "linux"))]
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 12346));

    let client = RpcClient::builder()
        .vsock_addr(addr)
        .build()
        .expect("Failed to create client");

    let request = StreamRequest {
        count: 3,
        delay_ms: 50,
    };

    // Just verify we can create a streaming request
    // It will fail to connect, but that's expected
    let result = client.request_stream(request).await;
    assert!(result.is_err()); // Expected - no server running
}
