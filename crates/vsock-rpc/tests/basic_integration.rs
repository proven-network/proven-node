//! Basic integration tests for vsock-rpc-core.

use proven_vsock_rpc::{Result, RpcMessage};
use serde::{Deserialize, Serialize};

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

impl RpcMessage for TestRequest {
    type Response = TestResponse;

    fn message_id(&self) -> &'static str {
        "test.request"
    }
}

#[test]
fn test_message_implementation() {
    let request = TestRequest {
        id: 42,
        message: "Hello, World!".to_string(),
    };

    assert_eq!(request.message_id(), "test.request");
    assert_eq!(request.response_id(), "test.request.response");
}

#[test]
fn test_codec_roundtrip() -> Result<()> {
    use proven_vsock_rpc::protocol::codec;

    let request = TestRequest {
        id: 123,
        message: "Test message".to_string(),
    };

    // Encode
    let encoded = codec::encode(&request)?;

    // Decode
    let decoded: TestRequest = codec::decode(&encoded)?;

    assert_eq!(request, decoded);
    Ok(())
}

#[test]
fn test_frame_creation() {
    use bytes::Bytes;
    use proven_vsock_rpc::protocol::{Frame, FrameType};

    let payload = Bytes::from("test payload");
    let frame = Frame::new(FrameType::Request, payload.clone());

    assert_eq!(frame.frame_type, FrameType::Request);
    assert_eq!(frame.payload, payload);
    assert!(frame.checksum.is_some());
}

// Note: Full integration tests with actual VSOCK connections would require
// a more complex test setup with mock VSOCK support or running in a VM.
