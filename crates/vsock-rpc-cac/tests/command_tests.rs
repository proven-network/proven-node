//! Tests for CAC command handling.

use async_trait::async_trait;
use bytes::Bytes;
use cidr::Ipv4Cidr;
use proven_vsock_rpc::{MessagePattern, RpcHandler};
use proven_vsock_rpc_cac::{
    InitializeRequest, InitializeResponse, Result, ShutdownResponse,
    commands::ShutdownRequest,
    server::{CacCommandHandler, CacHandler},
};
use std::net::Ipv4Addr;
use std::str::FromStr;

/// Mock handler for testing.
struct MockHandler {
    should_fail: bool,
}

#[async_trait]
impl CacCommandHandler for MockHandler {
    async fn handle_initialize(&self, _request: InitializeRequest) -> Result<InitializeResponse> {
        if self.should_fail {
            Err(anyhow::anyhow!("Mock initialization failure").into())
        } else {
            Ok(InitializeResponse {
                success: true,
                error: None,
            })
        }
    }

    async fn handle_shutdown(&self, _request: ShutdownRequest) -> Result<ShutdownResponse> {
        if self.should_fail {
            Err(anyhow::anyhow!("Mock shutdown failure").into())
        } else {
            Ok(ShutdownResponse {
                success: true,
                message: Some("Shutting down".to_string()),
            })
        }
    }
}

fn create_test_initialize_request() -> InitializeRequest {
    InitializeRequest {
        certificates_bucket: "test-certs".to_string(),
        cidr: Ipv4Cidr::from_str("10.0.0.0/24").unwrap(),
        email: vec!["test@example.com".to_string()],
        enclave_ip: Ipv4Addr::new(10, 0, 0, 2),
        file_systems_bucket: "test-fs".to_string(),
        host_dns_resolv: "nameserver 8.8.8.8".to_string(),
        host_ip: Ipv4Addr::new(10, 0, 0, 1),
        https_port: 443,
        kms_key_id: "test-key-id".to_string(),
        max_runtime_workers: 4,
        nats_cluster_port: 4222,
        node_key: "test-node-key".to_string(),
        nfs_mount_point: "/mnt/nfs".to_string(),
        proxy_port: 3000,
        skip_fsck: false,
        skip_speedtest: true,
        skip_vacuum: false,
        sql_snapshots_bucket: "test-sql".to_string(),
        radix_mainnet_port: 30000,
        radix_stokenet_port: 30001,
        testnet: true,
    }
}

#[tokio::test]
async fn test_successful_initialize() {
    let handler = CacHandler::new(MockHandler { should_fail: false });

    // Serialize request directly (no more wrapping in enum)
    let request = create_test_initialize_request();
    let message: Bytes = request.try_into().unwrap();

    // Handle message with the appropriate message_id
    let response = handler
        .handle_message(
            "cac.initialize",
            message,
            MessagePattern::OneWay {
                reliability: proven_vsock_rpc::protocol::patterns::ReliabilityLevel::BestEffort,
                wait_for_ack: false,
            },
        )
        .await
        .unwrap();

    // Verify response
    match response {
        proven_vsock_rpc::HandlerResponse::Single(bytes) => {
            let init_resp = InitializeResponse::try_from(bytes).unwrap();
            assert!(init_resp.success);
            assert!(init_resp.error.is_none());
        }
        _ => panic!("Expected single response"),
    }

    // Verify state
    assert!(handler.is_initialized().await);
    assert!(!handler.is_shutting_down().await);
}

#[tokio::test]
async fn test_failed_initialize() {
    let handler = CacHandler::new(MockHandler { should_fail: true });

    // Serialize request directly
    let request = create_test_initialize_request();
    let message: Bytes = request.try_into().unwrap();

    // Handle message
    let response = handler
        .handle_message(
            "cac.initialize",
            message,
            MessagePattern::OneWay {
                reliability: proven_vsock_rpc::protocol::patterns::ReliabilityLevel::BestEffort,
                wait_for_ack: false,
            },
        )
        .await
        .unwrap();

    // Verify response
    match response {
        proven_vsock_rpc::HandlerResponse::Single(bytes) => {
            let init_resp = InitializeResponse::try_from(bytes).unwrap();
            assert!(!init_resp.success);
            assert!(init_resp.error.is_some());
            assert!(
                init_resp
                    .error
                    .unwrap()
                    .contains("Mock initialization failure")
            );
        }
        _ => panic!("Expected single response"),
    }

    // Verify state
    assert!(!handler.is_initialized().await);
}

#[tokio::test]
async fn test_shutdown() {
    let handler = CacHandler::new(MockHandler { should_fail: false });

    // Serialize request directly
    let request = ShutdownRequest {
        grace_period_secs: Some(30),
    };
    let message: Bytes = request.try_into().unwrap();

    // Handle message
    let response = handler
        .handle_message(
            "cac.shutdown",
            message,
            MessagePattern::OneWay {
                reliability: proven_vsock_rpc::protocol::patterns::ReliabilityLevel::BestEffort,
                wait_for_ack: false,
            },
        )
        .await
        .unwrap();

    // Verify response
    match response {
        proven_vsock_rpc::HandlerResponse::Single(bytes) => {
            let shutdown_resp = ShutdownResponse::try_from(bytes).unwrap();
            assert!(shutdown_resp.success);
            assert_eq!(shutdown_resp.message, Some("Shutting down".to_string()));
        }
        _ => panic!("Expected single response"),
    }

    // Verify state
    assert!(handler.is_shutting_down().await);
}

#[tokio::test]
async fn test_unknown_message_id() {
    let handler = CacHandler::new(MockHandler { should_fail: false });

    // Create a message with unknown message_id
    let request = create_test_initialize_request();
    let message: Bytes = request.try_into().unwrap();

    // Handle message with wrong message_id
    let response = handler
        .handle_message(
            "unknown.message",
            message,
            MessagePattern::OneWay {
                reliability: proven_vsock_rpc::protocol::patterns::ReliabilityLevel::BestEffort,
                wait_for_ack: false,
            },
        )
        .await;

    // Should return an error
    assert!(response.is_err());
    match response {
        Err(proven_vsock_rpc::Error::Handler(proven_vsock_rpc::error::HandlerError::NotFound(
            msg,
        ))) => {
            assert!(msg.contains("Unknown message_id"));
        }
        _ => panic!("Expected NotFound error"),
    }
}
