//! Tests for PubSub request-reply functionality

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proven_engine::Message;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tokio::test]
async fn test_request_reply_basic() {
    // Setup cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(2).await;

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get clients
    let client1 = Arc::new(engines[0].client());
    let client2 = Arc::new(engines[1].client());

    // Set up a responder on node 2
    let responder_client = client2.clone();
    tokio::spawn(async move {
        let mut sub = responder_client
            .subscribe("test.service", None)
            .await
            .expect("Failed to subscribe");

        use futures::StreamExt;
        while let Some(msg) = sub.next().await {
            if let Some(reply_to) = msg.get_header("reply_to")
                && let Some(correlation_id) = msg.get_header("correlation_id")
            {
                // Send response
                let response =
                    Message::new("response data").with_header("correlation_id", correlation_id);

                responder_client
                    .publish(reply_to, vec![response])
                    .await
                    .expect("Failed to send response");
            }
        }
    });

    // Give the responder time to subscribe
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send request from node 1
    let request = Message::new("request data");
    let response = client1
        .request("test.service", request, Duration::from_secs(5))
        .await
        .expect("Request should succeed");

    // Verify response
    assert_eq!(response.payload, Bytes::from("response data"));
}

#[tokio::test]
async fn test_request_reply_no_responders() {
    // Setup cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(2).await;

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get client
    let client = engines[0].client();

    // Send request to subject with no subscribers
    let request = Message::new("request data");
    let result = client
        .request("no.subscribers.here", request, Duration::from_secs(1))
        .await;

    // Should fail with no responders
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("No responders") || err_msg.contains("no responders"));
}

#[tokio::test]
async fn test_request_reply_timeout() {
    // Setup cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(2).await;

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get clients
    let client1 = Arc::new(engines[0].client());
    let client2 = Arc::new(engines[1].client());

    // Set up a responder that never responds
    let responder_client = client2.clone();
    tokio::spawn(async move {
        let mut sub = responder_client
            .subscribe("test.slow.service", None)
            .await
            .expect("Failed to subscribe");

        use futures::StreamExt;
        while let Some(_msg) = sub.next().await {
            // Receive but don't respond
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    // Give the responder time to subscribe
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send request with short timeout
    let request = Message::new("request data");
    let result = client1
        .request("test.slow.service", request, Duration::from_secs(1))
        .await;

    // Should timeout
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("timed out") || err_msg.contains("timeout"));
}
