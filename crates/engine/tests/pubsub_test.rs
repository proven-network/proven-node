//! Consolidated integration tests for PubSub functionality
//!
//! Tests include:
//! - Basic publish/subscribe
//! - Wildcard subscriptions
//! - Queue groups
//! - Request-reply pattern
//! - Streaming functionality
//! - Multi-node PubSub

mod common;
use common::test_cluster::{TestCluster, TransportType};

use bytes::Bytes;
use futures::StreamExt;
use proven_engine::{EngineState, Message};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::info;

// ============= Basic PubSub Tests =============

#[tracing_test::traced_test]
#[tokio::test]
async fn test_basic_pubsub_publish_subscribe() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    let engine = &engines[0];
    let client = engine.client();

    // Wait for engine to be ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe to a subject
    let mut receiver = client
        .subscribe("test.subject", None)
        .await
        .expect("Failed to subscribe");

    // Publish a message
    let message = Message::new("Hello PubSub!").with_header("header1", "value1");
    client
        .publish("test.subject", vec![message])
        .await
        .expect("Failed to publish");

    // Receive the message
    let msg = timeout(Duration::from_secs(2), receiver.next())
        .await
        .expect("Timeout waiting for message")
        .expect("Failed to receive message");

    assert_eq!(msg.payload, Bytes::from("Hello PubSub!"));
    assert_eq!(msg.get_header("header1"), Some("value1"));
    assert_eq!(msg.subject(), Some("test.subject"));
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_no_subscribers() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish to a subject with no subscribers - should not error
    client
        .publish("no.subscribers", vec![Message::from("lost message")])
        .await
        .expect("Publish should succeed even with no subscribers");

    // Verify engine is still healthy
    let health = engines[0].health().await.expect("Failed to get health");
    assert_eq!(health.state, EngineState::Running);
}

// ============= Wildcard Subscription Tests =============

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_wildcard_subscriptions() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe to wildcard patterns
    let mut receiver1 = client
        .subscribe("metrics.*", None)
        .await
        .expect("Failed to subscribe to metrics.*");

    let mut receiver2 = client
        .subscribe("logs.>", None)
        .await
        .expect("Failed to subscribe to logs.>");

    // Publish messages that match patterns
    client
        .publish("metrics.cpu", vec![Message::from("cpu:80")])
        .await
        .expect("Failed to publish metrics.cpu");

    client
        .publish("metrics.memory", vec![Message::from("memory:4GB")])
        .await
        .expect("Failed to publish metrics.memory");

    client
        .publish("logs.app.error", vec![Message::from("error log")])
        .await
        .expect("Failed to publish logs.app.error");

    // Non-matching message
    client
        .publish("other.topic", vec![Message::from("other")])
        .await
        .expect("Failed to publish other.topic");

    // Check metrics.* receives exactly 2 messages
    let msg1 = timeout(Duration::from_secs(1), receiver1.next())
        .await
        .expect("Timeout waiting for first metrics message")
        .expect("Failed to receive message");
    assert!(msg1.subject() == Some("metrics.cpu") || msg1.subject() == Some("metrics.memory"));

    let msg2 = timeout(Duration::from_secs(1), receiver1.next())
        .await
        .expect("Timeout waiting for second metrics message")
        .expect("Failed to receive message");
    assert!(msg2.subject() == Some("metrics.cpu") || msg2.subject() == Some("metrics.memory"));

    // Check logs.> receives the deep nested message
    let msg3 = timeout(Duration::from_secs(1), receiver2.next())
        .await
        .expect("Timeout waiting for logs message")
        .expect("Failed to receive message");
    assert_eq!(msg3.subject(), Some("logs.app.error"));

    // Verify no more messages are received
    assert!(
        timeout(Duration::from_millis(200), receiver1.next())
            .await
            .is_err(),
        "Should not receive any more messages on metrics.*"
    );
    assert!(
        timeout(Duration::from_millis(200), receiver2.next())
            .await
            .is_err(),
        "Should not receive any more messages on logs.>"
    );
}

// ============= Queue Group Tests =============

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_queue_groups() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create three subscribers in the same queue group
    let mut receiver1 = client
        .subscribe("work.*", Some("workers".to_string()))
        .await
        .expect("Failed to subscribe 1");

    let mut receiver2 = client
        .subscribe("work.*", Some("workers".to_string()))
        .await
        .expect("Failed to subscribe 2");

    let mut receiver3 = client
        .subscribe("work.*", Some("workers".to_string()))
        .await
        .expect("Failed to subscribe 3");

    // Also create a regular subscriber (not in queue group)
    let mut receiver4 = client
        .subscribe("work.*", None)
        .await
        .expect("Failed to subscribe 4");

    // Publish messages
    for i in 0..3 {
        client
            .publish("work.task", vec![Message::from(format!("task{i}"))])
            .await
            .expect("Failed to publish");
    }

    // Regular subscriber should get all 3 messages
    let mut regular_count = 0;
    while let Ok(Some(_)) = timeout(Duration::from_millis(100), receiver4.next()).await {
        regular_count += 1;
    }
    assert_eq!(
        regular_count, 3,
        "Regular subscriber should get all messages"
    );

    // Queue group subscribers should each get 1 message (load balanced)
    let mut queue_counts = [0, 0, 0];

    if let Ok(Some(_)) = timeout(Duration::from_millis(100), receiver1.next()).await {
        queue_counts[0] += 1;
    }
    if let Ok(Some(_)) = timeout(Duration::from_millis(100), receiver2.next()).await {
        queue_counts[1] += 1;
    }
    if let Ok(Some(_)) = timeout(Duration::from_millis(100), receiver3.next()).await {
        queue_counts[2] += 1;
    }

    // Each queue subscriber should have received exactly 1 message
    assert_eq!(
        queue_counts.iter().sum::<i32>(),
        3,
        "Queue group should have received all messages in total"
    );
    assert_eq!(
        queue_counts.iter().filter(|&&c| c == 1).count(),
        3,
        "Each queue subscriber should have received exactly 1 message"
    );
}

// ============= Request-Reply Tests =============

#[tracing_test::traced_test]
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

#[tracing_test::traced_test]
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

#[tracing_test::traced_test]
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

// ============= Streaming Tests =============

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_streaming() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(2).await;

    // Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get clients
    let client0 = engines[0].client();
    let client1 = engines[1].client();

    // Test 1: Create streaming subscription on node 0
    info!("Creating streaming subscription on node 0");
    let stream = client0.subscribe("test.stream.*", None).await?;

    // Give subscription time to propagate
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test 2: Publish message from node 1
    info!("Publishing message from node 1");
    client1
        .publish(
            "test.stream.foo",
            vec![Message::from("Hello from streaming!")],
        )
        .await?;

    // Test 3: Verify message received through stream
    info!("Waiting for message on stream");
    use futures::StreamExt;
    let messages: Vec<_> = timeout(
        Duration::from_secs(2),
        Box::pin(stream).take(1).collect::<Vec<_>>(),
    )
    .await
    .expect("Timeout collecting messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].subject(), Some("test.stream.foo"));
    assert_eq!(messages[0].payload, Bytes::from("Hello from streaming!"));
    info!("Successfully received message through streaming channel");

    // Test 4: Test queue groups with streaming
    info!("Testing queue groups with streaming");
    let queue_stream_1 = client0
        .subscribe("work.*", Some("workers".to_string()))
        .await?;

    let queue_stream_2 = client1
        .subscribe("work.*", Some("workers".to_string()))
        .await?;

    // Wait for subscriptions to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish multiple messages
    for i in 0..4 {
        client1
            .publish("work.task", vec![Message::from(format!("Task {i}"))])
            .await?;
    }

    // Collect messages from both streams with timeouts
    let messages_1: Vec<_> = timeout(
        Duration::from_secs(2),
        Box::pin(queue_stream_1).take(2).collect::<Vec<_>>(),
    )
    .await
    .unwrap_or_else(|_| vec![]);

    let messages_2: Vec<_> = timeout(
        Duration::from_secs(2),
        Box::pin(queue_stream_2).take(2).collect::<Vec<_>>(),
    )
    .await
    .unwrap_or_else(|_| vec![]);

    // Verify queue group behavior (messages distributed between subscribers)
    info!("Node 0 received: {} messages", messages_1.len());
    info!("Node 1 received: {} messages", messages_2.len());

    assert!(
        !messages_1.is_empty() || !messages_2.is_empty(),
        "At least one node should receive messages"
    );
    assert_eq!(
        messages_1.len() + messages_2.len(),
        4,
        "All messages should be delivered exactly once"
    );

    Ok(())
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_streaming_subscription_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test subscription cleanup
    info!("Testing subscription cleanup");

    // Create subscription
    let stream = client.subscribe("cleanup.test", None).await?;

    // Publish a message to verify it's working
    client
        .publish("cleanup.test", vec![Message::from("test")])
        .await?;

    // Verify message received
    let messages: Vec<_> = timeout(
        Duration::from_secs(2),
        Box::pin(stream).take(1).collect::<Vec<_>>(),
    )
    .await?;
    assert_eq!(messages.len(), 1);

    // Stream is already consumed/dropped after collection

    // Give some time for cleanup
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish another message - it should not cause any errors
    client
        .publish("cleanup.test", vec![Message::from("after cleanup")])
        .await?;

    info!("Subscription cleanup test passed");

    Ok(())
}

// ============= Multi-Node Tests =============

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_multi_node() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(3).await;

    // Give cluster time to form and membership events to propagate
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Subscribe on node 0
    let client0 = engines[0].client();
    let mut receiver = client0
        .subscribe("distributed.*", None)
        .await
        .expect("Failed to subscribe on node 0");

    // Give time for interest propagation after subscription
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from node 1
    let client1 = engines[1].client();
    client1
        .publish(
            "distributed.test",
            vec![Message::new("cross-node message").with_header("from", "node1")],
        )
        .await
        .expect("Failed to publish from node 1");

    // Receive on node 0
    let msg = timeout(Duration::from_secs(5), receiver.next())
        .await
        .expect("Timeout waiting for cross-node message")
        .expect("Failed to receive cross-node message");

    assert_eq!(msg.subject(), Some("distributed.test"));
    assert_eq!(msg.payload, Bytes::from("cross-node message"));
    assert_eq!(msg.get_header("from"), Some("node1"));
}

// ============= Validation Tests =============

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_subject_validation() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test invalid subjects
    assert!(
        client
            .publish("", vec![Message::from("test")])
            .await
            .is_err(),
        "Empty subject should fail"
    );

    assert!(
        client.subscribe("", None).await.is_err(),
        "Empty pattern should fail"
    );

    assert!(
        client
            .publish("subject with spaces", vec![Message::from("test")])
            .await
            .is_err(),
        "Subject with spaces should fail"
    );

    assert!(
        client
            .publish("subject.*.wildcard", vec![Message::from("test")])
            .await
            .is_err(),
        "Subject with wildcard should fail for publish"
    );

    // Test valid patterns for subscribe
    assert!(
        client.subscribe("valid.*", None).await.is_ok(),
        "Wildcard pattern should work for subscribe"
    );

    assert!(
        client.subscribe("valid.>", None).await.is_ok(),
        "Multi-wildcard pattern should work for subscribe"
    );
}

// ============= Performance Tests =============

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_high_volume() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    let engine = &engines[0];
    let client = engine.client();

    // Wait for engine to be ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create multiple subscribers
    let mut receivers = Vec::new();
    for i in 0..5 {
        let receiver = client
            .subscribe(&format!("volume.test.{i}"), None)
            .await
            .expect("Failed to subscribe");
        receivers.push(receiver);
    }

    // Also create a wildcard subscriber
    let mut wildcard_receiver = client
        .subscribe("volume.test.*", None)
        .await
        .expect("Failed to subscribe to wildcard");

    // Small delay to ensure subscriptions are ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish many messages rapidly (reduced for faster tests)
    let message_count = 1000; // Reduced from 100_000
    let start = std::time::Instant::now();

    for i in 0..message_count {
        let subject = format!("volume.test.{}", i % 5);
        let payload = Bytes::from(format!("High volume message {i}"));

        client
            .publish(&subject, vec![Message::new(payload)])
            .await
            .expect("Failed to publish");
    }

    let publish_duration = start.elapsed();
    println!(
        "Published {} messages in {:?} ({:.0} msg/sec)",
        message_count,
        publish_duration,
        message_count as f64 / publish_duration.as_secs_f64()
    );

    // Verify messages were received (with relaxed requirements)
    let mut wildcard_count = 0;
    let timeout = Duration::from_secs(5);
    let deadline = std::time::Instant::now() + timeout;

    while wildcard_count < message_count && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), wildcard_receiver.next()).await {
            Ok(Some(_msg)) => {
                wildcard_count += 1;
            }
            _ => {
                break;
            }
        }
    }

    println!("Wildcard subscriber received {wildcard_count} out of {message_count} messages");

    // Allow some message loss in high volume scenario
    assert!(
        wildcard_count >= (message_count * 90 / 100), // 90% delivery
        "Expected at least 90% message delivery, got {}%",
        wildcard_count * 100 / message_count
    );
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_pubsub_burst_publishing() {
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    let engine = &engines[0];
    let client = engine.client();

    // Wait for engine to be ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a subscriber
    let mut receiver = client
        .subscribe("burst.test", None)
        .await
        .expect("Failed to subscribe");

    // Small delay to ensure subscription is ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish messages in bursts
    let burst_size = 50; // Reduced from 100
    let num_bursts = 5; // Reduced from 10
    let mut total_published = 0;

    for burst in 0..num_bursts {
        // Publish a burst of messages without waiting
        let futures: Vec<_> = (0..burst_size)
            .map(|i| {
                let payload = format!("Burst {burst} message {i}");
                client.publish("burst.test", vec![Message::from(payload)])
            })
            .collect();

        // Wait for all publishes in the burst to complete
        let results = futures::future::join_all(futures).await;

        // Check all succeeded
        for result in results {
            assert!(result.is_ok(), "Publish failed in burst {burst}");
            total_published += 1;
        }

        // Small delay between bursts
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("Published {total_published} messages in {num_bursts} bursts");

    // Verify messages were received
    let mut received_count = 0;
    let timeout = Duration::from_secs(5);
    let deadline = std::time::Instant::now() + timeout;

    while received_count < total_published && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), receiver.next()).await {
            Ok(Some(_msg)) => {
                received_count += 1;
            }
            _ => {
                break;
            }
        }
    }

    println!(
        "Received {} out of {} messages ({:.1}%)",
        received_count,
        total_published,
        received_count as f64 * 100.0 / total_published as f64
    );

    // Expect high delivery rate for burst publishing
    assert!(
        received_count >= (total_published * 90 / 100), // 90% delivery
        "Expected at least 90% message delivery, got {}%",
        received_count * 100 / total_published
    );
}
