//! Integration test for PubSub streaming functionality

use bytes::Bytes;
use proven_engine::Message;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::info;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tokio::test]
async fn test_pubsub_streaming() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug,proven_network=debug")
        .with_test_writer()
        .try_init();

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

    // Test 5: Test streaming performance (no intermediate broadcast channel)
    info!("Testing streaming performance");
    let perf_stream = client0.subscribe("perf.test", None).await?;

    // Wait for subscription to be ready
    tokio::time::sleep(Duration::from_millis(200)).await;

    let start = std::time::Instant::now();

    // Publish many messages
    let message_count = 100;
    for i in 0..message_count {
        client1
            .publish("perf.test", vec![Message::from(format!("Message {i}"))])
            .await?;
    }

    // Receive all messages using stream
    let messages: Vec<_> = timeout(
        Duration::from_secs(10),
        Box::pin(perf_stream)
            .take(message_count)
            .collect::<Vec<_>>(),
    )
    .await
    .expect("Timeout collecting performance messages");

    let elapsed = start.elapsed();
    info!("Received {} messages in {:?}", messages.len(), elapsed);

    // Allow for some message loss in performance test
    assert!(
        messages.len() >= (message_count * 95 / 100),
        "Should receive at least 95% of messages, got {}%",
        messages.len() * 100 / message_count
    );

    Ok(())
}

#[tokio::test]
async fn test_streaming_subscription_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

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
