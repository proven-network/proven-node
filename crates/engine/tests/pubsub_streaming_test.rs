//! Integration test for PubSub streaming functionality

use bytes::Bytes;
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
    let (_sub_id, stream) = client0
        .pubsub_subscribe_stream("test.stream.*", None)
        .await?;

    // Give subscription time to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test 2: Publish message from node 1
    info!("Publishing message from node 1");
    client1
        .pubsub_publish(
            "test.stream.foo",
            Bytes::from("Hello from streaming!"),
            vec![],
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
    assert_eq!(messages[0].subject.as_str(), "test.stream.foo");
    assert_eq!(messages[0].payload, Bytes::from("Hello from streaming!"));
    info!("Successfully received message through streaming channel");

    // Test 4: Test queue groups with streaming
    info!("Testing queue groups with streaming");
    let (_qsub1, queue_stream_1) = client0
        .pubsub_subscribe_stream("work.*", Some("workers".to_string()))
        .await?;

    let (_qsub2, queue_stream_2) = client1
        .pubsub_subscribe_stream("work.*", Some("workers".to_string()))
        .await?;

    // Wait for subscriptions to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish multiple messages
    for i in 0..4 {
        client1
            .pubsub_publish("work.task", Bytes::from(format!("Task {i}")), vec![])
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
    let (_perf_sub, perf_stream) = client0.pubsub_subscribe_stream("perf.test", None).await?;

    // Wait for subscription to be ready
    tokio::time::sleep(Duration::from_millis(200)).await;

    let start = std::time::Instant::now();

    // Publish many messages
    let message_count = 100;
    for i in 0..message_count {
        client1
            .pubsub_publish("perf.test", Bytes::from(format!("Message {i}")), vec![])
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
    let (_sub_id, stream) = client.pubsub_subscribe_stream("cleanup.test", None).await?;

    // Publish a message to verify it's working
    client
        .pubsub_publish("cleanup.test", Bytes::from("test"), vec![])
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
        .pubsub_publish("cleanup.test", Bytes::from("after cleanup"), vec![])
        .await?;

    info!("Subscription cleanup test passed");

    Ok(())
}

#[tokio::test]
async fn test_streaming_vs_regular_subscriptions() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create both streaming and regular subscriptions
    let (_stream_sub, stream) = client.pubsub_subscribe_stream("compare.*", None).await?;

    let (_regular_sub, mut regular_rx) = client.pubsub_subscribe("compare.*", None).await?;

    // Publish messages
    for i in 0..5 {
        client
            .pubsub_publish(
                &format!("compare.msg{i}"),
                Bytes::from(format!("Message {i}")),
                vec![],
            )
            .await?;
    }

    // Collect from streaming subscription
    let stream_messages: Vec<_> = timeout(
        Duration::from_secs(2),
        Box::pin(stream).take(5).collect::<Vec<_>>(),
    )
    .await?;

    // Collect from regular subscription
    let mut regular_messages = Vec::new();
    for _ in 0..5 {
        if let Ok(msg) = timeout(Duration::from_millis(100), regular_rx.recv()).await {
            regular_messages.push(msg?);
        }
    }

    // Both should receive the same messages
    assert_eq!(
        stream_messages.len(),
        5,
        "Stream should receive all messages"
    );
    assert_eq!(
        regular_messages.len(),
        5,
        "Regular subscription should receive all messages"
    );

    info!("Both streaming and regular subscriptions working correctly");

    Ok(())
}
