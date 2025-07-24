//! Integration tests for PubSub functionality

use bytes::Bytes;
use proven_engine::EngineState;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tokio::test]
async fn test_basic_pubsub_publish_subscribe() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug,proven_network=debug")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    let engine = &engines[0];
    let client = engine.client();

    // Wait for engine to be ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe to a subject
    let (sub_id, mut receiver) = client
        .pubsub_subscribe("test.subject", None)
        .await
        .expect("Failed to subscribe");

    // Publish a message
    let payload = Bytes::from("Hello PubSub!");
    let headers = vec![("header1".to_string(), "value1".to_string())];
    client
        .pubsub_publish("test.subject", payload.clone(), headers.clone())
        .await
        .expect("Failed to publish");

    // Receive the message
    let msg = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("Timeout waiting for message")
        .expect("Failed to receive message");

    assert_eq!(msg.payload, payload);
    assert_eq!(msg.headers, headers);
    assert_eq!(msg.subject.as_str(), "test.subject");

    // Unsubscribe
    client
        .pubsub_unsubscribe(&sub_id)
        .await
        .expect("Failed to unsubscribe");
}

#[tokio::test]
async fn test_pubsub_wildcard_subscriptions() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe to wildcard patterns
    let (_sub1, mut receiver1) = client
        .pubsub_subscribe("metrics.*", None)
        .await
        .expect("Failed to subscribe to metrics.*");

    let (_sub2, mut receiver2) = client
        .pubsub_subscribe("logs.>", None)
        .await
        .expect("Failed to subscribe to logs.>");

    // Publish messages that match patterns
    client
        .pubsub_publish("metrics.cpu", Bytes::from("cpu:80"), vec![])
        .await
        .expect("Failed to publish metrics.cpu");

    client
        .pubsub_publish("metrics.memory", Bytes::from("memory:4GB"), vec![])
        .await
        .expect("Failed to publish metrics.memory");

    client
        .pubsub_publish("logs.app.error", Bytes::from("error log"), vec![])
        .await
        .expect("Failed to publish logs.app.error");

    // Non-matching message
    client
        .pubsub_publish("other.topic", Bytes::from("other"), vec![])
        .await
        .expect("Failed to publish other.topic");

    // Check metrics.* receives exactly 2 messages
    let msg1 = timeout(Duration::from_secs(1), receiver1.recv())
        .await
        .expect("Timeout waiting for first metrics message")
        .expect("Failed to receive message");
    assert!(msg1.subject.as_str() == "metrics.cpu" || msg1.subject.as_str() == "metrics.memory");

    let msg2 = timeout(Duration::from_secs(1), receiver1.recv())
        .await
        .expect("Timeout waiting for second metrics message")
        .expect("Failed to receive message");
    assert!(msg2.subject.as_str() == "metrics.cpu" || msg2.subject.as_str() == "metrics.memory");

    // Check logs.> receives the deep nested message
    let msg3 = timeout(Duration::from_secs(1), receiver2.recv())
        .await
        .expect("Timeout waiting for logs message")
        .expect("Failed to receive message");
    assert_eq!(msg3.subject.as_str(), "logs.app.error");

    // Verify no more messages are received
    assert!(
        timeout(Duration::from_millis(200), receiver1.recv())
            .await
            .is_err(),
        "Should not receive any more messages on metrics.*"
    );
    assert!(
        timeout(Duration::from_millis(200), receiver2.recv())
            .await
            .is_err(),
        "Should not receive any more messages on logs.>"
    );
}

#[tokio::test]
async fn test_pubsub_queue_groups() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create three subscribers in the same queue group
    let (_sub1, mut receiver1) = client
        .pubsub_subscribe("work.*", Some("workers".to_string()))
        .await
        .expect("Failed to subscribe 1");

    let (_sub2, mut receiver2) = client
        .pubsub_subscribe("work.*", Some("workers".to_string()))
        .await
        .expect("Failed to subscribe 2");

    let (_sub3, mut receiver3) = client
        .pubsub_subscribe("work.*", Some("workers".to_string()))
        .await
        .expect("Failed to subscribe 3");

    // Also create a regular subscriber (not in queue group)
    let (_sub4, mut receiver4) = client
        .pubsub_subscribe("work.*", None)
        .await
        .expect("Failed to subscribe 4");

    // Publish messages
    for i in 0..3 {
        client
            .pubsub_publish("work.task", Bytes::from(format!("task{i}")), vec![])
            .await
            .expect("Failed to publish");
    }

    // Regular subscriber should get all 3 messages
    let mut regular_count = 0;
    while let Ok(Ok(_)) = timeout(Duration::from_millis(100), receiver4.recv()).await {
        regular_count += 1;
    }
    assert_eq!(
        regular_count, 3,
        "Regular subscriber should get all messages"
    );

    // Queue group subscribers should each get 1 message (load balanced)
    let mut queue_counts = [0, 0, 0];

    if let Ok(Ok(_)) = timeout(Duration::from_millis(100), receiver1.recv()).await {
        queue_counts[0] += 1;
    }
    if let Ok(Ok(_)) = timeout(Duration::from_millis(100), receiver2.recv()).await {
        queue_counts[1] += 1;
    }
    if let Ok(Ok(_)) = timeout(Duration::from_millis(100), receiver3.recv()).await {
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

#[tokio::test]
async fn test_pubsub_stream_api() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe using the stream API
    let (_sub_id, stream) = client
        .pubsub_subscribe_stream("stream.>", None)
        .await
        .expect("Failed to subscribe stream");

    // Publish messages
    for i in 0..5 {
        client
            .pubsub_publish(
                &format!("stream.test.{i}"),
                Bytes::from(format!("message{i}")),
                vec![],
            )
            .await
            .expect("Failed to publish");
    }

    // Collect messages from stream
    let messages: Vec<_> = timeout(
        Duration::from_secs(2),
        Box::pin(stream).take(5).collect::<Vec<_>>(),
    )
    .await
    .expect("Timeout collecting messages");

    assert_eq!(messages.len(), 5);
    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg.subject.as_str(), format!("stream.test.{i}"));
        assert_eq!(msg.payload, Bytes::from(format!("message{i}")));
    }
}

#[tokio::test]
async fn test_pubsub_multi_node() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            "proven_engine=debug,proven_network=error,proven_engine::services::pubsub=trace",
        )
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(3).await;

    // Give cluster time to form and membership events to propagate
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Subscribe on node 0
    let client0 = engines[0].client();
    let (_sub_id, mut receiver) = client0
        .pubsub_subscribe("distributed.*", None)
        .await
        .expect("Failed to subscribe on node 0");

    // Give time for interest propagation after subscription
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from node 1
    let client1 = engines[1].client();
    client1
        .pubsub_publish(
            "distributed.test",
            Bytes::from("cross-node message"),
            vec![("from".to_string(), "node1".to_string())],
        )
        .await
        .expect("Failed to publish from node 1");

    // Receive on node 0
    let msg = timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("Timeout waiting for cross-node message")
        .expect("Failed to receive cross-node message");

    assert_eq!(msg.subject.as_str(), "distributed.test");
    assert_eq!(msg.payload, Bytes::from("cross-node message"));
    assert_eq!(msg.headers.len(), 1);
    assert_eq!(msg.headers[0], ("from".to_string(), "node1".to_string()));
}

#[tokio::test]
async fn test_pubsub_no_subscribers() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish to a subject with no subscribers - should not error
    client
        .pubsub_publish("no.subscribers", Bytes::from("lost message"), vec![])
        .await
        .expect("Publish should succeed even with no subscribers");

    // Verify engine is still healthy
    let health = engines[0].health().await.expect("Failed to get health");
    assert_eq!(health.state, EngineState::Running);
}

#[tokio::test]
async fn test_pubsub_subject_validation() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;
    let client = engines[0].client();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test invalid subjects
    assert!(
        client
            .pubsub_publish("", Bytes::from("test"), vec![])
            .await
            .is_err(),
        "Empty subject should fail"
    );

    assert!(
        client.pubsub_subscribe("", None).await.is_err(),
        "Empty pattern should fail"
    );

    assert!(
        client
            .pubsub_publish("subject with spaces", Bytes::from("test"), vec![])
            .await
            .is_err(),
        "Subject with spaces should fail"
    );

    assert!(
        client
            .pubsub_publish("subject.*.wildcard", Bytes::from("test"), vec![])
            .await
            .is_err(),
        "Subject with wildcard should fail for publish"
    );

    // Test valid patterns for subscribe
    assert!(
        client.pubsub_subscribe("valid.*", None).await.is_ok(),
        "Wildcard pattern should work for subscribe"
    );

    assert!(
        client.pubsub_subscribe("valid.>", None).await.is_ok(),
        "Multi-wildcard pattern should work for subscribe"
    );
}
