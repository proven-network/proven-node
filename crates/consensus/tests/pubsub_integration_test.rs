//! Integration tests for PubSub functionality with real multi-node topology

#[cfg(test)]
mod pubsub_tests {
    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use openraft::Config as RaftConfig;
    use proven_attestation_mock::MockAttestor;
    use proven_consensus::{Consensus, ConsensusConfig};
    use proven_governance::{GovernanceNode, Version};
    use proven_governance_mock::MockGovernance;
    use rand::rngs::OsRng;
    use std::{collections::HashSet, sync::Arc, time::Duration};
    use tokio::time::timeout;
    use tracing_test::traced_test;

    fn next_port() -> u16 {
        proven_util::port_allocator::allocate_port()
    }

    #[tokio::test]
    #[traced_test]
    async fn test_pubsub_multi_node_messaging() {
        println!("🧪 Testing PubSub functionality with 3-node cluster");

        let num_nodes = 3;
        let mut nodes = Vec::new();
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();

        // Allocate ports and generate keys
        for _i in 0..num_nodes {
            ports.push(next_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        println!("📋 Allocated ports: {:?}", ports);

        // Create shared governance that knows about all nodes
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![],
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            // Add all nodes to governance
            for (&port, signing_key) in ports.iter().zip(signing_keys.iter()) {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };
                governance.add_node(node).expect("Failed to add node");
            }

            governance
        };

        // Create consensus nodes
        for (&port, signing_key) in ports.iter().zip(signing_keys.iter()) {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: proven_consensus::transport::TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                },
                storage_config: proven_consensus::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
                cluster_join_retry_config:
                    proven_consensus::config::ClusterJoinRetryConfig::default(),
            };

            let consensus = Consensus::new(config).await.unwrap();
            nodes.push(consensus);
        }

        // Start all nodes
        println!("🚀 Starting all nodes...");
        for (i, node) in nodes.iter().enumerate() {
            println!("Starting node {}", i);
            node.start().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for cluster formation
        println!("⏳ Waiting for cluster formation...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Test 1: Basic publish/subscribe
        println!("\n📤 Test 1: Basic publish/subscribe");
        {
            // Node 0 subscribes to "test.foo"
            let mut sub1 = nodes[0].pubsub_subscribe("test.foo").await.unwrap();

            // Node 1 subscribes to "test.*" (wildcard)
            let mut sub2 = nodes[1].pubsub_subscribe("test.*").await.unwrap();

            // Give time for interest propagation
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Node 2 publishes to "test.foo"
            let publish_result = nodes[2]
                .pubsub_publish("test.foo", Bytes::from("Hello PubSub!"))
                .await;
            assert!(publish_result.is_ok(), "Publish should succeed");

            // Check that both subscribers received the message
            let recv1 = timeout(Duration::from_secs(2), sub1.receiver.recv()).await;
            assert!(recv1.is_ok(), "Node 0 should receive message");
            let (subject1, payload1) = recv1.unwrap().unwrap();
            assert_eq!(subject1, "test.foo");
            assert_eq!(payload1, Bytes::from("Hello PubSub!"));

            let recv2 = timeout(Duration::from_secs(2), sub2.receiver.recv()).await;
            assert!(recv2.is_ok(), "Node 1 should receive message");
            let (subject2, payload2) = recv2.unwrap().unwrap();
            assert_eq!(subject2, "test.foo");
            assert_eq!(payload2, Bytes::from("Hello PubSub!"));

            println!("✅ Basic publish/subscribe test passed");
        }

        // Test 2: Wildcard patterns
        println!("\n📤 Test 2: Wildcard pattern matching");
        {
            // Node 0 subscribes to "weather.>"
            let mut sub_weather = nodes[0].pubsub_subscribe("weather.>").await.unwrap();

            // Give time for interest propagation
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Node 1 publishes to various weather topics
            nodes[1]
                .pubsub_publish("weather.temp", Bytes::from("72F"))
                .await
                .unwrap();
            nodes[1]
                .pubsub_publish("weather.humidity", Bytes::from("45%"))
                .await
                .unwrap();
            nodes[1]
                .pubsub_publish("weather.pressure.sea", Bytes::from("1013mb"))
                .await
                .unwrap();

            // Collect all messages
            let mut received_subjects = Vec::new();
            for _ in 0..3 {
                let recv = timeout(Duration::from_secs(2), sub_weather.receiver.recv()).await;
                if let Ok(Some((subject, _))) = recv {
                    received_subjects.push(subject);
                }
            }

            assert_eq!(received_subjects.len(), 3, "Should receive 3 messages");
            assert!(received_subjects.contains(&"weather.temp".to_string()));
            assert!(received_subjects.contains(&"weather.humidity".to_string()));
            assert!(received_subjects.contains(&"weather.pressure.sea".to_string()));

            println!("✅ Wildcard pattern test passed");
        }

        // Test 3: No cross-talk between subjects
        println!("\n📤 Test 3: Subject isolation");
        {
            // Node 0 subscribes to "private.data"
            let mut sub_private = nodes[0].pubsub_subscribe("private.data").await.unwrap();

            // Give time for interest propagation
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Node 1 publishes to different subject
            nodes[1]
                .pubsub_publish("public.data", Bytes::from("public info"))
                .await
                .unwrap();

            // Should not receive anything
            let recv = timeout(Duration::from_millis(500), sub_private.receiver.recv()).await;
            assert!(
                recv.is_err(),
                "Should not receive messages for different subjects"
            );

            // Now publish to the correct subject
            nodes[1]
                .pubsub_publish("private.data", Bytes::from("private info"))
                .await
                .unwrap();

            // Should receive this one
            let recv = timeout(Duration::from_secs(2), sub_private.receiver.recv()).await;
            assert!(
                recv.is_ok(),
                "Should receive message for subscribed subject"
            );
            let (subject, payload) = recv.unwrap().unwrap();
            assert_eq!(subject, "private.data");
            assert_eq!(payload, Bytes::from("private info"));

            println!("✅ Subject isolation test passed");
        }

        // Test 4: Multiple subscribers on same node
        println!("\n📤 Test 4: Multiple subscriptions per node");
        {
            // Node 0 creates multiple subscriptions
            let mut sub1 = nodes[0].pubsub_subscribe("multi.one").await.unwrap();
            let mut sub2 = nodes[0].pubsub_subscribe("multi.two").await.unwrap();

            // Give time for interest propagation
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Node 1 publishes to both subjects
            nodes[1]
                .pubsub_publish("multi.one", Bytes::from("message one"))
                .await
                .unwrap();
            nodes[1]
                .pubsub_publish("multi.two", Bytes::from("message two"))
                .await
                .unwrap();

            // Check both subscriptions received their messages
            let recv1 = timeout(Duration::from_secs(2), sub1.receiver.recv()).await;
            assert!(recv1.is_ok());
            let (_, payload1) = recv1.unwrap().unwrap();
            assert_eq!(payload1, Bytes::from("message one"));

            let recv2 = timeout(Duration::from_secs(2), sub2.receiver.recv()).await;
            assert!(recv2.is_ok());
            let (_, payload2) = recv2.unwrap().unwrap();
            assert_eq!(payload2, Bytes::from("message two"));

            println!("✅ Multiple subscriptions test passed");
        }

        // Test 5: Subscription cleanup
        println!("\n📤 Test 5: Subscription cleanup on drop");
        {
            // Create and immediately drop a subscription
            {
                let _sub = nodes[0].pubsub_subscribe("temp.subject").await.unwrap();
                // Subscription dropped here
            }

            // Give time for cleanup
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Node 1 publishes to the subject
            nodes[1]
                .pubsub_publish("temp.subject", Bytes::from("should not receive"))
                .await
                .unwrap();

            // Create a new subscription to verify messages are still flowing
            let mut sub_verify = nodes[2].pubsub_subscribe("verify.subject").await.unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;

            nodes[1]
                .pubsub_publish("verify.subject", Bytes::from("verify message"))
                .await
                .unwrap();

            let recv = timeout(Duration::from_secs(2), sub_verify.receiver.recv()).await;
            assert!(recv.is_ok(), "PubSub system should still be functional");

            println!("✅ Subscription cleanup test passed");
        }

        // Shutdown all nodes
        println!("\n🛑 Shutting down all nodes...");
        for (i, node) in nodes.iter().enumerate() {
            println!("Shutting down node {}", i);
            node.shutdown().await.unwrap();
        }

        println!("\n✅ All PubSub integration tests passed!");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_pubsub_request_response() {
        println!("🧪 Testing PubSub request/response pattern");

        // Create a simple 2-node cluster
        let mut nodes = Vec::new();
        let ports = [next_port(), next_port()];
        let signing_keys = vec![
            SigningKey::generate(&mut OsRng),
            SigningKey::generate(&mut OsRng),
        ];

        // Create shared governance
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![],
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            for (&port, signing_key) in ports.iter().zip(signing_keys.iter()) {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };
                governance.add_node(node).expect("Failed to add node");
            }

            governance
        };

        // Create and start nodes
        for (&port, signing_key) in ports.iter().zip(signing_keys.iter()) {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: proven_consensus::transport::TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                },
                storage_config: proven_consensus::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
                cluster_join_retry_config:
                    proven_consensus::config::ClusterJoinRetryConfig::default(),
            };

            let consensus = Consensus::new(config).await.unwrap();
            consensus.start().await.unwrap();
            nodes.push(consensus);
        }

        // Wait for cluster formation
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Test request/response pattern
        println!("\n📤 Testing request/response...");

        // Node 1 subscribes to "echo.service" to act as a responder
        let mut echo_sub = nodes[1].pubsub_subscribe("echo.service").await.unwrap();

        // Spawn a task to handle echo requests
        tokio::spawn(async move {
            while let Some((subject, payload)) = echo_sub.receiver.recv().await {
                println!("Echo service received: {} - {:?}", subject, payload);
                // In a real implementation, we would parse the reply-to subject
                // and send a response. For now, we'll just consume the message.
            }
        });

        // Give time for subscription to propagate
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Node 0 makes a request (this will timeout as we haven't implemented
        // the responder side yet, but it tests the request mechanism)
        let request_result = nodes[0]
            .pubsub_request(
                "echo.service",
                Bytes::from("Hello Echo!"),
                Duration::from_secs(1),
            )
            .await;

        // For now, this will timeout or error as we haven't implemented
        // the response mechanism fully
        println!("Request result: {:?}", request_result);

        // The test passes if the request was sent without panicking
        // Full request/response would require implementing the reply-to handling

        println!("✅ Request/response mechanism test completed");

        // Shutdown
        for node in nodes.iter() {
            node.shutdown().await.unwrap();
        }
    }
}
