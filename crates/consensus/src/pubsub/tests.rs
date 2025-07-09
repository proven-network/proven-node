//! Tests for PubSub functionality

#[cfg(test)]
mod pubsub_tests {
    use super::super::*;
    use bytes::Bytes;
    use std::sync::Arc;

    use crate::NodeId;
    use crate::pubsub::interest::InterestTracker;
    use crate::pubsub::router::MessageRouter;

    #[tokio::test]
    async fn test_basic_pubsub_operations() {
        // This is a unit test for the interest tracker and router
        // Full integration tests would require setting up actual network connections

        let node_id = NodeId::from_seed(1);
        let interest_tracker = Arc::new(InterestTracker::new());
        let router = MessageRouter::new(node_id.clone(), interest_tracker.clone());

        // Test adding interests
        let remote_node = NodeId::from_seed(2);
        interest_tracker
            .add_remote_interest(remote_node.clone(), "test.*")
            .unwrap();

        // Test routing
        let message = PubSubMessage::Publish {
            subject: "test.foo".to_string(),
            payload: Bytes::from("hello"),
            reply_to: None,
            message_id: uuid::Uuid::new_v4(),
        };

        let routes = router.route_message(&message).unwrap();
        assert_eq!(routes.len(), 1);
        assert!(routes.contains(&remote_node));
    }

    #[tokio::test]
    async fn test_subscription_lifecycle() {
        let interest_tracker = InterestTracker::new();

        // Add local subscription
        let sub_id = "test-sub-1";
        interest_tracker
            .add_local_subscription(sub_id.to_string(), "foo.*".to_string())
            .unwrap();

        // Verify it exists
        let patterns = interest_tracker.get_local_patterns();
        assert!(patterns.contains("foo.*"));

        // Remove subscription
        let removed = interest_tracker.remove_local_subscription(sub_id);
        assert_eq!(removed, Some("foo.*".to_string()));

        // Verify it's gone
        let patterns = interest_tracker.get_local_patterns();
        assert!(!patterns.contains("foo.*"));
    }

    #[tokio::test]
    async fn test_interest_updates() {
        let interest_tracker = InterestTracker::new();
        let node = NodeId::from_seed(3);

        // Set initial interests
        let mut interests = std::collections::HashSet::new();
        interests.insert("foo.*".to_string());
        interests.insert("bar.>".to_string());

        interest_tracker
            .update_node_interests(node.clone(), interests.clone())
            .unwrap();

        // Verify interests
        let node_interests = interest_tracker.get_node_interests(&node);
        assert_eq!(node_interests.len(), 2);
        assert!(node_interests.contains("foo.*"));
        assert!(node_interests.contains("bar.>"));

        // Update to new interests
        let mut new_interests = std::collections::HashSet::new();
        new_interests.insert("baz.*".to_string());

        interest_tracker
            .update_node_interests(node.clone(), new_interests)
            .unwrap();

        // Verify old interests are gone
        let node_interests = interest_tracker.get_node_interests(&node);
        assert_eq!(node_interests.len(), 1);
        assert!(!node_interests.contains("foo.*"));
        assert!(node_interests.contains("baz.*"));
    }

    #[tokio::test]
    async fn test_pattern_matching() {
        let interest_tracker = InterestTracker::new();
        let node1 = NodeId::from_seed(4);
        let node2 = NodeId::from_seed(5);

        // Add different patterns
        interest_tracker
            .add_remote_interest(node1.clone(), "foo.*")
            .unwrap();
        interest_tracker
            .add_remote_interest(node2.clone(), "foo.bar")
            .unwrap();
        interest_tracker
            .add_remote_interest(node2.clone(), "*.baz")
            .unwrap();

        // Test exact match
        let interested = interest_tracker.find_interested_nodes("foo.bar");
        assert_eq!(interested.len(), 2);
        assert!(interested.contains(&node1));
        assert!(interested.contains(&node2));

        // Test wildcard match
        let interested = interest_tracker.find_interested_nodes("foo.test");
        assert_eq!(interested.len(), 1);
        assert!(interested.contains(&node1));

        // Test different wildcard match
        let interested = interest_tracker.find_interested_nodes("test.baz");
        assert_eq!(interested.len(), 1);
        assert!(interested.contains(&node2));

        // Test no match
        let interested = interest_tracker.find_interested_nodes("bar.test");
        assert_eq!(interested.len(), 0);
    }

    #[tokio::test]
    async fn test_deduplication() {
        let node_id = NodeId::from_seed(6);
        let interest_tracker = Arc::new(InterestTracker::new());
        let router = MessageRouter::new(node_id, interest_tracker);

        let message_id = uuid::Uuid::new_v4();
        let message = PubSubMessage::Publish {
            subject: "test".to_string(),
            payload: Bytes::from("data"),
            reply_to: None,
            message_id,
        };

        // First attempt should succeed
        let routes1 = router.route_message(&message).unwrap();
        assert_eq!(routes1.len(), 0); // No remote nodes interested

        // Second attempt should be deduplicated
        let routes2 = router.route_message(&message).unwrap();
        assert_eq!(routes2.len(), 0);

        // Verify deduplication tracking
        assert!(router.seen_message_count() > 0);
    }

    #[test]
    fn test_interest_update_acknowledgment_message() {
        // Test that InterestUpdateAck message type works correctly
        let node_id = NodeId::from_seed(7);

        let ack_msg = PubSubMessage::InterestUpdateAck {
            node_id: node_id.clone(),
            success: true,
            error: None,
        };

        // Verify it doesn't require a response
        assert!(!ack_msg.requires_response());

        // Test with error
        let error_ack = PubSubMessage::InterestUpdateAck {
            node_id,
            success: false,
            error: Some("Invalid pattern".to_string()),
        };

        match error_ack {
            PubSubMessage::InterestUpdateAck { success, error, .. } => {
                assert!(!success);
                assert_eq!(error, Some("Invalid pattern".to_string()));
            }
            _ => panic!("Expected InterestUpdateAck"),
        }
    }

    #[test]
    fn test_interest_update_requires_response() {
        let node_id = NodeId::from_seed(8);
        let mut interests = std::collections::HashSet::new();
        interests.insert("test.*".to_string());

        let update_msg = PubSubMessage::InterestUpdate { interests, node_id };

        // InterestUpdate should require a response
        assert!(update_msg.requires_response());
    }

    #[test]
    fn test_message_serialization() {
        // Test that all PubSub messages can be serialized/deserialized
        let node_id = NodeId::from_seed(9);

        // Test InterestUpdate serialization
        let mut interests = std::collections::HashSet::new();
        interests.insert("test.*".to_string());
        interests.insert("foo.bar".to_string());

        let update_msg = PubSubMessage::InterestUpdate {
            interests: interests.clone(),
            node_id: node_id.clone(),
        };

        let serialized = serde_json::to_string(&update_msg).unwrap();
        let deserialized: PubSubMessage = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            PubSubMessage::InterestUpdate {
                interests: deser_interests,
                node_id: deser_node_id,
            } => {
                assert_eq!(deser_interests, interests);
                assert_eq!(deser_node_id, node_id);
            }
            _ => panic!("Expected InterestUpdate"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_publish_delivery() {
        // Test that published messages are delivered to local subscribers
        let node_id = NodeId::from_seed(10);
        let interest_tracker = Arc::new(InterestTracker::new());
        let router = Arc::new(MessageRouter::new(
            node_id.clone(),
            interest_tracker.clone(),
        ));

        // Add a local subscription
        let sub_id = "test-sub-1";
        interest_tracker
            .add_local_subscription(sub_id.to_string(), "test.*".to_string())
            .unwrap();

        // Publish a message that matches
        let _message = PubSubMessage::Publish {
            subject: "test.foo".to_string(),
            payload: Bytes::from("test payload"),
            reply_to: None,
            message_id: uuid::Uuid::new_v4(),
        };

        // Check that router identifies it should be delivered locally
        assert!(router.should_deliver_locally("test.foo"));

        // Test with non-matching subject
        assert!(!router.should_deliver_locally("other.subject"));
    }

    #[tokio::test]
    async fn test_interest_propagation() {
        // Test that interests are properly propagated through the system
        let _node_id = NodeId::from_seed(11);
        let interest_tracker = Arc::new(InterestTracker::new());

        // Add multiple nodes with different interests
        let node2 = NodeId::from_seed(12);
        let node3 = NodeId::from_seed(13);

        interest_tracker
            .add_remote_interest(node2.clone(), "weather.*")
            .unwrap();
        interest_tracker
            .add_remote_interest(node2.clone(), "news.>")
            .unwrap();
        interest_tracker
            .add_remote_interest(node3.clone(), "weather.temperature")
            .unwrap();

        // Find nodes interested in weather data
        let interested = interest_tracker.find_interested_nodes("weather.temperature");
        assert_eq!(interested.len(), 2);
        assert!(interested.contains(&node2));
        assert!(interested.contains(&node3));

        // Find nodes interested in news
        let interested = interest_tracker.find_interested_nodes("news.sports.football");
        assert_eq!(interested.len(), 1);
        assert!(interested.contains(&node2));

        // Update node2's interests (remove weather, keep news)
        let mut new_interests = std::collections::HashSet::new();
        new_interests.insert("news.>".to_string());
        interest_tracker
            .update_node_interests(node2.clone(), new_interests)
            .unwrap();

        // Verify weather interest was removed
        let interested = interest_tracker.find_interested_nodes("weather.temperature");
        assert_eq!(interested.len(), 1);
        assert!(interested.contains(&node3));
        assert!(!interested.contains(&node2));
    }

    #[tokio::test]
    async fn test_request_response_pattern() {
        // Test request-response message types
        let request_id = uuid::Uuid::new_v4();
        let reply_to = "_INBOX.test";

        let request = PubSubMessage::Request {
            subject: "service.echo".to_string(),
            payload: Bytes::from("echo this"),
            reply_to: reply_to.to_string(),
            request_id,
        };

        // Verify request requires response
        assert!(request.requires_response());

        // Create response
        let response = PubSubMessage::Response {
            request_id,
            payload: Bytes::from("echo this"),
            responder: NodeId::from_seed(14),
        };

        // Verify response doesn't require response
        assert!(!response.requires_response());

        // Test subject extraction
        assert_eq!(request.subject(), Some("service.echo"));
        assert_eq!(response.subject(), None);
    }

    #[test]
    fn test_subscription_lifecycle_complete() {
        // Comprehensive test of subscription lifecycle
        let interest_tracker = InterestTracker::new();

        // Add multiple local subscriptions
        let subs = vec![
            ("sub1", "orders.*"),
            ("sub2", "orders.new"),
            ("sub3", "inventory.>"),
            ("sub4", "*.status"),
        ];

        for (sub_id, pattern) in &subs {
            interest_tracker
                .add_local_subscription(sub_id.to_string(), pattern.to_string())
                .unwrap();
        }

        // Test pattern matching for various subjects
        let test_cases = vec![
            ("orders.new", vec!["sub1", "sub2"]),
            ("orders.update", vec!["sub1"]),
            ("inventory.items.widget", vec!["sub3"]),
            ("system.status", vec!["sub4"]),
            ("orders.status", vec!["sub1", "sub4"]),
            ("other.topic", vec![]),
        ];

        for (subject, expected_subs) in test_cases {
            let matched = interest_tracker.get_local_subscriptions_for_subject(subject);
            assert_eq!(
                matched.len(),
                expected_subs.len(),
                "Failed for subject: {}",
                subject
            );

            for expected_sub in expected_subs {
                assert!(
                    matched.contains(&expected_sub.to_string()),
                    "Expected {} to match {}",
                    expected_sub,
                    subject
                );
            }
        }

        // Remove some subscriptions
        interest_tracker.remove_local_subscription("sub1");
        interest_tracker.remove_local_subscription("sub4");

        // Verify removal
        let matched = interest_tracker.get_local_subscriptions_for_subject("orders.new");
        assert_eq!(matched.len(), 1);
        assert!(matched.contains(&"sub2".to_string()));

        let matched = interest_tracker.get_local_subscriptions_for_subject("orders.status");
        assert_eq!(matched.len(), 0);
    }
}
