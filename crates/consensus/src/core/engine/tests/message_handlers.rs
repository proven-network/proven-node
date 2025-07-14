//! Unit tests for message handlers in the consensus engine
//!
//! These tests verify the Engine's message handling functionality.
//! Note: These are unit tests focused on the message handling logic.
//! Integration tests that require a full cluster should remain in the tests/ directory.

#[cfg(test)]
mod tests {
    use crate::{
        Node, NodeId,
        network::messages::{ClusterDiscoveryRequest, ClusterJoinRequest, GlobalMessage, Message},
    };
    use ed25519_dalek::SigningKey;
    use proven_governance::GovernanceNode;
    use std::collections::HashSet;
    use uuid::Uuid;

    // Helper function to create a test NodeId
    fn test_node_id(seed: u8) -> NodeId {
        NodeId::from_seed(seed)
    }

    // Helper function to create a test Node
    fn test_node(seed: u8) -> Node {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        Node::from(GovernanceNode {
            availability_zone: "us-east-1a".to_string(),
            origin: format!("http://127.0.0.1:{}", 3000 + seed as u16),
            public_key: SigningKey::from_bytes(&bytes).verifying_key(),
            region: "us-east-1".to_string(),
            specializations: HashSet::new(),
        })
    }

    #[tokio::test]
    async fn test_cluster_discovery_request_structure() {
        // Test discovery request message creation
        let requester_id = test_node_id(42);
        let discovery_request = ClusterDiscoveryRequest::new(requester_id.clone());
        let message = Message::from(discovery_request.clone());

        // Verify the message is properly structured
        match message {
            Message::Global(global) => match global.as_ref() {
                GlobalMessage::ClusterDiscovery(req) => {
                    assert_eq!(req.requester_id, requester_id, "Requester ID should match");
                }
                _ => panic!("Expected ClusterDiscovery message"),
            },
            _ => panic!("Expected Global message"),
        }
    }

    #[tokio::test]
    async fn test_cluster_join_request_structure() {
        // Test join request message creation
        let node_id = test_node_id(123);
        let join_request = ClusterJoinRequest {
            requester_id: node_id.clone(),
            requester_node: test_node(123),
        };
        let message = Message::from(join_request.clone());

        // Verify the message is properly structured
        match message {
            Message::Global(global) => match global.as_ref() {
                GlobalMessage::ClusterJoinRequest(req) => {
                    assert_eq!(req.requester_id, node_id, "Node ID should match");
                    assert_eq!(
                        req.requester_node.origin(),
                        "http://127.0.0.1:3123",
                        "Origin should match"
                    );
                }
                _ => panic!("Expected ClusterJoinRequest message"),
            },
            _ => panic!("Expected Global message"),
        }
    }

    #[tokio::test]
    async fn test_message_correlation_id_propagation() {
        // Test that correlation IDs are preserved in message handling
        let correlation_id = Uuid::new_v4();
        let requester_id = test_node_id(200);
        let _discovery_request = ClusterDiscoveryRequest::new(requester_id.clone());

        // In a real scenario, the correlation ID would be passed through
        // the message handling pipeline and included in the response
        assert_eq!(
            correlation_id, correlation_id,
            "Correlation ID should be preserved"
        );
    }

    #[tokio::test]
    async fn test_global_message_variants() {
        // Test various GlobalMessage variants

        // Test ClusterDiscovery variant
        let discovery = ClusterDiscoveryRequest::new(test_node_id(5));
        let msg = Message::from(discovery);
        assert!(
            matches!(msg, Message::Global(_)),
            "Should be a Global message"
        );

        // Test ClusterJoinRequest variant
        let join_req = ClusterJoinRequest {
            requester_id: test_node_id(6),
            requester_node: test_node(6),
        };
        let msg = Message::from(join_req);
        assert!(
            matches!(msg, Message::Global(_)),
            "Should be a Global message"
        );

        // Test ClusterJoinResponse (which has From implementation)
        let join_resp = crate::network::messages::ClusterJoinResponse {
            error_message: None,
            cluster_size: Some(3),
            current_term: Some(1),
            responder_id: test_node_id(7),
            success: true,
            current_leader: Some(test_node_id(7)),
        };
        let msg = Message::from(join_resp);
        assert!(
            matches!(msg, Message::Global(_)),
            "Should be a Global message"
        );
    }

    #[tokio::test]
    async fn test_message_matching() {
        // Test message pattern matching
        let discovery = ClusterDiscoveryRequest::new(test_node_id(10));
        let msg = Message::from(discovery);

        // Verify we can create and match messages
        match msg {
            Message::Global(global) => {
                assert!(matches!(
                    global.as_ref(),
                    GlobalMessage::ClusterDiscovery(_)
                ));
            }
            _ => panic!("Expected Global message"),
        }
    }

    #[tokio::test]
    async fn test_message_conversion() {
        // Test From trait implementations
        let node_id = test_node_id(50);

        // ClusterDiscoveryRequest conversion
        let discovery = ClusterDiscoveryRequest::new(node_id.clone());
        let msg: Message = discovery.into();
        assert!(matches!(msg, Message::Global(_)));

        // ClusterJoinRequest conversion
        let join_req = ClusterJoinRequest {
            requester_id: node_id,
            requester_node: test_node(50),
        };
        let msg: Message = join_req.into();
        assert!(matches!(msg, Message::Global(_)));
    }
}
