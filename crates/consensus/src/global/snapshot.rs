//! Snapshot data structures for consensus
//!
//! This module provides serialization and deserialization of GlobalState
//! for use in openraft snapshots.

use base64::prelude::*;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

use super::global_state::{
    GlobalState, MessageData as InternalMessageData, StreamData as InternalStreamData,
};

/// Snapshot data containing all StreamStore state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// Stream data storage
    pub streams: HashMap<String, StreamData>,
    /// Subject router data
    pub subject_router: SubjectRouterData,
    // Note: subscription handlers are runtime-only and cannot be serialized
    // They must be re-registered after snapshot restore
}

/// Serializable stream data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamData {
    /// Messages in the stream
    pub messages: BTreeMap<u64, MessageData>,
    /// Next sequence number
    pub next_sequence: u64,
    /// Subject subscriptions for this stream
    pub subscriptions: HashSet<String>,
}

/// Serializable message data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    /// Message content (base64 encoded for JSON compatibility)
    pub data: String,
    /// Optional metadata
    pub metadata: Option<HashMap<String, String>>,
    /// Timestamp
    pub timestamp: u64,
}

/// Serializable subject router data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectRouterData {
    /// Subject pattern to streams mapping
    pub subscriptions: HashMap<String, HashSet<String>>,
}

impl SnapshotData {
    /// Create a snapshot from a GlobalState
    pub async fn from_global_state(global_state: &GlobalState) -> Self {
        let streams = global_state.streams.read().await;
        let subject_router = global_state.subject_router.read().await;

        let snapshot_streams = streams
            .iter()
            .map(|(name, data)| {
                let snapshot_messages = data
                    .messages
                    .iter()
                    .map(|(seq, msg)| {
                        let snapshot_msg = MessageData {
                            data: BASE64_STANDARD.encode(&msg.data),
                            metadata: msg.metadata.clone(),
                            timestamp: msg.timestamp,
                        };
                        (*seq, snapshot_msg)
                    })
                    .collect();

                let snapshot_data = StreamData {
                    messages: snapshot_messages,
                    next_sequence: data.next_sequence,
                    subscriptions: data.subscriptions.clone(),
                };

                (name.clone(), snapshot_data)
            })
            .collect();

        let snapshot_router = SubjectRouterData {
            subscriptions: subject_router.get_subscriptions().clone(),
        };

        SnapshotData {
            streams: snapshot_streams,
            subject_router: snapshot_router,
        }
    }

    /// Restore a StreamStore from snapshot data
    pub async fn restore_to_global_state(&self, global_state: &GlobalState) {
        // Clear existing data
        {
            let mut streams = global_state.streams.write().await;
            streams.clear();
        }

        {
            let mut subject_router = global_state.subject_router.write().await;
            subject_router.clear();
        }

        // Restore streams
        {
            let mut streams = global_state.streams.write().await;
            for (name, snapshot_data) in &self.streams {
                let restored_messages = snapshot_data
                    .messages
                    .iter()
                    .map(|(seq, snapshot_msg)| {
                        let data = BASE64_STANDARD
                            .decode(&snapshot_msg.data)
                            .unwrap_or_else(|_| Vec::new());
                        let msg = InternalMessageData {
                            data: Bytes::from(data),
                            metadata: snapshot_msg.metadata.clone(),
                            timestamp: snapshot_msg.timestamp,
                            source: super::state_machine::MessageSource::Consensus, // Default to consensus source for restored messages
                        };
                        (*seq, msg)
                    })
                    .collect();

                let restored_data = InternalStreamData {
                    messages: restored_messages,
                    next_sequence: snapshot_data.next_sequence,
                    subscriptions: snapshot_data.subscriptions.clone(),
                };

                streams.insert(name.clone(), restored_data);
            }
        }

        // Restore subject router
        {
            let mut subject_router = global_state.subject_router.write().await;
            for (pattern, stream_names) in &self.subject_router.subscriptions {
                for stream_name in stream_names {
                    subject_router.subscribe_stream(stream_name, pattern);
                }
            }
        }

        // Note: Subscription handlers are runtime-only and must be re-registered
        // after snapshot restore by the application
    }

    /// Serialize to JSON bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::GlobalOperation;
    use crate::global::GlobalState;

    #[tokio::test]
    async fn test_snapshot_serialization() {
        let store = GlobalState::new();

        // First create a consensus group
        let response = store
            .apply_operation(
                &GlobalOperation::AddConsensusGroup {
                    group_id: crate::allocation::ConsensusGroupId::new(1),
                    members: vec![crate::NodeId::from_seed(1)],
                },
                1,
            )
            .await;
        assert!(response.success);

        // In the hierarchical model, we only test admin operations
        // Now create a stream
        let response = store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "test-stream".to_string(),
                    config: crate::global::StreamConfig::default(),
                    group_id: crate::allocation::ConsensusGroupId::new(1),
                },
                2,
            )
            .await;
        assert!(response.success);

        // Then subscribe to a subject (this is an admin operation)
        let response = store
            .apply_operation(
                &GlobalOperation::SubscribeToSubject {
                    stream_name: "test-stream".to_string(),
                    subject_pattern: "foo.*".to_string(),
                },
                3,
            )
            .await;
        if !response.success {
            panic!("Failed to subscribe: {:?}", response.error);
        }

        // Create snapshot
        let snapshot = SnapshotData::from_global_state(&store).await;

        // Test serialization
        let bytes = snapshot.to_bytes().unwrap();
        let deserialized = SnapshotData::from_bytes(&bytes).unwrap();

        // Verify deserialization worked
        assert_eq!(
            deserialized.subject_router.subscriptions.len(),
            snapshot.subject_router.subscriptions.len()
        );
    }

    #[tokio::test]
    async fn test_snapshot_restore() {
        let original_store = GlobalState::new();

        // First create a consensus group
        let response = original_store
            .apply_operation(
                &GlobalOperation::AddConsensusGroup {
                    group_id: crate::allocation::ConsensusGroupId::new(1),
                    members: vec![crate::NodeId::from_seed(1)],
                },
                1,
            )
            .await;
        assert!(response.success);

        // Create streams first
        let response = original_store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "test-stream".to_string(),
                    config: crate::global::StreamConfig::default(),
                    group_id: crate::allocation::ConsensusGroupId::new(1),
                },
                2,
            )
            .await;
        assert!(response.success);

        let response = original_store
            .apply_operation(
                &GlobalOperation::CreateStream {
                    stream_name: "test-stream-2".to_string(),
                    config: crate::global::StreamConfig::default(),
                    group_id: crate::allocation::ConsensusGroupId::new(1),
                },
                3,
            )
            .await;
        assert!(response.success);

        // Subscribe to a subject (admin operation)
        let response = original_store
            .apply_operation(
                &GlobalOperation::SubscribeToSubject {
                    stream_name: "test-stream".to_string(),
                    subject_pattern: "foo.*".to_string(),
                },
                4,
            )
            .await;
        assert!(response.success);

        // Subscribe to another subject
        let response = original_store
            .apply_operation(
                &GlobalOperation::SubscribeToSubject {
                    stream_name: "test-stream-2".to_string(),
                    subject_pattern: "bar.*".to_string(),
                },
                5,
            )
            .await;
        assert!(response.success);

        // Create snapshot
        let snapshot = SnapshotData::from_global_state(&original_store).await;

        // Create new store and restore
        let new_store = GlobalState::new();
        snapshot.restore_to_global_state(&new_store).await;

        // Verify subject routing still works after restore
        let routed_streams = new_store.route_subject("foo.bar").await;
        assert!(routed_streams.contains("test-stream"));

        let routed_streams2 = new_store.route_subject("bar.baz").await;
        assert!(routed_streams2.contains("test-stream-2"));
    }
}
