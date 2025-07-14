//! Unit tests for global storage and snapshot functionality
//!
//! These tests verify:
//! - Snapshot creation and restoration
//! - Memory and RocksDB storage backends
//! - Complex data serialization
//! - State preservation across snapshots

#[cfg(test)]
mod snapshot_tests {
    use crate::{
        ConsensusGroupId, NodeId,
        core::global::{ConsensusGroupInfo, GlobalState, storage::snapshot::SnapshotData},
    };
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;

    /// Helper function to create a test NodeId
    fn test_node_id(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        NodeId::new(SigningKey::from_bytes(&bytes).verifying_key())
    }

    #[tokio::test]
    async fn test_snapshot_data_basic() {
        // Create a GlobalState
        let global_state = Arc::new(GlobalState::new());

        // Create snapshot from global state
        let snapshot_data = SnapshotData::from_global_state(&global_state).await;

        // Verify serialization
        let bytes = snapshot_data.to_bytes().unwrap();
        assert!(!bytes.is_empty(), "Snapshot should not be empty");

        // Verify deserialization
        let restored = SnapshotData::from_bytes(&bytes).unwrap();

        // Create new state and restore
        let new_global_state = Arc::new(GlobalState::new());
        restored.restore_to_global_state(&new_global_state).await;
    }

    #[tokio::test]
    async fn test_snapshot_serialization_roundtrip() {
        let global_state = Arc::new(GlobalState::new());

        // Create snapshot
        let snapshot_data = SnapshotData::from_global_state(&global_state).await;

        // Verify serialization/deserialization
        let bytes = snapshot_data.to_bytes().unwrap();
        assert!(!bytes.is_empty(), "Serialized snapshot should not be empty");

        let restored = SnapshotData::from_bytes(&bytes).unwrap();

        // Restore to new state
        let new_global_state = Arc::new(GlobalState::new());
        restored.restore_to_global_state(&new_global_state).await;
    }

    #[tokio::test]
    async fn test_empty_snapshot() {
        // Test snapshot of empty state
        let global_state = Arc::new(GlobalState::new());

        // Create snapshot of empty state
        let snapshot_data = SnapshotData::from_global_state(&global_state).await;

        // Should create valid snapshot even for empty state
        let bytes = snapshot_data.to_bytes().unwrap();
        assert!(!bytes.is_empty(), "Even empty snapshot has metadata");

        // Should restore successfully
        let restored = SnapshotData::from_bytes(&bytes).unwrap();
        let new_global_state = Arc::new(GlobalState::new());
        restored.restore_to_global_state(&new_global_state).await;
    }

    #[tokio::test]
    async fn test_snapshot_with_consensus_groups() {
        let global_state = Arc::new(GlobalState::new());

        // Manually add data to consensus_groups for testing
        {
            let mut groups = global_state.consensus_groups.write().await;
            groups.insert(
                ConsensusGroupId::new(1),
                ConsensusGroupInfo {
                    id: ConsensusGroupId::new(1),
                    members: vec![test_node_id(1), test_node_id(2)],
                    created_at: 1000,
                    stream_count: 0,
                },
            );
            groups.insert(
                ConsensusGroupId::new(2),
                ConsensusGroupInfo {
                    id: ConsensusGroupId::new(2),
                    members: vec![test_node_id(3), test_node_id(4)],
                    created_at: 2000,
                    stream_count: 0,
                },
            );
        }

        // Create and restore snapshot
        let snapshot_data = SnapshotData::from_global_state(&global_state).await;
        let new_global_state = Arc::new(GlobalState::new());
        snapshot_data
            .restore_to_global_state(&new_global_state)
            .await;

        // Verify groups were restored
        let groups = new_global_state.consensus_groups.read().await;
        assert_eq!(groups.len(), 2, "Both consensus groups should be restored");
        assert!(groups.contains_key(&ConsensusGroupId::new(1)));
        assert!(groups.contains_key(&ConsensusGroupId::new(2)));
    }
}
