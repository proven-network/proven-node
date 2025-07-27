//! Handler for GetGlobalState command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use crate::foundation::{
    GlobalStateRead, GlobalStateReader,
    events::{Error as EventError, EventMetadata, RequestHandler},
};
use crate::services::global_consensus::commands::{
    GetGlobalState, GlobalStateSnapshot, GroupSnapshot, StreamSnapshot,
};

/// Handler for GetGlobalState command
pub struct GetGlobalStateHandler {
    global_state: Arc<RwLock<Option<GlobalStateReader>>>,
}

impl GetGlobalStateHandler {
    pub fn new(global_state: Arc<RwLock<Option<GlobalStateReader>>>) -> Self {
        Self { global_state }
    }
}

#[async_trait]
impl RequestHandler<GetGlobalState> for GetGlobalStateHandler {
    async fn handle(
        &self,
        _request: GetGlobalState,
        _metadata: EventMetadata,
    ) -> Result<GlobalStateSnapshot, EventError> {
        debug!("GetGlobalStateHandler: Getting global state");

        // Check if global state is initialized
        let state_guard = self.global_state.read().await;
        let state = state_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global state not initialized".to_string()))?;

        // Get all groups
        let groups = state.get_all_groups().await;
        let group_snapshots: Vec<GroupSnapshot> = groups
            .into_iter()
            .map(|info| GroupSnapshot {
                group_id: info.id,
                members: info.members,
            })
            .collect();

        // Get all streams
        let streams = state.get_all_streams().await;
        let stream_snapshots: Vec<StreamSnapshot> = streams
            .into_iter()
            .map(|info| StreamSnapshot {
                stream_name: info.name,
                config: info.config,
                group_id: info.group_id,
            })
            .collect();

        Ok(GlobalStateSnapshot {
            groups: group_snapshots,
            streams: stream_snapshots,
        })
    }
}
