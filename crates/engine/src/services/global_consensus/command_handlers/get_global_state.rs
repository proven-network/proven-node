//! Handler for GetGlobalState command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use crate::foundation::{
    GlobalStateRead, GlobalStateReader,
    events::{Error as EventError, EventMetadata, RequestHandler},
};
use crate::services::global_consensus::commands::GetGlobalState;

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
    ) -> Result<GlobalStateReader, EventError> {
        debug!("GetGlobalStateHandler: Getting global state");

        // Check if global state is initialized
        let state_guard = self.global_state.read().await;
        let state = state_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global state not initialized".to_string()))?;

        Ok(state.clone())
    }
}
