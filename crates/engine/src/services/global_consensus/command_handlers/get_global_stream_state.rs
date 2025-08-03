//! Handler for GetGlobalStreamState command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::foundation::models::stream::StreamState;
use crate::foundation::{GlobalStateRead, GlobalStateReader, StreamName};
use crate::services::global_consensus::commands::GetGlobalStreamState;

/// Handler for GetGlobalStreamState command
pub struct GetGlobalStreamStateHandler {
    /// Global state reader
    global_state: Arc<RwLock<Option<GlobalStateReader>>>,
}

impl GetGlobalStreamStateHandler {
    pub fn new(global_state: Arc<RwLock<Option<GlobalStateReader>>>) -> Self {
        Self { global_state }
    }
}

#[async_trait]
impl RequestHandler<GetGlobalStreamState> for GetGlobalStreamStateHandler {
    async fn handle(
        &self,
        request: GetGlobalStreamState,
        _metadata: EventMetadata,
    ) -> Result<Option<StreamState>, EventError> {
        // Get the global state reader
        let state_guard = self.global_state.read().await;
        let state = state_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global state not initialized".to_string()))?;

        // Get the stream state
        Ok(state.get_global_stream_state(&request.stream_name).await)
    }
}
