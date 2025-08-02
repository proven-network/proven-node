//! Handler for RegisterStream command

use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;

use crate::foundation::{
    StreamConfig, StreamName,
    events::{Error as EventError, EventMetadata, RequestHandler},
};
use crate::services::stream::commands::RegisterStream;

/// Handler for RegisterStream command
pub struct RegisterStreamHandler {
    stream_configs: Arc<DashMap<StreamName, StreamConfig>>,
}

impl RegisterStreamHandler {
    pub fn new(stream_configs: Arc<DashMap<StreamName, StreamConfig>>) -> Self {
        Self { stream_configs }
    }
}

#[async_trait]
impl RequestHandler<RegisterStream> for RegisterStreamHandler {
    async fn handle(
        &self,
        request: RegisterStream,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        // Simply register the stream config
        // The storage will be created on-demand when needed
        self.stream_configs
            .insert(request.stream_name.clone(), request.config);

        tracing::info!(
            "Registered stream {} with placement {:?} via consensus callback",
            request.stream_name,
            request.placement
        );

        Ok(())
    }
}
