//! Global consensus operations
//!
//! This module defines operations that can be performed on the global state
//! and their handlers.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::{
    GlobalState, GlobalStateRead, GlobalStateWrite, GlobalStateWriter, traits::OperationHandler,
    types::OperationId, validations,
};

use super::types::{GlobalRequest, GlobalResponse};

/// Global consensus operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalOperation {
    /// Operation ID
    pub id: OperationId,
    /// The request
    pub request: GlobalRequest,
    /// Timestamp
    pub timestamp: u64,
}

impl GlobalOperation {
    /// Create a new operation
    pub fn new(request: GlobalRequest) -> Self {
        Self {
            id: OperationId::new(),
            request,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// Handler for global consensus operations
pub struct GlobalOperationHandler {
    /// Global state
    state: GlobalStateWriter,
}

impl GlobalOperationHandler {
    /// Create a new handler
    pub fn new(state: GlobalStateWriter) -> Self {
        Self { state }
    }
}

#[async_trait]
impl OperationHandler for GlobalOperationHandler {
    type Operation = GlobalOperation;
    type Response = GlobalResponse;

    async fn handle(
        &self,
        operation: Self::Operation,
        is_replay: bool,
    ) -> ConsensusResult<Self::Response> {
        // Only validate for current operations (skip for replay)
        if !is_replay {
            self.validate(&operation).await?;
        }
        match operation.request {
            GlobalRequest::CreateStream {
                name,
                config,
                group_id,
            } => {
                // Only validate for current operations (skip for replay)
                if !is_replay {
                    // Check if stream already exists
                    if let Some(existing) = self.state.get_stream(&name).await {
                        return Ok(GlobalResponse::StreamAlreadyExists {
                            name: name.clone(),
                            group_id: existing.group_id,
                        });
                    }

                    // Check if group exists
                    if self.state.get_group(&group_id).await.is_none() {
                        return Ok(GlobalResponse::error(format!(
                            "Group {group_id:?} does not exist"
                        )));
                    }
                }

                // Create stream
                let info = crate::foundation::models::StreamInfo {
                    name: name.clone(),
                    config,
                    group_id,
                    created_at: operation.timestamp,
                };

                self.state.add_stream(info).await?;

                Ok(GlobalResponse::StreamCreated { name, group_id })
            }

            GlobalRequest::DeleteStream { name } => {
                if self.state.remove_stream(&name).await.is_some() {
                    Ok(GlobalResponse::StreamDeleted { name })
                } else {
                    Ok(GlobalResponse::error(format!("Stream {name} not found")))
                }
            }

            GlobalRequest::UpdateStreamConfig { name, config } => {
                if self.state.update_stream_config(&name, config).await {
                    Ok(GlobalResponse::Success)
                } else {
                    Ok(GlobalResponse::error(format!("Stream {name} not found")))
                }
            }

            GlobalRequest::CreateGroup { info } => {
                // Only validate for current operations (skip for replay)
                if !is_replay {
                    // Check if group already exists
                    if self.state.get_group(&info.id).await.is_some() {
                        return Ok(GlobalResponse::Error {
                            message: format!("Group {:?} already exists", info.id),
                        });
                    }
                }

                self.state.add_group(info.clone()).await?;

                Ok(GlobalResponse::GroupCreated {
                    id: info.id,
                    group_info: info,
                })
            }

            GlobalRequest::DissolveGroup { id } => {
                // Check if group has streams
                let stream_count = self.state.count_streams_in_group(id).await;
                if stream_count > 0 {
                    return Ok(GlobalResponse::Error {
                        message: format!("Group {id:?} has {stream_count} active streams"),
                    });
                }

                if self.state.remove_group(id).await.is_some() {
                    Ok(GlobalResponse::GroupDissolved { id })
                } else {
                    Ok(GlobalResponse::Error {
                        message: format!("Group {id:?} not found"),
                    })
                }
            }

            GlobalRequest::AddNode { node_id, metadata } => {
                let info = crate::foundation::models::NodeInfo {
                    node_id: node_id.clone(),
                    joined_at: operation.timestamp,
                    metadata,
                };

                self.state.add_member(info).await;

                Ok(GlobalResponse::NodeAdded { node_id })
            }

            GlobalRequest::RemoveNode { node_id } => {
                // Check if node is in any groups
                let groups = self.state.get_node_groups(&node_id).await;
                if !groups.is_empty() {
                    return Ok(GlobalResponse::Error {
                        message: format!("Node {} is member of {} groups", node_id, groups.len()),
                    });
                }

                if self.state.remove_member(&node_id).await.is_some() {
                    Ok(GlobalResponse::NodeRemoved { node_id })
                } else {
                    Ok(GlobalResponse::Error {
                        message: format!("Node {node_id} not found"),
                    })
                }
            }

            GlobalRequest::ReassignStream { name, to_group } => {
                // Check if target group exists
                if self.state.get_group(&to_group).await.is_none() {
                    return Ok(GlobalResponse::Error {
                        message: format!("Target group {to_group:?} does not exist"),
                    });
                }

                if self.state.reassign_stream(&name, to_group).await {
                    Ok(GlobalResponse::Success)
                } else {
                    Ok(GlobalResponse::error(format!("Stream {name} not found")))
                }
            }
        }
    }

    async fn validate(&self, operation: &Self::Operation) -> ConsensusResult<()> {
        // Basic validation
        match &operation.request {
            GlobalRequest::CreateStream { config, .. } => {
                validations::greater_than_zero(config.max_message_size as u64, "Max message size")?;
            }
            GlobalRequest::CreateGroup { info } => {
                validations::not_empty(&info.members, "Group members")?;
            }
            _ => {}
        }

        Ok(())
    }
}
