//! Global consensus operations
//!
//! This module defines operations that can be performed on the global state
//! and their handlers.

use async_trait::async_trait;
use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};

use crate::error::ConsensusResult;
use crate::foundation::models::stream::StreamPlacement;
use crate::foundation::{
    GlobalStateRead, GlobalStateWrite, GlobalStateWriter, traits::OperationHandler,
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
                stream_name,
                config,
                placement,
            } => {
                // Only validate for current operations (skip for replay)
                if !is_replay {
                    // Check if stream already exists
                    if let Some(existing) = self.state.get_stream(&stream_name).await {
                        return Ok(GlobalResponse::StreamAlreadyExists {
                            stream_name: stream_name.clone(),
                            placement: existing.placement,
                        });
                    }

                    // Check if group exists for group placements
                    if let StreamPlacement::Group(group_id) = placement
                        && self.state.get_group(&group_id).await.is_none()
                    {
                        return Ok(GlobalResponse::error(format!(
                            "Group {group_id:?} does not exist"
                        )));
                    }
                }

                // Create stream
                let info = crate::foundation::models::StreamInfo {
                    stream_name: stream_name.clone(),
                    config,
                    placement,
                    created_at: operation.timestamp,
                };

                self.state.add_stream(info).await?;

                Ok(GlobalResponse::StreamCreated {
                    stream_name,
                    placement,
                })
            }

            GlobalRequest::DeleteStream { stream_name } => {
                if self.state.remove_stream(&stream_name).await.is_some() {
                    Ok(GlobalResponse::StreamDeleted { stream_name })
                } else {
                    Ok(GlobalResponse::error(format!(
                        "Stream {stream_name} not found"
                    )))
                }
            }

            GlobalRequest::UpdateStreamConfig {
                stream_name,
                config,
            } => {
                if self.state.update_stream_config(&stream_name, config).await {
                    Ok(GlobalResponse::Success)
                } else {
                    Ok(GlobalResponse::error(format!(
                        "Stream {stream_name} not found"
                    )))
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

            GlobalRequest::AddNodeToGroup { node_id, metadata } => {
                let info = crate::foundation::models::NodeInfo {
                    node_id: node_id.clone(),
                    joined_at: operation.timestamp,
                    metadata,
                };

                self.state.add_member(info).await;

                Ok(GlobalResponse::NodeAdded { node_id })
            }

            GlobalRequest::RemoveNodeFromGroup { node_id } => {
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

            GlobalRequest::ReassignStream {
                stream_name,
                new_placement,
            } => {
                // Get current stream info to track old placement
                let stream_info = match self.state.get_stream(&stream_name).await {
                    Some(info) => info,
                    None => {
                        return Ok(GlobalResponse::error(format!(
                            "Stream {stream_name} not found"
                        )));
                    }
                };

                let old_placement = stream_info.placement;

                // If reassigning to a group, verify the group exists
                if let StreamPlacement::Group(group_id) = new_placement {
                    // Check if target group exists
                    if self.state.get_group(&group_id).await.is_none() {
                        return Ok(GlobalResponse::Error {
                            message: format!("Target group {group_id:?} does not exist"),
                        });
                    }
                }

                // Reassign the stream to the new placement
                if self
                    .state
                    .reassign_stream(&stream_name, new_placement)
                    .await
                {
                    Ok(GlobalResponse::StreamReassigned {
                        stream_name,
                        old_placement,
                        new_placement,
                    })
                } else {
                    Ok(GlobalResponse::error(
                        "Failed to reassign stream (concurrent modification?)",
                    ))
                }
            }

            GlobalRequest::AppendToGlobalStream {
                stream_name,
                messages,
                timestamp,
            } => {
                // First check if stream exists and verify it's a global stream
                let stream_info = match self.state.get_stream(&stream_name).await {
                    Some(info) => info,
                    None => {
                        return Ok(GlobalResponse::error(format!(
                            "Stream {stream_name} not found"
                        )));
                    }
                };

                // Verify this is actually a global stream
                match stream_info.placement {
                    StreamPlacement::Global => {
                        // Continue with append
                    }
                    StreamPlacement::Group(_) => {
                        return Ok(GlobalResponse::error(format!(
                            "Stream {stream_name} is not a global stream"
                        )));
                    }
                }

                // Append messages to global stream - this handles state tracking internally
                let (entries, last_sequence) = self
                    .state
                    .append_to_global_stream(&stream_name, messages, timestamp)
                    .await;

                // Return error if no sequence was returned (meaning no messages were appended)
                match last_sequence {
                    Some(sequence) => Ok(GlobalResponse::Appended {
                        stream_name,
                        sequence,
                        entries: Some(entries),
                    }),
                    None => Ok(GlobalResponse::error(format!(
                        "Failed to append messages to stream {stream_name}"
                    ))),
                }
            }

            GlobalRequest::TrimGlobalStream {
                stream_name,
                up_to_seq,
            } => {
                // First check if stream exists and verify it's a global stream
                let stream_info = match self.state.get_stream(&stream_name).await {
                    Some(info) => info,
                    None => {
                        return Ok(GlobalResponse::error(format!(
                            "Stream {stream_name} not found"
                        )));
                    }
                };

                // Verify this is actually a global stream
                match stream_info.placement {
                    StreamPlacement::Global => {
                        // Continue with trim
                    }
                    StreamPlacement::Group(_) => {
                        return Ok(GlobalResponse::error(format!(
                            "Stream {stream_name} is not a global stream"
                        )));
                    }
                }

                // Trim the global stream
                if let Some(new_start_seq) =
                    self.state.trim_global_stream(&stream_name, up_to_seq).await
                {
                    Ok(GlobalResponse::GlobalStreamTrimmed {
                        stream_name,
                        new_start_seq,
                    })
                } else {
                    Ok(GlobalResponse::error(format!(
                        "Failed to trim stream {stream_name} (invalid sequence range?)"
                    )))
                }
            }

            GlobalRequest::DeleteFromGlobalStream {
                stream_name,
                sequence,
            } => {
                // First check if stream exists and verify it's a global stream
                let stream_info = match self.state.get_stream(&stream_name).await {
                    Some(info) => info,
                    None => {
                        return Ok(GlobalResponse::error(format!(
                            "Stream {stream_name} not found"
                        )));
                    }
                };

                // Verify this is actually a global stream
                match stream_info.placement {
                    StreamPlacement::Global => {
                        // Continue with delete
                    }
                    StreamPlacement::Group(_) => {
                        return Ok(GlobalResponse::error(format!(
                            "Stream {stream_name} is not a global stream"
                        )));
                    }
                }

                // Delete from the global stream
                if let Some(deleted_seq) = self
                    .state
                    .delete_from_global_stream(&stream_name, sequence)
                    .await
                {
                    Ok(GlobalResponse::GlobalStreamMessageDeleted {
                        stream_name,
                        sequence: deleted_seq,
                    })
                } else {
                    Ok(GlobalResponse::error(format!(
                        "Failed to delete message from stream {stream_name} (sequence out of range?)"
                    )))
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
