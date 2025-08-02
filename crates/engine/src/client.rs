//! Simplified client implementation using EventBus for all service communication
//!
//! This client provides a clean API for interacting with the consensus engine
//! without dealing with routing, forwarding, or service discovery. All requests
//! are sent via the EventBus to the appropriate services.

use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use proven_storage::LogIndex;
use proven_topology::NodeId;
use tokio::time::timeout;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    consensus::{
        global::{GlobalRequest, GlobalResponse},
        group::{GroupRequest, GroupResponse},
    },
    error::{ConsensusResult, Error, ErrorKind},
    foundation::{
        GlobalStateRead, GlobalStateReader, GroupStateInfo, Message, StreamConfig, StreamName,
        events::EventBus,
        models::stream::StreamPlacement,
        types::{ConsensusGroupId, Subject, SubjectPattern},
    },
    services::{
        global_consensus::commands as global_commands,
        group_consensus::commands as group_commands,
        pubsub::{PublishMessage, Subscribe, commands::HasResponders, types::PubSubError},
        stream::commands as stream_commands,
    },
};

/// Simplified client for interacting with the consensus engine
pub struct Client {
    /// Node ID
    node_id: NodeId,
    /// Event bus for sending commands
    event_bus: Arc<EventBus>,
}

impl Client {
    /// Create a new client
    pub fn new(node_id: NodeId, event_bus: Arc<EventBus>) -> Self {
        Self { node_id, event_bus }
    }

    /// Get the node ID of this client
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    // ===== Global Consensus Operations =====

    /// Submit a request to the global consensus layer
    pub async fn submit_global_request(
        &self,
        request: GlobalRequest,
    ) -> ConsensusResult<GlobalResponse> {
        debug!("Submitting global consensus request");

        let cmd = global_commands::SubmitGlobalRequest { request };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Global request failed: {e}"))
        })
    }

    /// Get global consensus state information
    pub async fn get_global_state(&self) -> ConsensusResult<GlobalStateReader> {
        debug!("Getting global consensus state");

        let cmd = global_commands::GetGlobalState {};
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Get global state failed: {e}"))
        })
    }

    // ===== Group Consensus Operations =====

    /// Submit a request to a specific consensus group
    pub async fn submit_group_request(
        &self,
        group_id: ConsensusGroupId,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        debug!("Submitting request to group {:?}", group_id);

        let cmd = group_commands::SubmitToGroup { group_id, request };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Group request failed: {e}"))
        })
    }

    /// Join an existing consensus group
    pub async fn join_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        info!("Joining group {:?}", group_id);

        // For now, joining a group is handled automatically by the GroupConsensusService
        // when it receives membership updates from global consensus.
        // This method is kept for API compatibility but may not be needed.
        Err(Error::with_context(
            ErrorKind::InvalidState,
            "Group membership is managed automatically by global consensus",
        ))
    }

    /// Leave a consensus group
    pub async fn leave_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        info!("Leaving group {:?}", group_id);

        // Similar to join_group, leaving is handled automatically
        Err(Error::with_context(
            ErrorKind::InvalidState,
            "Group membership is managed automatically by global consensus",
        ))
    }

    /// Get information about a specific group
    pub async fn get_group_state(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GroupStateInfo> {
        debug!("Getting state for group {:?}", group_id);

        // GroupStateInfo is a more detailed type that's not directly available via commands
        // For now, we'll need to implement a proper command handler for this
        Err(Error::with_context(
            ErrorKind::InvalidState,
            "Full group state retrieval not yet implemented. Use get_group_info instead.",
        ))
    }

    /// Get basic information about a group
    pub async fn get_group_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<group_commands::GroupInfo>> {
        debug!("Getting info for group {:?}", group_id);

        let cmd = group_commands::GetGroupInfo { group_id };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Get group info failed: {e}"))
        })
    }

    // ===== Stream Operations =====

    /// Create a new stream in a consensus group (automatically selects the best group)
    pub async fn create_group_stream(
        &self,
        name: String,
        config: StreamConfig,
    ) -> ConsensusResult<ConsensusGroupId> {
        info!("Creating stream '{}'", name);

        let cmd = global_commands::CreateGroupStream {
            stream_name: name.into(),
            config,
            target_group: None, // Let global consensus choose the best group
        };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Create stream failed: {e}"))
        })
    }

    /// Create a new global stream (remains in global consensus for maximum redundancy)
    pub async fn create_global_stream(
        &self,
        name: String,
        config: StreamConfig,
    ) -> ConsensusResult<crate::foundation::models::StreamInfo> {
        info!("Creating global stream '{}'", name);

        let cmd = global_commands::CreateGlobalStream {
            stream_name: name.into(),
            config,
        };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Create global stream failed: {e}"),
            )
        })
    }

    /// Delete a stream
    pub async fn delete_stream(&self, name: String) -> ConsensusResult<()> {
        info!("Deleting stream '{}'", name);

        let cmd = stream_commands::DeleteStream { name: name.into() };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Delete stream failed: {e}"))
        })
    }

    /// Write messages to a stream (goes through consensus)
    pub async fn publish_to_stream<M>(
        &self,
        stream_name: String,
        messages: Vec<M>,
    ) -> ConsensusResult<LogIndex>
    where
        M: Into<crate::foundation::Message>,
    {
        debug!(
            "Writing {} messages to stream '{}'",
            messages.len(),
            stream_name
        );

        // First, we need to find which group owns this stream
        let stream_info = self.get_stream_info(&stream_name).await?.ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("Stream '{stream_name}' not found"),
            )
        })?;

        // Convert messages to Message type
        let messages: Vec<crate::foundation::Message> =
            messages.into_iter().map(Into::into).collect();

        // Create the append request with current timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if let StreamPlacement::Group(group_id) = stream_info.placement {
            let group_request =
                GroupRequest::Stream(crate::consensus::group::types::StreamOperation::Append {
                    stream_name: stream_name.clone().into(),
                    messages,
                    timestamp,
                });

            // Submit the append request to the owning group
            let response = self.submit_group_request(group_id, group_request).await?;

            match response {
                GroupResponse::Appended { sequence, .. } => Ok(sequence),
                _ => Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Expected stream response from group consensus",
                )),
            }
        } else {
            let global_request = GlobalRequest::AppendToGlobalStream {
                stream_name: stream_name.into(),
                messages,
                timestamp,
            };

            let response = self.submit_global_request(global_request).await?;

            match response {
                GlobalResponse::Appended { sequence, .. } => Ok(sequence),
                _ => Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Expected stream response from global consensus",
                )),
            }
        }
    }

    /// Stream messages from a stream
    pub async fn stream_messages(
        &self,
        stream_name: String,
        start_sequence: Option<LogIndex>,
    ) -> ConsensusResult<impl Stream<Item = (Message, u64, u64)>> {
        debug!(
            "Creating stream for '{}' from {:?}",
            stream_name, start_sequence
        );

        let cmd = stream_commands::StreamMessages {
            stream_name,
            start_sequence,
        };

        let receiver = self.event_bus.stream(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Stream messages failed: {e}"))
        })?;

        // Convert flume::Receiver to a futures::Stream
        Ok(futures::stream::unfold(receiver, |rx| async move {
            match rx.recv_async().await {
                Ok(item) => Some((item, rx)),
                Err(_) => None,
            }
        }))
    }

    /// Get information about a stream from global consensus
    pub async fn get_stream_info(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::foundation::models::StreamInfo>> {
        debug!(
            "Getting info for stream '{}' from global consensus",
            stream_name
        );

        // Get the global state snapshot
        let global_state = self.get_global_state().await?;

        // Find the stream in the snapshot
        Ok(global_state
            .get_all_streams()
            .await
            .into_iter()
            .find(|s| s.stream_name.as_str() == stream_name))
    }

    /// Get detailed stream state from group consensus
    pub async fn get_stream_state(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::foundation::models::stream::StreamState>> {
        debug!(
            "Getting state for stream '{}' from group consensus",
            stream_name
        );

        // First, get the stream info to find which group owns it
        let stream_info = self.get_stream_info(stream_name).await?.ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("Stream '{stream_name}' not found"),
            )
        })?;

        if let StreamPlacement::Group(group_id) = stream_info.placement {
            // Query the group consensus for stream state
            let cmd = group_commands::GetStreamState {
                group_id,
                stream_name: StreamName::from(stream_name),
            };

            self.event_bus.request(cmd).await.map_err(|e| {
                Error::with_context(ErrorKind::Service, format!("Get stream state failed: {e}"))
            })
        } else {
            todo!("Need to add global stream states")
        }
    }

    /// Delete a specific message from a stream
    pub async fn delete_message(
        &self,
        stream_name: String,
        sequence: LogIndex,
    ) -> ConsensusResult<GroupResponse> {
        debug!(
            "Deleting message at sequence {:?} from stream '{}'",
            sequence, stream_name
        );

        // First, find which group owns this stream
        let stream_info = self.get_stream_info(&stream_name).await?.ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("Stream '{stream_name}' not found"),
            )
        })?;

        if let StreamPlacement::Group(group_id) = stream_info.placement {
            // Create the delete request
            let request =
                GroupRequest::Stream(crate::consensus::group::types::StreamOperation::Delete {
                    stream_name: stream_name.into(),
                    sequence,
                });

            // Submit the delete request to the owning group
            self.submit_group_request(group_id, request).await
        } else {
            todo!("Need to add global stream deletion")
        }
    }

    // ===== PubSub Operations =====

    /// Publish messages to a subject
    pub async fn publish<M>(&self, subject: &str, messages: Vec<M>) -> ConsensusResult<()>
    where
        M: Into<crate::foundation::Message>,
    {
        debug!(
            "Publishing {} messages to subject '{}'",
            messages.len(),
            subject
        );

        // Validate subject
        let subject = Subject::new(subject).map_err(|e| {
            Error::with_context(ErrorKind::Validation, format!("Invalid subject: {e}"))
        })?;

        // Convert messages and add subject header
        let messages: Vec<crate::foundation::Message> = messages
            .into_iter()
            .map(|m| m.into().with_subject(subject.as_str()))
            .collect();

        let cmd = PublishMessage { subject, messages };
        self.event_bus
            .request(cmd)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Service, format!("Publish failed: {e}")))
    }

    /// Get information about available topics
    pub async fn list_topics(&self) -> ConsensusResult<Vec<String>> {
        debug!("Listing available topics");

        // For now, topic listing is not implemented in the pubsub service
        // This would require tracking all active subscriptions
        Err(Error::with_context(
            ErrorKind::InvalidState,
            "Topic listing not yet implemented",
        ))
    }

    /// Subscribe to a subject pattern with streaming
    pub async fn subscribe(
        &self,
        subject_pattern: &str,
        queue_group: Option<String>,
    ) -> ConsensusResult<impl Stream<Item = Message> + Send + 'static> {
        debug!(
            "Creating streaming subscription to pattern '{}'",
            subject_pattern
        );

        let pattern = SubjectPattern::new(subject_pattern).map_err(|e| {
            Error::with_context(
                ErrorKind::Validation,
                format!("Invalid subject pattern: {e}"),
            )
        })?;

        let cmd = Subscribe {
            subject_pattern: pattern,
            queue_group,
        };

        // Get the stream of messages
        let receiver = self.event_bus.stream(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Subscribe failed: {e}"))
        })?;

        // Convert the flume receiver to a futures Stream
        let stream = receiver.into_stream();

        Ok(stream)
    }

    /// Send a request and wait for a reply
    pub async fn request(
        &self,
        subject: &str,
        message: impl Into<Message>,
        timeout_duration: Duration,
    ) -> ConsensusResult<Message> {
        debug!("Sending request to subject '{}'", subject);

        // 1. Validate subject
        let subject_obj = Subject::new(subject).map_err(|e| {
            Error::with_context(ErrorKind::Validation, format!("Invalid subject: {e}"))
        })?;

        // 2. Generate unique correlation_id
        let correlation_id = Uuid::new_v4().to_string();

        // 3. Create inbox subject
        let inbox = format!("_INBOX.{}.{}", self.node_id, correlation_id);

        // 4. Subscribe to inbox BEFORE checking for responders
        let mut inbox_sub = self.subscribe(&inbox, None).await?;

        // 5. Check if anyone is listening (fail-fast)
        let has_responders_cmd = HasResponders {
            subject: subject_obj.clone(),
        };
        let has_responders = self
            .event_bus
            .request(has_responders_cmd)
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Service,
                    format!("Failed to check responders: {e}"),
                )
            })?;

        if !has_responders {
            return Err(Error::with_context(
                ErrorKind::Service,
                format!("No responders for subject: {subject}"),
            ));
        }

        // 6. Prepare message with headers
        let message = message
            .into()
            .with_header(
                crate::foundation::messages::headers::CORRELATION_ID_STR,
                &correlation_id,
            )
            .with_header(crate::foundation::messages::headers::REPLY_TO_STR, &inbox)
            .with_subject(subject);

        // 7. Publish request
        self.publish(subject, vec![message]).await?;

        // 8. Wait for response with timeout
        match timeout(timeout_duration, inbox_sub.next()).await {
            Ok(Some(response)) => {
                // Verify correlation_id matches
                if response.get_header(crate::foundation::messages::headers::CORRELATION_ID_STR)
                    == Some(&correlation_id)
                {
                    Ok(response)
                } else {
                    Err(Error::with_context(
                        ErrorKind::InvalidState,
                        "Correlation ID mismatch in response",
                    ))
                }
            }
            Ok(None) => Err(Error::with_context(
                ErrorKind::InvalidState,
                "Inbox subscription closed",
            )),
            Err(_) => Err(Error::with_context(
                ErrorKind::Timeout,
                format!("Request timed out after {timeout_duration:?}"),
            )),
        }
    }

    // ===== Query Operations =====

    /// Execute a query across streams
    pub async fn query(&self, query: String, _timeout: Duration) -> ConsensusResult<Vec<Message>> {
        debug!("Executing query: {}", query);

        // For now, queries are not implemented - this is a placeholder
        // In the future, this would parse the query and execute it across relevant streams
        Err(Error::with_context(
            ErrorKind::InvalidState,
            "Query functionality not yet implemented",
        ))
    }

    // ===== Health and Status =====

    // ===== Engine State Operations =====

    /// Get all consensus groups this node is a member of
    pub async fn node_groups(&self) -> ConsensusResult<Vec<ConsensusGroupId>> {
        debug!("Getting node groups");

        let cmd = group_commands::GetNodeGroups;
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Get node groups failed: {e}"))
        })
    }

    /// Get state information for a specific consensus group
    pub async fn group_state(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupStateInfo>> {
        debug!("Getting state for group {:?}", group_id);

        let cmd = group_commands::GetGroupState { group_id };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Get group state failed: {e}"))
        })
    }

    /// Get global consensus members
    pub async fn global_consensus_members(&self) -> ConsensusResult<Vec<NodeId>> {
        let cmd = global_commands::GetGlobalConsensusMembers;
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Get global consensus members failed: {e}"),
            )
        })
    }

    /// Get global consensus leader
    pub async fn global_leader(&self) -> ConsensusResult<Option<NodeId>> {
        let cmd = global_commands::GetGlobalLeader;
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Get global consensus leader failed: {e}"),
            )
        })
    }

    // ===== Node Information Operations =====

    /// Get information about the current node
    pub async fn node_info(&self) -> ConsensusResult<proven_topology::Node> {
        debug!("Getting current node info");

        let cmd = crate::services::membership::commands::GetNodeInfo;
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Get node info failed: {e}"))
        })
    }

    /// Get information about a specific peer
    pub async fn peer_info(
        &self,
        node_id: NodeId,
    ) -> ConsensusResult<Option<proven_topology::Node>> {
        debug!("Getting info for peer {}", node_id);

        let cmd = crate::services::membership::commands::GetPeerInfo { node_id };
        self.event_bus.request(cmd).await.map_err(|e| {
            Error::with_context(ErrorKind::Service, format!("Get peer info failed: {e}"))
        })
    }

    /// Check if the client can connect to services
    pub async fn health_check(&self) -> ConsensusResult<bool> {
        // Try to get global state as a health check
        match self.get_global_state().await {
            Ok(_) => Ok(true),
            Err(e) => {
                error!("Health check failed: {}", e);
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::events::EventBusBuilder;

    #[tokio::test]
    async fn test_client_creation() {
        let node_id = NodeId::from_seed(1);
        let event_bus = Arc::new(EventBusBuilder::new().build());

        let client = Client::new(node_id.clone(), event_bus);
        assert_eq!(client.node_id, node_id);
    }

    #[tokio::test]
    async fn test_health_check_fails_without_services() {
        let node_id = NodeId::from_seed(1);
        let event_bus = Arc::new(EventBusBuilder::new().build());

        let client = Client::new(node_id, event_bus);

        // Without services registered, health check should fail
        let result = client.health_check().await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
