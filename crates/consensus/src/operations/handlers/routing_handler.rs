//! Handler for routing operations (PubSub subscriptions)

use async_trait::async_trait;
use tracing::{debug, info};

use crate::{core::global::GlobalState, error::ConsensusResult, operations::RoutingOperation};

use super::{GlobalOperationHandler, OperationContext, RoutingOperationResponse};

/// Handler for routing operations
pub struct RoutingOperationHandler;

impl RoutingOperationHandler {
    /// Create a new routing operation handler
    pub fn new() -> Self {
        Self
    }

    /// Handle subscribing a stream to a subject pattern
    async fn handle_subscribe(
        &self,
        stream_name: &str,
        subject_pattern: &str,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<RoutingOperationResponse> {
        // Validate subject pattern
        if let Err(e) = RoutingOperation::validate_subject_pattern(subject_pattern) {
            return Ok(RoutingOperationResponse::Failed {
                operation: "subscribe".to_string(),
                reason: format!("Invalid subject pattern '{subject_pattern}': {e}"),
            });
        }

        let mut router = state.subject_router.write().await;
        let mut streams = state.streams.write().await;

        // Add subscription to router
        router.subscribe_stream(stream_name, subject_pattern);

        // Update stream's subscription list
        let stream_data = streams.entry(stream_name.to_string()).or_insert_with(|| {
            crate::core::global::StreamData {
                messages: std::collections::BTreeMap::new(),
                next_sequence: 1,
                subscriptions: std::collections::HashSet::new(),
            }
        });
        stream_data
            .subscriptions
            .insert(subject_pattern.to_string());

        info!(
            "Subscribed stream '{}' to subject pattern '{}'",
            stream_name, subject_pattern
        );

        Ok(RoutingOperationResponse::Subscribed {
            sequence: context.sequence,
            stream_name: stream_name.to_string(),
            subject_pattern: subject_pattern.to_string(),
            total_subscriptions: stream_data.subscriptions.len(),
        })
    }

    /// Handle unsubscribing a stream from a subject pattern
    async fn handle_unsubscribe(
        &self,
        stream_name: &str,
        subject_pattern: &str,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<RoutingOperationResponse> {
        let mut router = state.subject_router.write().await;
        let mut streams = state.streams.write().await;

        // Check if subscription exists before removing
        let had_subscription = streams
            .get(stream_name)
            .map(|s| s.subscriptions.contains(subject_pattern))
            .unwrap_or(false);

        if !had_subscription {
            return Ok(RoutingOperationResponse::Failed {
                operation: "unsubscribe".to_string(),
                reason: format!(
                    "Stream '{stream_name}' was not subscribed to subject pattern '{subject_pattern}'"
                ),
            });
        }

        // Remove subscription from router
        router.unsubscribe_stream(stream_name, subject_pattern);

        // Update stream's subscription list
        if let Some(stream_data) = streams.get_mut(stream_name) {
            stream_data.subscriptions.remove(subject_pattern);
        }

        // Get remaining count
        let remaining_subscriptions = streams
            .get(stream_name)
            .map(|s| s.subscriptions.len())
            .unwrap_or(0);

        info!(
            "Unsubscribed stream '{}' from subject pattern '{}'",
            stream_name, subject_pattern
        );

        Ok(RoutingOperationResponse::Unsubscribed {
            sequence: context.sequence,
            stream_name: stream_name.to_string(),
            subject_pattern: subject_pattern.to_string(),
            remaining_subscriptions,
        })
    }

    /// Handle removing all subscriptions for a stream
    async fn handle_remove_all_subscriptions(
        &self,
        stream_name: &str,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<RoutingOperationResponse> {
        let mut router = state.subject_router.write().await;
        let mut streams = state.streams.write().await;

        // Get current subscriptions
        let subscriptions = if let Some(stream_data) = streams.get(stream_name) {
            stream_data.subscriptions.clone()
        } else {
            return Ok(RoutingOperationResponse::Failed {
                operation: "remove_all_subscriptions".to_string(),
                reason: format!("Stream '{stream_name}' not found"),
            });
        };

        // Remove all subscriptions from router
        for pattern in &subscriptions {
            router.unsubscribe_stream(stream_name, pattern);
        }

        // Clear stream's subscription list
        if let Some(stream_data) = streams.get_mut(stream_name) {
            stream_data.subscriptions.clear();
        }

        info!(
            "Removed all {} subscriptions for stream '{}'",
            subscriptions.len(),
            stream_name
        );

        Ok(RoutingOperationResponse::Unsubscribed {
            sequence: context.sequence,
            stream_name: stream_name.to_string(),
            subject_pattern: format!("{} patterns", subscriptions.len()),
            remaining_subscriptions: 0,
        })
    }

    /// Handle bulk subscribe operation
    async fn handle_bulk_subscribe(
        &self,
        stream_name: &str,
        subject_patterns: &[String],
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<RoutingOperationResponse> {
        // Validate all patterns first
        for pattern in subject_patterns {
            if let Err(e) = RoutingOperation::validate_subject_pattern(pattern) {
                return Ok(RoutingOperationResponse::Failed {
                    operation: "bulk_subscribe".to_string(),
                    reason: format!("Invalid subject pattern '{pattern}': {e}"),
                });
            }
        }

        let mut router = state.subject_router.write().await;
        let mut streams = state.streams.write().await;

        // Get or create stream data
        let stream_data = streams.entry(stream_name.to_string()).or_insert_with(|| {
            crate::core::global::StreamData {
                messages: std::collections::BTreeMap::new(),
                next_sequence: 1,
                subscriptions: std::collections::HashSet::new(),
            }
        });

        // Add all subscriptions
        let mut added = 0;
        for pattern in subject_patterns {
            if stream_data.subscriptions.insert(pattern.clone()) {
                router.subscribe_stream(stream_name, pattern);
                added += 1;
            }
        }

        info!(
            "Bulk subscribed stream '{}' to {} patterns",
            stream_name, added
        );

        Ok(RoutingOperationResponse::Subscribed {
            sequence: context.sequence,
            stream_name: stream_name.to_string(),
            subject_pattern: format!("{added} patterns"),
            total_subscriptions: stream_data.subscriptions.len(),
        })
    }

    /// Handle bulk unsubscribe operation
    async fn handle_bulk_unsubscribe(
        &self,
        stream_name: &str,
        subject_patterns: &[String],
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<RoutingOperationResponse> {
        let mut router = state.subject_router.write().await;
        let mut streams = state.streams.write().await;

        let stream_data = match streams.get_mut(stream_name) {
            Some(data) => data,
            None => {
                return Ok(RoutingOperationResponse::Failed {
                    operation: "bulk_unsubscribe".to_string(),
                    reason: format!("Stream '{stream_name}' not found"),
                });
            }
        };

        // Remove subscriptions
        let mut removed = 0;
        for pattern in subject_patterns {
            if stream_data.subscriptions.remove(pattern) {
                router.unsubscribe_stream(stream_name, pattern);
                removed += 1;
            }
        }

        info!(
            "Bulk unsubscribed stream '{}' from {} patterns",
            stream_name, removed
        );

        Ok(RoutingOperationResponse::Unsubscribed {
            sequence: context.sequence,
            stream_name: stream_name.to_string(),
            subject_pattern: format!("{removed} patterns"),
            remaining_subscriptions: stream_data.subscriptions.len(),
        })
    }
}

impl Default for RoutingOperationHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GlobalOperationHandler for RoutingOperationHandler {
    type Operation = RoutingOperation;
    type Response = RoutingOperationResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<RoutingOperationResponse> {
        match operation {
            RoutingOperation::Subscribe {
                stream_name,
                subject_pattern,
            } => {
                self.handle_subscribe(stream_name, subject_pattern, state, context)
                    .await
            }
            RoutingOperation::Unsubscribe {
                stream_name,
                subject_pattern,
            } => {
                self.handle_unsubscribe(stream_name, subject_pattern, state, context)
                    .await
            }
            RoutingOperation::RemoveAllSubscriptions { stream_name } => {
                self.handle_remove_all_subscriptions(stream_name, state, context)
                    .await
            }
            RoutingOperation::BulkSubscribe {
                stream_name,
                subject_patterns,
            } => {
                self.handle_bulk_subscribe(stream_name, subject_patterns, state, context)
                    .await
            }
            RoutingOperation::BulkUnsubscribe {
                stream_name,
                subject_patterns,
            } => {
                self.handle_bulk_unsubscribe(stream_name, subject_patterns, state, context)
                    .await
            }
        }
    }

    async fn post_execute(
        &self,
        operation: &Self::Operation,
        response: &Self::Response,
        _state: &GlobalState,
        _context: &OperationContext,
    ) -> ConsensusResult<()> {
        match response {
            RoutingOperationResponse::Failed {
                operation: op,
                reason,
            } => {
                debug!("Failed to execute routing operation {}: {}", op, reason);
            }
            _ => {
                debug!("Successfully executed routing operation: {:?}", operation);
            }
        }
        Ok(())
    }

    fn operation_type(&self) -> &'static str {
        "RoutingOperation"
    }
}
