//! Stream streaming handler for efficient stream reads
//!
//! This handler manages streaming sessions that allow clients to read
//! large streams efficiently in batches.

use std::collections::HashMap;
use std::num::NonZero;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use proven_logger::{debug, error, warn};
use proven_storage::{LogStorageStreaming, StorageAdaptor};
use tokio::sync::RwLock;
use tokio_stream::{Stream, StreamExt};
use uuid::Uuid;

use crate::{
    error::{ConsensusResult, Error, ErrorKind},
    services::stream::StoredMessage,
};

use super::types::StreamServiceRef;

/// Active streaming session
struct StreamSession {
    /// Session ID
    id: Uuid,
    /// Stream name
    stream_name: String,
    /// The actual stream - wrapped in Mutex for thread safety
    stream: tokio::sync::Mutex<Pin<Box<dyn Stream<Item = ConsensusResult<StoredMessage>> + Send>>>,
    /// Last activity time
    last_activity: tokio::sync::Mutex<Instant>,
    /// Original requester
    requester_id: proven_topology::NodeId,
}

/// Manages streaming sessions for efficient stream reads
pub struct StreamStreamingHandler<S>
where
    S: StorageAdaptor + LogStorageStreaming,
{
    /// Stream service for creating streams
    stream_service: StreamServiceRef<S>,
    /// Active streaming sessions
    sessions: Arc<RwLock<HashMap<Uuid, StreamSession>>>,
    /// Session timeout duration
    session_timeout: Duration,
}

impl<S> StreamStreamingHandler<S>
where
    S: StorageAdaptor + LogStorageStreaming + 'static,
{
    /// Create a new streaming handler
    pub fn new(stream_service: StreamServiceRef<S>) -> Self {
        Self {
            stream_service,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            session_timeout: Duration::from_secs(300), // 5 minutes
        }
    }

    /// Start a new streaming session
    pub async fn start_stream(
        &self,
        requester_id: proven_topology::NodeId,
        stream_name: String,
        start_sequence: NonZero<u64>,
        end_sequence: Option<NonZero<u64>>,
        batch_size: NonZero<u64>,
    ) -> ConsensusResult<(Uuid, Vec<StoredMessage>, bool, Option<NonZero<u64>>)> {
        // Get stream service
        let stream_guard = self.stream_service.read().await;
        let stream_service = stream_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Stream service not available")
        })?;

        // Create the stream
        let mut message_stream = stream_service
            .stream_messages(&stream_name, start_sequence, end_sequence)
            .await?;

        // Generate session ID
        let session_id = Uuid::new_v4();

        // Collect first batch
        let mut messages = Vec::new();
        let mut next_sequence = None;

        for _ in 0..batch_size.get() {
            match message_stream.next().await {
                Some(Ok(msg)) => {
                    next_sequence = Some(msg.sequence.saturating_add(1));
                    messages.push(msg);
                }
                Some(Err(e)) => {
                    error!("Error reading from stream: {e}");
                    if messages.is_empty() {
                        return Err(e);
                    }
                    break;
                }
                None => break,
            }
        }

        // Check if there are more messages
        let has_more = if let Some(Ok(_)) = message_stream.next().await {
            // We peeked one message, so we need to create a new stream that includes it
            let new_stream = stream_service
                .stream_messages(
                    &stream_name,
                    next_sequence.unwrap_or(start_sequence),
                    end_sequence,
                )
                .await?;

            // Store the session
            let session = StreamSession {
                id: session_id,
                stream_name: stream_name.clone(),
                stream: tokio::sync::Mutex::new(new_stream),
                last_activity: tokio::sync::Mutex::new(Instant::now()),
                requester_id,
            };

            self.sessions.write().await.insert(session_id, session);
            true
        } else {
            false
        };

        debug!(
            "Started streaming session {} for stream {}: {} messages, has_more: {}",
            session_id,
            stream_name,
            messages.len(),
            has_more
        );

        Ok((session_id, messages, has_more, next_sequence))
    }

    /// Continue an existing streaming session
    pub async fn continue_stream(
        &self,
        session_id: Uuid,
        max_messages: u32,
    ) -> ConsensusResult<(Vec<StoredMessage>, bool, Option<NonZero<u64>>)> {
        let sessions = self.sessions.read().await;

        let session = sessions.get(&session_id).ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("Session {session_id} not found"),
            )
        })?;

        // Update last activity
        *session.last_activity.lock().await = Instant::now();

        // Collect batch
        let mut messages = Vec::new();
        let mut next_sequence = None;

        let mut stream = session.stream.lock().await;
        for _ in 0..max_messages {
            match stream.next().await {
                Some(Ok(msg)) => {
                    next_sequence = Some(msg.sequence.saturating_add(1));
                    messages.push(msg);
                }
                Some(Err(e)) => {
                    error!("Error continuing stream: {e}");
                    if messages.is_empty() {
                        drop(stream);
                        drop(sessions);
                        // Remove failed session
                        self.sessions.write().await.remove(&session_id);
                        return Err(e);
                    }
                    break;
                }
                None => break,
            }
        }

        // Check if stream has more
        // Note: We can't peek without consuming, so we assume has_more based on
        // whether we got the full batch
        let has_more = messages.len() == max_messages as usize;

        drop(stream);
        drop(sessions);

        // If no more messages, remove the session
        if messages.is_empty() || !has_more {
            self.sessions.write().await.remove(&session_id);
        }

        debug!(
            "Continued session {}: {} messages, has_more: {}",
            session_id,
            messages.len(),
            has_more
        );

        Ok((messages, has_more, next_sequence))
    }

    /// Cancel a streaming session
    pub async fn cancel_stream(&self, session_id: Uuid) -> ConsensusResult<()> {
        let removed = self.sessions.write().await.remove(&session_id).is_some();

        if removed {
            debug!("Cancelled streaming session {session_id}");
            Ok(())
        } else {
            Err(Error::with_context(
                ErrorKind::NotFound,
                format!("Session {session_id} not found"),
            ))
        }
    }

    /// Clean up expired sessions
    pub async fn cleanup_expired_sessions(&self) {
        let now = Instant::now();
        let sessions = self.sessions.read().await;

        let mut expired = Vec::new();
        for (id, session) in sessions.iter() {
            let last_activity = *session.last_activity.lock().await;
            if now.duration_since(last_activity) > self.session_timeout {
                expired.push(*id);
            }
        }
        drop(sessions);

        if !expired.is_empty() {
            let mut sessions = self.sessions.write().await;
            for id in expired {
                warn!("Removing expired streaming session {id}");
                sessions.remove(&id);
            }
        }
    }

    /// Get the number of active sessions
    pub async fn active_session_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

impl<S> Clone for StreamStreamingHandler<S>
where
    S: StorageAdaptor + LogStorageStreaming,
{
    fn clone(&self) -> Self {
        Self {
            stream_service: self.stream_service.clone(),
            sessions: self.sessions.clone(),
            session_timeout: self.session_timeout,
        }
    }
}
