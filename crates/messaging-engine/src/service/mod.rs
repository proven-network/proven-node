//! Services are special consumers that respond to requests in the engine messaging system.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, warn};

use proven_messaging::service::{Service, ServiceError, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;

use crate::error::MessagingEngineError;
use crate::service_responder::EngineMessagingServiceResponder;
use crate::service_responder::EngineMessagingUsedResponder;
use crate::stream::InitializedEngineStream;

/// Options for engine messaging services.
#[derive(Clone, Debug, Copy, Default)]
pub struct EngineMessagingServiceOptions {
    /// Starting sequence number.
    pub start_sequence: Option<u64>,
}

impl ServiceOptions for EngineMessagingServiceOptions {}

/// Error type for engine messaging services.
#[derive(Debug, thiserror::Error)]
pub enum EngineMessagingServiceError {
    /// Engine error.
    #[error("Engine error: {0}")]
    Engine(#[from] MessagingEngineError),
}

impl ServiceError for EngineMessagingServiceError {}

/// An engine messaging service.
#[derive(Debug)]
#[allow(dead_code)]
pub struct EngineMessagingService<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    name: String,
    stream: InitializedEngineStream<T, D, S>,
    options: EngineMessagingServiceOptions,
    handler: X,
    last_processed_seq: u64,
    /// Current sequence number being processed.
    current_seq: Arc<Mutex<u64>>,
    /// Shutdown token for graceful termination.
    shutdown_token: CancellationToken,
    /// Task tracker for background processing.
    task_tracker: TaskTracker,
}

impl<X, T, D, S> Clone for EngineMessagingService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            stream: self.stream.clone(),
            options: self.options,
            handler: self.handler.clone(),
            last_processed_seq: self.last_processed_seq,
            current_seq: self.current_seq.clone(),
            shutdown_token: self.shutdown_token.clone(),
            task_tracker: self.task_tracker.clone(),
        }
    }
}

#[async_trait]
impl<X, T, D, S> Service<X, T, D, S> for EngineMessagingService<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error = EngineMessagingServiceError;
    type Options = EngineMessagingServiceOptions;
    type Responder = EngineMessagingServiceResponder<
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >;
    type UsedResponder = EngineMessagingUsedResponder;
    type StreamType = InitializedEngineStream<T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            name,
            stream,
            options,
            handler,
            last_processed_seq: options.start_sequence.unwrap_or(0),
            current_seq: Arc::new(Mutex::new(0)),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        Ok(*self.current_seq.lock().await)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}

impl<X, T, D, S> EngineMessagingService<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    /// Process requests from the engine stream.
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::too_many_lines)]
    async fn process_requests(
        stream: InitializedEngineStream<T, D, S>,
        handler: X,
        current_seq: Arc<Mutex<u64>>,
        start_sequence: u64,
        shutdown_token: CancellationToken,
        service_name: String,
    ) -> Result<(), EngineMessagingServiceError> {
        let mut last_checked_seq = start_sequence;
        let initial_stream_msgs =
            match tokio::time::timeout(tokio::time::Duration::from_millis(100), stream.messages())
                .await
            {
                Ok(Ok(msgs)) => msgs,
                Ok(Err(e)) => {
                    warn!("Service failed to get initial stream messages: {:?}", e);
                    0
                }
                Err(_) => {
                    warn!("Service initial stream messages query timed out");
                    0
                }
            };
        let mut caught_up = initial_stream_msgs == 0 || start_sequence >= initial_stream_msgs;
        let mut requests_processed = 0;

        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    break;
                }
                () = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Check for new messages
                    let current_stream_msgs = match tokio::time::timeout(
                        tokio::time::Duration::from_millis(50),
                        stream.messages()
                    ).await {
                        Ok(Ok(msgs)) => msgs,
                        Ok(Err(e)) => {
                            warn!("Service failed to get stream messages: {:?}", e);
                            continue;
                        }
                        Err(_) => {
                            warn!("Service stream messages query timed out");
                            continue;
                        }
                    };

                    // Process any new messages
                    while last_checked_seq < current_stream_msgs {
                        last_checked_seq += 1;

                        // Read message
                        let msg = match tokio::time::timeout(
                            tokio::time::Duration::from_millis(50),
                            stream.get(last_checked_seq)
                        ).await {
                            Ok(Ok(Some(msg))) => msg,
                            Ok(Ok(None)) => {
                                debug!("Request {} not found in stream, skipping", last_checked_seq);
                                continue;
                            }
                            Ok(Err(e)) => {
                                warn!("Service failed to get message {}: {:?}", last_checked_seq, e);
                                continue;
                            }
                            Err(_) => {
                                warn!("Service get message {} timed out", last_checked_seq);
                                continue;
                            }
                        };

                        // Extract metadata
                        let metadata: HashMap<String, String> = HashMap::new(); // TODO: Get metadata from message when available

                        // Create responder
                        let request_id = metadata.get("request_id")
                            .cloned()
                            .unwrap_or_else(|| format!("seq_{last_checked_seq}"));
                        let response_stream = metadata.get("response_stream")
                            .cloned()
                            .unwrap_or_else(|| format!("{service_name}_responses"));

                        let responder = EngineMessagingServiceResponder::new(
                            &stream.clone(),
                            response_stream,
                            request_id,
                        );

                        // Handle the request
                        if let Err(e) = handler.handle(msg, responder).await {
                            warn!(
                                "Service handler error at sequence {}: {:?}",
                                last_checked_seq, e
                            );
                        }

                        requests_processed += 1;

                        // Update current sequence
                        let _ = tokio::time::timeout(
                            tokio::time::Duration::from_millis(10),
                            async {
                                let mut seq = current_seq.lock().await;
                                *seq = last_checked_seq;
                            }
                        ).await;

                        // Check if we've caught up
                        if !caught_up && last_checked_seq >= current_stream_msgs {
                            caught_up = true;
                            debug!(
                                "Service caught up after processing {} requests",
                                requests_processed
                            );
                        }
                    }
                }
            }
        }

        debug!(
            "Service shutting down after processing {} requests",
            requests_processed
        );
        Ok(())
    }
}

#[async_trait]
impl<X, T, D, S> Bootable for EngineMessagingService<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn bootable_name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Spawn the request processing task
        let stream = self.stream.clone();
        let handler = self.handler.clone();
        let current_seq = self.current_seq.clone();
        let start_sequence = self.last_processed_seq;
        let shutdown_token = self.shutdown_token.clone();
        let service_name = self.name.clone();

        self.task_tracker.spawn(async move {
            if let Err(e) = Self::process_requests(
                stream,
                handler,
                current_seq,
                start_sequence,
                shutdown_token,
                service_name.clone(),
            )
            .await
            {
                warn!(
                    "Service '{}' request processing error: {:?}",
                    service_name, e
                );
            }
        });

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Cancel the processing task
        self.shutdown_token.cancel();

        // Close the task tracker
        self.task_tracker.close();

        Ok(())
    }

    async fn wait(&self) -> () {
        self.task_tracker.wait().await;
    }
}

// TODO: Add tests for engine-based messaging service
