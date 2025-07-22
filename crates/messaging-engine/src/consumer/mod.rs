//! Consumers are stateful views of engine streams.

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

use proven_messaging::consumer::{Consumer, ConsumerError, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::stream::InitializedStream;

use crate::error::MessagingEngineError;
use crate::stream::InitializedEngineStream;

/// Options for engine messaging consumers.
#[derive(Clone, Debug, Copy, Default)]
pub struct EngineMessagingConsumerOptions {
    /// Starting sequence number.
    pub start_sequence: Option<u64>,
}

impl ConsumerOptions for EngineMessagingConsumerOptions {}

/// Error type for engine messaging consumers.
#[derive(Debug, thiserror::Error)]
pub enum EngineMessagingConsumerError {
    /// Engine error.
    #[error("Engine error: {0}")]
    Engine(#[from] MessagingEngineError),
}

impl ConsumerError for EngineMessagingConsumerError {}

/// An engine messaging consumer.
#[allow(dead_code)]
pub struct EngineMessagingConsumer<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ConsumerHandler<T, D, S>,
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
    stream: InitializedEngineStream<Tr, G, St, T, D, S>,
    options: EngineMessagingConsumerOptions,
    handler: X,
    last_processed_seq: u64,
    /// Current sequence number being processed.
    current_seq: Arc<Mutex<u64>>,
    /// Shutdown token for graceful termination.
    shutdown_token: CancellationToken,
    /// Task tracker for background processing.
    task_tracker: TaskTracker,
}

impl<Tr, G, St, X, T, D, S> Debug for EngineMessagingConsumer<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ConsumerHandler<T, D, S>,
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineMessagingConsumer")
            .field("name", &self.name)
            .field("stream", &self.stream)
            .field("options", &self.options)
            .field("last_processed_seq", &self.last_processed_seq)
            .field("current_seq", &self.current_seq)
            .finish_non_exhaustive()
    }
}

impl<Tr, G, St, X, T, D, S> Clone for EngineMessagingConsumer<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ConsumerHandler<T, D, S> + Clone,
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
impl<Tr, G, St, X, T, D, S> Consumer<X, T, D, S> for EngineMessagingConsumer<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ConsumerHandler<T, D, S>,
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
    type Error = EngineMessagingConsumerError;
    type Options = EngineMessagingConsumerOptions;
    type StreamType = InitializedEngineStream<Tr, G, St, T, D, S>;

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
}

impl<Tr, G, St, X, T, D, S> EngineMessagingConsumer<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ConsumerHandler<T, D, S>,
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
    /// Process messages from the engine stream.
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::too_many_lines)]
    async fn process_messages(
        stream: InitializedEngineStream<Tr, G, St, T, D, S>,
        handler: X,
        current_seq: Arc<Mutex<u64>>,
        start_sequence: u64,
        shutdown_token: CancellationToken,
    ) -> Result<(), EngineMessagingConsumerError> {
        let mut last_checked_seq = start_sequence;
        let initial_stream_msgs = match tokio::time::timeout(
            tokio::time::Duration::from_millis(100), // Slightly longer timeout for initial call
            stream.messages(),
        )
        .await
        {
            Ok(Ok(msgs)) => msgs,
            Ok(Err(e)) => {
                warn!("Consumer failed to get initial stream messages: {:?}", e);
                0 // Default to 0 messages if we can't get count
            }
            Err(_) => {
                warn!("Consumer initial stream messages query timed out");
                0 // Default to 0 messages if timeout
            }
        };
        let mut caught_up = initial_stream_msgs == 0 || start_sequence >= initial_stream_msgs;
        let mut msgs_processed = 0;

        if caught_up {
            let _ = handler.on_caught_up().await;
        }

        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    break;
                }
                () = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Check for new messages with timeout to avoid hanging on engine calls
                    let current_stream_msgs = match tokio::time::timeout(
                        tokio::time::Duration::from_millis(50),
                        stream.messages()
                    ).await {
                        Ok(Ok(msgs)) => msgs,
                        Ok(Err(e)) => {
                            warn!("Consumer failed to get stream messages: {:?}", e);
                            continue;
                        }
                        Err(_) => {
                            warn!("Consumer stream messages query timed out");
                            continue;
                        }
                    };

                    // Process any new messages
                    while last_checked_seq < current_stream_msgs {
                        last_checked_seq += 1;

                        // Read message with timeout
                        let msg = match tokio::time::timeout(
                            tokio::time::Duration::from_millis(50),
                            stream.get(last_checked_seq)
                        ).await {
                            Ok(Ok(Some(msg))) => msg,
                            Ok(Ok(None)) => {
                                debug!("Message {} not found in stream, skipping", last_checked_seq);
                                continue;
                            }
                            Ok(Err(e)) => {
                                warn!("Consumer failed to get message {}: {:?}", last_checked_seq, e);
                                continue;
                            }
                            Err(_) => {
                                warn!("Consumer get message {} timed out", last_checked_seq);
                                continue;
                            }
                        };

                        // Process the message
                        if let Err(e) = handler.handle(msg, last_checked_seq).await {
                            warn!(
                                "Consumer handler error at sequence {}: {:?}",
                                last_checked_seq, e
                            );
                        }

                        msgs_processed += 1;

                        // Update current sequence with timeout
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
                                "Consumer caught up after processing {} messages",
                                msgs_processed
                            );
                            let _ = handler.on_caught_up().await;
                        }
                    }
                }
            }
        }

        debug!(
            "Consumer shutting down after processing {} messages",
            msgs_processed
        );
        Ok(())
    }
}

#[async_trait]
impl<Tr, G, St, X, T, D, S> Bootable for EngineMessagingConsumer<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: ConsumerHandler<T, D, S>,
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
        // Spawn the message processing task
        let stream = self.stream.clone();
        let handler = self.handler.clone();
        let current_seq = self.current_seq.clone();
        let start_sequence = self.last_processed_seq;
        let shutdown_token = self.shutdown_token.clone();
        let consumer_name = self.name.clone();

        self.task_tracker.spawn(async move {
            if let Err(e) =
                Self::process_messages(stream, handler, current_seq, start_sequence, shutdown_token)
                    .await
            {
                warn!(
                    "Consumer '{}' message processing error: {:?}",
                    consumer_name, e
                );
            }
        });

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Cancel the processing task
        self.shutdown_token.cancel();

        // Close the task tracker to signal no more tasks will be spawned
        self.task_tracker.close();

        Ok(())
    }

    async fn wait(&self) -> () {
        self.task_tracker.wait().await;
    }
}
