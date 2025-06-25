mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};

use crate::stream::InitializedMemoryStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryConsumerOptions;
impl ConsumerOptions for MemoryConsumerOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    handler: X,
    last_seq: Arc<Mutex<u64>>,
    shutdown_token: CancellationToken,
    stream: InitializedMemoryStream<T, D, S>,
    task_tracker: TaskTracker,
    _marker: PhantomData<(T, S, D)>,
}

impl<X, T, D, S> Clone for MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
            handler: self.handler.clone(),
            last_seq: self.last_seq.clone(),
            shutdown_token: self.shutdown_token.clone(),
            stream: self.stream.clone(),
            task_tracker: self.task_tracker.clone(),
            _marker: PhantomData,
        }
    }
}

impl<X, T, D, S> MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    /// Creates a new NATS consumer.
    async fn process_messages(
        last_seq: Arc<Mutex<u64>>,
        mut receiver_stream: ReceiverStream<T>,
        handler: X,
        shutdown_token: CancellationToken,
    ) -> Result<(), Error> {
        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    debug!("shutdown token cancelled, exiting message processing loop");
                    break;
                }
                maybe_message = receiver_stream.next() => {
                    if let Some(message) = maybe_message {
                        let mut seq = last_seq.lock().await;
                        *seq += 1;
                        let current_seq = *seq;
                        drop(seq); // Release lock before calling handler

                        if let Err(e) = handler.handle(message, current_seq).await {
                            error!("error handling message: {}", e);
                            return Err(Error::Handler);
                        }
                    } else {
                        // Stream closed
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<X, T, D, S> Consumer<X, T, D, S> for MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    type Error = Error;

    type Options = MemoryConsumerOptions;

    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn new(
        _name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Error> {
        // Call immediately as in-memory never has anythong to catch up on.
        let _ = handler.on_caught_up().await;

        let last_seq = Arc::new(Mutex::new(0));

        Ok(Self {
            handler,
            last_seq,
            shutdown_token: CancellationToken::new(),
            stream,
            task_tracker: TaskTracker::new(),
            _marker: PhantomData,
        })
    }

    async fn last_seq(&self) -> Result<u64, Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }
}

#[async_trait]
impl<X, T, D, S> Bootable for MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    type Error = Error;

    async fn start(&self) -> Result<(), Self::Error> {
        if self.task_tracker.is_closed() {
            return Err(Error::AlreadyRunning);
        }

        self.task_tracker.spawn(Self::process_messages(
            self.last_seq.clone(),
            self.stream.message_stream().await,
            self.handler.clone(),
            self.shutdown_token.clone(),
        ));

        self.task_tracker.close();

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        debug!("shutting down in-memory consumer");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}
