#![allow(clippy::or_fun_call)]

mod error;

use crate::service_responder::{MemoryServiceResponder, MemoryUsedServiceResponder};
use crate::stream::InitializedMemoryStream;
use crate::{GLOBAL_STATE, GlobalState};
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error};

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryServiceOptions;
impl ServiceOptions for MemoryServiceOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct MemoryService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    name: String,
    rx: Arc<Mutex<broadcast::Receiver<crate::ClientRequest<T>>>>,
    shutdown_token: CancellationToken,
    stream: <Self as Service<X, T, D, S>>::StreamType,
    task_tracker: TaskTracker,
}

impl<X, T, D, S> Clone for MemoryService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
            name: self.name.clone(),
            rx: self.rx.clone(),
            shutdown_token: self.shutdown_token.clone(),
            stream: self.stream.clone(),
            task_tracker: self.task_tracker.clone(),
        }
    }
}

impl<X, T, D, S> MemoryService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    async fn process_messages(
        last_seq: Arc<Mutex<u64>>,
        mut rx: broadcast::Receiver<crate::ClientRequest<T>>,
        handler: X,
        name: String,
        stream: InitializedMemoryStream<T, D, S>,
        shutdown_token: CancellationToken,
    ) -> Result<(), Error> {
        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    debug!("shutdown token cancelled, exiting service message processing loop");
                    break;
                }
                recv_result = rx.recv() => {
                    match recv_result {
                        Ok(request) => {
                            let handler = handler.clone();
                            let stream = stream.clone();

                            let responder = MemoryServiceResponder::new(
                                name.clone(),
                                request.client_id,
                                request.request_id,
                                stream.clone(),
                                request.sequence_number,
                            );

                            if let Err(e) = handler.handle(request.payload, responder).await {
                                error!("error handling service request: {}", e);
                            }

                            let mut seq = last_seq.lock().await;
                            if request.sequence_number > *seq {
                                *seq = request.sequence_number;
                            }
                            drop(seq);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            debug!("Broadcast channel closed, exiting service loop.");
                            break;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            error!("Service request receiver lagged by {} messages.", n);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<X, T, D, S> Service<X, T, D, S> for MemoryService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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

    type Options = MemoryServiceOptions;

    type StreamType = InitializedMemoryStream<T, D, S>;

    type Responder = MemoryServiceResponder<
        T,
        D,
        S,
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >;

    type UsedResponder = MemoryUsedServiceResponder;

    async fn new(
        name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Error> {
        // Call immediately as in-memory never has anythong to catch up on.
        let _ = handler.on_caught_up().await;

        let last_seq = Arc::new(Mutex::new(0));

        // Subscribe to client requests
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<GlobalState<T>>() {
            state.put(GlobalState::<T>::default());
        }
        let global_state = state.borrow::<GlobalState<T>>();
        let mut client_requests = global_state.client_requests.lock().await;
        let (tx, rx) = broadcast::channel(100);
        client_requests.insert(name.clone(), tx);
        drop(client_requests);
        drop(state);

        Ok(Self {
            handler,
            last_seq,
            name,
            rx: Arc::new(Mutex::new(rx)),
            shutdown_token: CancellationToken::new(),
            stream,
            task_tracker: TaskTracker::new(),
        })
    }

    async fn last_seq(&self) -> Result<u64, Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}

#[async_trait]
impl<X, T, D, S> Bootable for MemoryService<X, T, D, S>
where
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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

        let rx = self.rx.lock().await.resubscribe();

        self.task_tracker.spawn(Self::process_messages(
            self.last_seq.clone(),
            rx,
            self.handler.clone(),
            self.name.clone(),
            self.stream.clone(),
            self.shutdown_token.clone(),
        ));

        self.task_tracker.close();

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        debug!("shutting down in-memory service");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}
