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
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use tokio::sync::{Mutex, broadcast};

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
    last_seq: Arc<Mutex<u64>>,
    stream: <Self as Service<X, T, D, S>>::StreamType,
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
            last_seq: self.last_seq.clone(),
            stream: self.stream.clone(),
        }
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
    ) -> Result<Self, Self::Error> {
        // Nothing to catch up on for in-memory
        let _ = handler.on_caught_up().await;

        let last_seq = Arc::new(Mutex::new(0));

        // Subscribe to client requests
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<GlobalState<T>>() {
            state.put(GlobalState::<T>::default());
        }
        let global_state = state.borrow::<GlobalState<T>>();
        let mut client_requests = global_state.client_requests.lock().await;
        let (tx, mut rx) = broadcast::channel(100);
        client_requests.insert(name.clone(), tx);
        drop(client_requests);
        drop(state);

        let handler_clone = handler.clone();
        let stream_clone = stream.clone();
        let name_clone = name.clone();

        let last_seq_clone = last_seq.clone();
        tokio::spawn(async move {
            while let Ok(request) = rx.recv().await {
                let handler = handler_clone.clone();
                let stream = stream_clone.clone();

                let responder = MemoryServiceResponder::new(
                    name_clone.clone(),
                    request.client_id,
                    request.request_id,
                    stream.clone(),
                    request.sequence_number,
                );

                let _ = handler.handle(request.payload, responder).await;

                let mut seq = last_seq_clone.lock().await;
                if request.sequence_number > *seq {
                    *seq = request.sequence_number;
                }
                drop(seq);
            }
        });

        Ok(Self { last_seq, stream })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
