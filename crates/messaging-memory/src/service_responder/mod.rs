mod error;

use crate::stream::InitializedMemoryStream;
use crate::{GlobalState, ServiceResponse, GLOBAL_STATE};
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use proven_messaging::service_responder::{ServiceResponder, UsedServiceResponder};
use proven_messaging::stream::InitializedStream;

/// A used responder for a NATS service.
#[derive(Debug)]
pub struct MemoryUsedServiceResponder;
impl UsedServiceResponder for MemoryUsedServiceResponder {}

/// A responder for a NATS service.
#[derive(Clone, Debug)]
pub struct MemoryServiceResponder<T, TD, TS, R, RD, RS>
where
    Self: Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = TD>
        + TryInto<Bytes, Error = TS>
        + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    TD: Debug + Send + StdError + Sync + 'static,
    TS: Debug + Send + StdError + Sync + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    client_id: String,
    request_id: String,
    service_name: String,
    stream: InitializedMemoryStream<T, TD, TS>,
    stream_sequence: u64,
    _marker: PhantomData<R>,
}

impl<T, TD, TS, R, RD, RS> MemoryServiceResponder<T, TD, TS, R, RD, RS>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = TD>
        + TryInto<Bytes, Error = TS>
        + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    TD: Debug + Send + StdError + Sync + 'static,
    TS: Debug + Send + StdError + Sync + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    /// Creates a new NATS service responder.
    #[must_use]
    pub const fn new(
        service_name: String,
        client_id: String,
        request_id: String,
        stream: InitializedMemoryStream<T, TD, TS>,
        stream_sequence: u64,
    ) -> Self {
        Self {
            client_id,
            request_id,
            service_name,
            stream,
            stream_sequence,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, TD, TS, R, RD, RS> ServiceResponder<T, TD, TS, R, RD, RS>
    for MemoryServiceResponder<T, TD, TS, R, RD, RS>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = TD>
        + TryInto<Bytes, Error = TS>
        + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    TD: Debug + Send + StdError + Sync + 'static,
    TS: Debug + Send + StdError + Sync + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    type Error = Error;

    type UsedResponder = MemoryUsedServiceResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        MemoryUsedServiceResponder
    }

    async fn reply(self, response: R) -> Self::UsedResponder {
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<GlobalState<R>>() {
            state.put(GlobalState::<R>::default());
        }
        let global_state = state.borrow::<GlobalState<R>>();
        let service_responses = global_state.service_responses.lock().await;

        // Format key as "{service_name}:{client_id}"
        let response_key = format!("{}:{}", self.service_name, self.client_id);
        if let Some(sender) = service_responses.get(&response_key) {
            let response = ServiceResponse::Single(self.request_id, response);
            let _ = sender.send(response);
        }

        drop(service_responses);
        drop(state);

        MemoryUsedServiceResponder
    }

    async fn reply_and_delete_request(self, response: R) -> MemoryUsedServiceResponder {
        let stream_sequence = self.stream_sequence;
        let stream = self.stream.clone();

        let used_responder = Self::reply(self, response).await;

        stream.delete(stream_sequence).await.unwrap();

        used_responder
    }

    async fn stream<W>(self, response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let peekable = response_stream.peekable();

        tokio::pin!(peekable);

        if peekable.as_mut().peek().await.is_none() {
            let service_response = ServiceResponse::<R>::StreamEmpty(self.request_id.clone());

            let mut state = GLOBAL_STATE.lock().await;
            if !state.has::<GlobalState<R>>() {
                state.put(GlobalState::<R>::default());
            }
            let global_state = state.borrow::<GlobalState<R>>();
            let service_responses = global_state.service_responses.lock().await;

            let response_key = format!("{}:{}", self.service_name, self.client_id);
            if let Some(sender) = service_responses.get(&response_key) {
                let _ = sender.send(service_response);
            }

            drop(service_responses);
            drop(state);

            return MemoryUsedServiceResponder;
        }

        while let Some(response) = peekable.as_mut().next().await {
            let service_response = if peekable.as_mut().peek().await.is_none() {
                ServiceResponse::StreamEnd(self.request_id.clone(), response)
            } else {
                ServiceResponse::StreamItem(self.request_id.clone(), response)
            };

            let mut state = GLOBAL_STATE.lock().await;
            if !state.has::<GlobalState<R>>() {
                state.put(GlobalState::<R>::default());
            }
            let global_state = state.borrow::<GlobalState<R>>();
            let service_responses = global_state.service_responses.lock().await;

            let response_key = format!("{}:{}", self.service_name, self.client_id);
            if let Some(sender) = service_responses.get(&response_key) {
                let _ = sender.send(service_response);
            }

            drop(service_responses);
            drop(state);
        }

        MemoryUsedServiceResponder
    }

    async fn stream_and_delete_request<W>(self, response_stream: W) -> MemoryUsedServiceResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let stream_sequence = self.stream_sequence;
        let stream = self.stream.clone();

        let used_responder = Self::stream(self, response_stream).await;

        stream.delete(stream_sequence).await.unwrap();

        used_responder
    }

    fn stream_sequence(&self) -> u64 {
        self.stream_sequence
    }
}
