mod error;

use crate::stream::InitializedNatsStream;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;

use async_nats::Client as NatsClient;
use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use proven_messaging::service_responder::{ServiceResponder, UsedServiceResponder};
use proven_messaging::stream::InitializedStream;

/// A used responder for a NATS service.
#[derive(Debug)]
pub struct NatsUsedServiceResponder;
impl UsedServiceResponder for NatsUsedServiceResponder {}

/// A responder for a NATS service.
#[derive(Clone, Debug)]
pub struct NatsServiceResponder<T, TD, TS, R, RD, RS>
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
    nats_client: NatsClient,
    reply_stream_name: String,
    request_id: String,
    stream: InitializedNatsStream<T, TD, TS>,
    stream_sequence: u64,
    _marker: PhantomData<R>,
}

impl<T, TD, TS, R, RD, RS> NatsServiceResponder<T, TD, TS, R, RD, RS>
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
        nats_client: NatsClient,
        reply_stream_name: String,
        request_id: String,
        stream: InitializedNatsStream<T, TD, TS>,
        stream_sequence: u64,
    ) -> Self {
        Self {
            nats_client,
            reply_stream_name,
            request_id,
            stream,
            stream_sequence,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, TD, TS, R, RD, RS> ServiceResponder<T, TD, TS, R, RD, RS>
    for NatsServiceResponder<T, TD, TS, R, RD, RS>
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

    type UsedResponder = NatsUsedServiceResponder;

    async fn reply(self, response: R) -> Self::UsedResponder {
        let result_bytes: Bytes = response.try_into().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("request-id", self.request_id.clone());

        self.nats_client
            .publish_with_headers(self.reply_stream_name, headers, result_bytes)
            .await
            .unwrap();

        NatsUsedServiceResponder
    }

    async fn reply_and_delete_request(self, response: R) -> Self::UsedResponder {
        let result_bytes: Bytes = response.try_into().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("request-id", self.request_id.clone());

        self.nats_client
            .publish_with_headers(self.reply_stream_name, headers, result_bytes)
            .await
            .unwrap();

        self.stream.del(self.stream_sequence).await.unwrap();

        NatsUsedServiceResponder
    }

    async fn stream<W>(self, stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let mut peekable_stream = stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);

        let mut stream_id = 0;

        loop {
            match (
                pinned_stream.as_mut().next().await,
                pinned_stream.as_mut().peek().await,
            ) {
                (Some(payload), Some(_)) => {
                    stream_id += 1;

                    let mut next_headers = HeaderMap::new();
                    next_headers.insert("request-id", self.request_id.clone());
                    next_headers.insert("stream-id", stream_id.to_string());

                    let bytes: Bytes = payload.try_into().unwrap();

                    self.nats_client
                        .publish_with_headers(self.reply_stream_name.clone(), next_headers, bytes)
                        .await
                        .unwrap();
                }
                (Some(payload), None) => {
                    stream_id += 1;

                    let mut end_headers = HeaderMap::new();
                    end_headers.insert("request-id", self.request_id);
                    end_headers.insert("stream-id", stream_id.to_string());
                    end_headers.insert("stream-end", stream_id.to_string());

                    let bytes: Bytes = payload.try_into().unwrap();

                    self.nats_client
                        .publish_with_headers(self.reply_stream_name, end_headers, bytes)
                        .await
                        .unwrap();

                    break;
                }
                (None, Some(_) | None) => unreachable!(),
            }
        }

        NatsUsedServiceResponder
    }

    async fn stream_and_delete_request<W>(self, stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let mut peekable_stream = stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);

        let mut stream_id = 0;

        loop {
            match (
                pinned_stream.as_mut().next().await,
                pinned_stream.as_mut().peek().await,
            ) {
                (Some(payload), Some(_)) => {
                    stream_id += 1;

                    let mut next_headers = HeaderMap::new();
                    next_headers.insert("request-id", self.request_id.clone());
                    next_headers.insert("stream-id", stream_id.to_string());

                    let bytes: Bytes = payload.try_into().unwrap();

                    self.nats_client
                        .publish_with_headers(self.reply_stream_name.clone(), next_headers, bytes)
                        .await
                        .unwrap();
                }
                (Some(payload), None) => {
                    stream_id += 1;

                    let mut end_headers = HeaderMap::new();
                    end_headers.insert("request-id", self.request_id);
                    end_headers.insert("stream-id", stream_id.to_string());
                    end_headers.insert("stream-end", stream_id.to_string());

                    let bytes: Bytes = payload.try_into().unwrap();

                    self.nats_client
                        .publish_with_headers(self.reply_stream_name, end_headers, bytes)
                        .await
                        .unwrap();

                    break;
                }
                (None, Some(_) | None) => unreachable!(),
            }
        }

        self.stream.del(self.stream_sequence).await.unwrap();

        NatsUsedServiceResponder
    }
}
