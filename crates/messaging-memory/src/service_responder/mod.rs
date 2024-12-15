mod error;

use crate::stream::InitializedMemoryStream;
use crate::subject::MemorySubject;
pub use error::Error;
use proven_messaging::subject::PublishableSubject;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use proven_messaging::service_responder::{ServiceResponder, UsedServiceResponder};
use proven_messaging::stream::InitializedStream;
use uuid::Uuid;

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
    reply_subject: MemorySubject<R, RD, RS>,
    request_id: String,
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
        reply_subject: MemorySubject<R, RD, RS>,
        request_id: String,
        stream: InitializedMemoryStream<T, TD, TS>,
        stream_sequence: u64,
    ) -> Self {
        Self {
            reply_subject,
            request_id,
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

    async fn reply(self, response: R) -> MemoryUsedServiceResponder {
        self.reply_subject.publish(response).await.unwrap();

        MemoryUsedServiceResponder
    }

    async fn reply_and_delete_request(self, response: R) -> MemoryUsedServiceResponder {
        self.reply_subject.publish(response).await.unwrap();

        self.stream.del(self.stream_sequence).await.unwrap();

        MemoryUsedServiceResponder
    }

    async fn stream<W>(self, stream: W) -> MemoryUsedServiceResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let mut peekable_stream = stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);

        let _stream_uuid = Uuid::new_v4();

        match (pinned_stream.next().await, pinned_stream.peek().await) {
            // TODO: Need to deliminate the stream
            (Some(payload), Some(_)) => {
                self.reply_subject.publish(payload).await.unwrap();
            }
            (Some(payload), None) => {
                self.reply_subject.publish(payload).await.unwrap();
            }
            (None, Some(_) | None) => unreachable!(),
        }

        MemoryUsedServiceResponder
    }

    async fn stream_and_delete_request<W>(self, stream: W) -> MemoryUsedServiceResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let mut peekable_stream = stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);

        match (pinned_stream.next().await, pinned_stream.peek().await) {
            // TODO: Need to deliminate the stream
            (Some(payload), Some(_)) => {
                self.reply_subject.publish(payload).await.unwrap();
            }
            (Some(payload), None) => {
                self.reply_subject.publish(payload).await.unwrap();
            }
            (None, Some(_) | None) => unreachable!(),
        }

        self.stream.del(self.stream_sequence).await.unwrap();

        MemoryUsedServiceResponder
    }
}
