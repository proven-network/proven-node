mod error;

use bytes::BufMut;
use bytes::BytesMut;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use async_nats::Client as NatsClient;
use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use proven_messaging::subscription_responder::{SubscriptionResponder, UsedSubscriptionResponder};

static MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// A used responder for a NATS subscription.
#[derive(Debug)]
pub struct NatsUsedSubscriptionResponder;
impl UsedSubscriptionResponder for NatsUsedSubscriptionResponder {}

/// A responder for a NATS service.
#[derive(Clone, Debug)]
pub struct NatsSubscriptionResponder<R, RD, RS>
where
    Self: Send + Sync + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    nats_client: NatsClient,
    reply_stream_name: String,
    request_id: String,
    _marker: PhantomData<R>,
}

impl<R, RD, RS> NatsSubscriptionResponder<R, RD, RS>
where
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    /// Creates a new NATS service responder.
    #[must_use]
    pub const fn new(
        nats_client: NatsClient,
        reply_stream_name: String,
        request_id: String,
    ) -> Self {
        Self {
            nats_client,
            reply_stream_name,
            request_id,
            _marker: PhantomData,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn batch_stream<W>(stream: W) -> impl Stream<Item = Bytes> + Send + Unpin
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let mut stream = Box::pin(stream);
        let mut current_batch = BytesMut::with_capacity(MAX_MESSAGE_SIZE);
        let mut current_size = 0;

        futures::stream::poll_fn(move |cx| {
            loop {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        if let Ok(item_bytes) = item.try_into() as Result<Bytes, RS> {
                            let item_size = item_bytes.len();
                            let total_size = item_size + 4;

                            if !current_batch.is_empty()
                                && current_size + total_size > MAX_MESSAGE_SIZE
                            {
                                // Yield current batch
                                let batch = current_batch.split().freeze();
                                current_batch.reserve(MAX_MESSAGE_SIZE);

                                // Start new batch with current item
                                current_batch.put_u32(item_bytes.len() as u32);
                                current_batch.extend_from_slice(&item_bytes);
                                current_size = total_size;

                                return Poll::Ready(Some(batch));
                            }

                            // Add to current batch
                            current_batch.put_u32(item_bytes.len() as u32);
                            current_batch.extend_from_slice(&item_bytes);
                            current_size += total_size;
                        }
                    }
                    Poll::Ready(None) => {
                        return if current_batch.is_empty() {
                            Poll::Ready(None)
                        } else {
                            let batch = current_batch.split().freeze();
                            Poll::Ready(Some(batch))
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
    }
}

#[async_trait]
impl<R, D, S> SubscriptionResponder<R, D, S> for NatsSubscriptionResponder<R, D, S>
where
    R: Clone
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

    type UsedResponder = NatsUsedSubscriptionResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        NatsUsedSubscriptionResponder
    }

    async fn reply(self, response: R) -> Self::UsedResponder {
        let result_bytes: Bytes = response.try_into().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Reply-Id", self.request_id.clone());

        self.nats_client
            .publish_with_headers(self.reply_stream_name, headers, result_bytes)
            .await
            .unwrap();

        NatsUsedSubscriptionResponder
    }

    async fn stream<W>(self, response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Sync + Unpin,
    {
        let batched_stream = Self::batch_stream(response_stream);
        let mut peekable_stream = batched_stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);

        let mut stream_id = 0;

        loop {
            match (
                pinned_stream.as_mut().next().await,
                pinned_stream.as_mut().peek().await,
            ) {
                (Some(batched_bytes), Some(_)) => {
                    stream_id += 1;

                    let mut next_headers = HeaderMap::new();
                    next_headers.insert("Reply-Msg-Id", self.request_id.clone());
                    next_headers.insert("Reply-Seq", stream_id.to_string());

                    self.nats_client
                        .publish_with_headers(
                            self.reply_stream_name.clone(),
                            next_headers,
                            batched_bytes,
                        )
                        .await
                        .unwrap();
                }
                (Some(batched_bytes), None) => {
                    stream_id += 1;

                    let mut end_headers = HeaderMap::new();
                    end_headers.insert(
                        "Nats-Msg-Id",
                        format!("reply:{}:{}", self.request_id.clone(), stream_id),
                    );
                    end_headers.insert("Reply-Msg-Id", self.request_id.clone());
                    end_headers.insert("Reply-Seq", stream_id.to_string());
                    end_headers.insert("Reply-Seq-End", stream_id.to_string());

                    self.nats_client
                        .publish_with_headers(self.reply_stream_name, end_headers, batched_bytes)
                        .await
                        .unwrap();

                    break;
                }
                (None, Some(_) | None) => unreachable!(),
            }
        }

        NatsUsedSubscriptionResponder
    }
}
