mod error;

pub use error::Error;

use std::future::Future;
use std::pin::Pin;

use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_stream::{Stream, Stream1, Stream2, Stream3, StreamHandlerError};
use serde::Deserialize;

#[derive(Deserialize)]
struct StreamPublishReply {
    stream: String,
    seq: u64,
}

#[derive(Clone)]
pub enum ScopeMethod {
    StreamPostfix,
    SubjectPrefix,
}

pub struct NatsStreamOptions {
    pub client: Client,
    pub local_name: String,
    pub stream_name: String,
}

#[derive(Clone)]
pub struct NatsStream<HE: StreamHandlerError> {
    client: Client,
    jetstream_context: JetStreamContext,
    local_name: String,
    stream_name: String,
    _handler_error: std::marker::PhantomData<HE>,
}

impl<HE> NatsStream<HE>
where
    HE: StreamHandlerError,
{
    pub fn new(
        NatsStreamOptions {
            client,
            local_name,
            stream_name,
        }: NatsStreamOptions,
    ) -> Self {
        let jetstream_context = jetstream::new(client.clone());

        Self {
            client,
            jetstream_context,
            local_name,
            stream_name,
            _handler_error: std::marker::PhantomData,
        }
    }

    fn with_scope(&self, scope: String) -> Self {
        Self {
            client: self.client.clone(),
            jetstream_context: self.jetstream_context.clone(),
            local_name: self.local_name.clone(),
            stream_name: format!("{}_{}", self.stream_name, scope),
            _handler_error: std::marker::PhantomData,
        }
    }

    fn get_durable_consumer_name(&self) -> String {
        format!("{}_{}_consumer", self.local_name, self.stream_name).to_ascii_uppercase()
    }

    fn get_reply_stream_name(&self) -> String {
        format!("{}_reply", self.stream_name).to_ascii_uppercase()
    }

    async fn get_reply_stream(&self) -> Result<jetstream::stream::Stream, Error<HE>> {
        self.jetstream_context
            .create_stream(StreamConfig {
                name: self.get_reply_stream_name(),
                allow_direct: true,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::StreamCreate(e.kind()))
    }

    fn get_request_stream_name(&self) -> String {
        format!("{}_request", self.stream_name).to_ascii_uppercase()
    }

    async fn get_request_stream(&self) -> Result<jetstream::stream::Stream, Error<HE>> {
        self.jetstream_context
            .create_stream(StreamConfig {
                name: self.get_request_stream_name(),
                ..Default::default()
            })
            .await
            .map_err(|e| Error::StreamCreate(e.kind()))
    }
}

#[async_trait]
impl<HE> Stream<HE> for NatsStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;

    async fn handle(
        &self,
        handler: impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Result<Bytes, HE>> + Send>> + Send + Sync,
    ) -> Result<(), Self::Error> {
        println!("Subscribing to {}", self.get_request_stream_name());

        // Setup stream and consumer
        let mut messages = self
            .get_request_stream()
            .await?
            .create_consumer(ConsumerConfig {
                durable_name: Some(self.get_durable_consumer_name()),
                ..Default::default()
            })
            .await
            .map_err(|e| Error::ConsumerCreate(e.kind()))?
            .messages()
            .await
            .map_err(|e| Error::ConsumerStream(e.kind()))?;

        // Process messages
        while let Some(message) = messages.next().await {
            let message = message.map_err(|e| Error::ConsumerMessages(e.kind()))?;

            // Grab message seq number
            let seq = message.info().map_err(|_| Error::NoInfo)?.stream_sequence;

            let response = handler(message.payload.clone())
                .await
                .map_err(|e| Error::Handler(e))?;

            // Ensure reply stream exists
            self.get_reply_stream().await?;

            // Headers that ensure seq matches between request and response
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Expected-Last-Sequence", (seq - 1).to_string());

            self.client
                .publish_with_headers(self.get_reply_stream_name(), headers, response)
                .await
                .map_err(|e| Error::ReplyPublish(e.kind()))?;

            message.double_ack().await.map_err(|_| Error::ConsumerAck)?;
        }

        Ok(())
    }

    fn name(&self) -> String {
        self.stream_name.clone()
    }

    async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error> {
        // Ensure request stream exists
        self.get_request_stream().await?;

        println!("publishing on subject: {}", self.get_request_stream_name());

        let response = loop {
            match self
                .client
                .request(self.get_request_stream_name(), data.clone())
                .await
            {
                Ok(response) => break response,
                Err(e) => {
                    if e.kind() == async_nats::client::RequestErrorKind::NoResponders {
                        tokio::task::yield_now().await;
                    } else {
                        return Err(Error::Request(e.kind()));
                    }
                }
            }
        };

        // Parse the seq number from the response json (use serde_json)
        let response: StreamPublishReply = serde_json::from_slice(&response.payload)?;
        assert_eq!(response.stream, self.get_request_stream_name());

        println!("Published message with seq: {}", response.seq);

        // Wait for corrosponding reply on reply stream
        let reply_stream = self.get_reply_stream().await?;
        loop {
            match reply_stream.direct_get(response.seq).await {
                Ok(message) => {
                    reply_stream
                        .delete_message(response.seq)
                        .await
                        .map_err(|e| Error::ReplyDelete(e.kind()))?;
                    return Ok(message.payload);
                }
                Err(e) => {
                    if e.kind() == async_nats::jetstream::stream::DirectGetErrorKind::NotFound {
                        // println!("Waiting for reply message");
                        tokio::task::yield_now().await;
                    } else {
                        println!("Error: {:?}", e);
                        return Err(Error::ReplyDirectGet(e.kind()));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<HE> Stream1<HE> for NatsStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;
    type Scoped = NatsStream<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl<HE> Stream2<HE> for NatsStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;
    type Scoped = NatsStream<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[async_trait]
impl<HE> Stream3<HE> for NatsStream<HE>
where
    HE: StreamHandlerError,
{
    type Error = Error<HE>;
    type Scoped = NatsStream<HE>;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    struct TestHandlerError;

    impl std::fmt::Display for TestHandlerError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TestHandlerError")
        }
    }

    impl std::error::Error for TestHandlerError {}
    impl StreamHandlerError for TestHandlerError {}

    #[tokio::test]
    async fn test_stream_name_scoping() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();

        let subscriber = NatsStream::<TestHandlerError>::new(NatsStreamOptions {
            client,
            local_name: "local".to_string(),
            stream_name: "SQL".to_string(),
        });

        let subscriber = subscriber.with_scope("app1".to_string());
        assert_eq!(subscriber.stream_name, "SQL_app1");

        let subscriber = subscriber.with_scope("db1".to_string());
        assert_eq!(subscriber.stream_name, "SQL_app1_db1");
    }
}
