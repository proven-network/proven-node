mod error;

pub use error::Error;

use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_stream::{Stream, Stream1, Stream2, Stream3, StreamHandler, StreamHandlerError};
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
    pub stream_name: String,
}

#[derive(Clone)]
pub struct NatsStream<H, HE>
where
    H: StreamHandler<HE>,
    HE: StreamHandlerError,
{
    client: Client,
    jetstream_context: JetStreamContext,
    stream_name: String,
    _handler: std::marker::PhantomData<H>,
    _handler_error: std::marker::PhantomData<HE>,
}

impl<H, HE> NatsStream<H, HE>
where
    H: StreamHandler<HE>,
    HE: StreamHandlerError,
{
    pub fn new(
        NatsStreamOptions {
            client,
            stream_name,
        }: NatsStreamOptions,
    ) -> Self {
        let jetstream_context = jetstream::new(client.clone());

        Self {
            client,
            jetstream_context,
            stream_name,
            _handler: std::marker::PhantomData,
            _handler_error: std::marker::PhantomData,
        }
    }

    fn with_scope(&self, scope: String) -> Self {
        Self {
            client: self.client.clone(),
            jetstream_context: self.jetstream_context.clone(),
            stream_name: format!("{}_{}", self.stream_name, scope),
            _handler: std::marker::PhantomData,
            _handler_error: std::marker::PhantomData,
        }
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
impl<H, HE> Stream<H, HE> for NatsStream<H, HE>
where
    H: StreamHandler<HE>,
    HE: StreamHandlerError,
{
    type Error = Error<HE>;

    async fn handle(&self, handler: H) -> Result<(), Self::Error> {
        println!("Subscribing to {}", self.get_request_stream_name());

        // Setup stream and consumer
        let mut messages = self
            .get_request_stream()
            .await?
            .create_consumer(ConsumerConfig {
                durable_name: None,
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
            let seq = message.info().map_err(|_| Error::NoInfo)?.stream_sequence;

            let response = handler
                .handle(message.payload.clone())
                .await
                .map_err(|e| Error::Handler(e))?;

            // Ensure reply stream exists
            self.get_reply_stream().await?;

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

    async fn publish(&self, data: Bytes) -> Result<(), Self::Error> {
        self.get_request_stream().await?;

        println!("publishing on subject: {}", self.get_request_stream_name());

        self.client
            .publish(self.get_request_stream_name(), data.clone())
            .await
            .map_err(|e| Error::Publish(e.kind()))?;

        Ok(())
    }

    async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error> {
        // Ensure request stream exists
        self.get_request_stream().await?;

        println!("requesting on subject: {}", self.get_request_stream_name());

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

macro_rules! impl_scoped_stream {
    ($name:ident, $parent:ident) => {
        #[async_trait]
        impl<H, HE> $name<H, HE> for NatsStream<H, HE>
        where
            H: StreamHandler<HE>,
            HE: StreamHandlerError,
        {
            type Error = Error<HE>;
            type Scoped = NatsStream<H, HE>;

            fn scope(&self, scope: String) -> Self::Scoped {
                self.with_scope(scope)
            }
        }
    };
}

impl_scoped_stream!(Stream1, Stream);
impl_scoped_stream!(Stream2, Stream1);
impl_scoped_stream!(Stream3, Stream2);

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

    #[derive(Clone)]
    struct PublishTestHandler {
        tx: tokio::sync::mpsc::Sender<Bytes>,
    }

    impl PublishTestHandler {
        fn new(tx: tokio::sync::mpsc::Sender<Bytes>) -> Self {
            Self { tx }
        }
    }

    #[async_trait]
    impl StreamHandler<TestHandlerError> for PublishTestHandler {
        async fn handle(&self, data: Bytes) -> Result<Bytes, TestHandlerError> {
            self.tx.send(data.clone()).await.unwrap();
            Ok(data)
        }
    }

    #[derive(Clone)]
    struct RequestTestHandler;

    #[async_trait]
    impl StreamHandler<TestHandlerError> for RequestTestHandler {
        async fn handle(&self, data: Bytes) -> Result<Bytes, TestHandlerError> {
            let mut response = b"reply: ".to_vec();
            response.extend_from_slice(&data);
            Ok(Bytes::from(response))
        }
    }

    #[tokio::test]
    async fn test_stream_name_scoping() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();

        let subscriber =
            NatsStream::<PublishTestHandler, TestHandlerError>::new(NatsStreamOptions {
                client,
                stream_name: "SQL".to_string(),
            });

        let subscriber = subscriber.with_scope("app1".to_string());
        assert_eq!(subscriber.stream_name, "SQL_app1");

        let subscriber = subscriber.with_scope("db1".to_string());
        assert_eq!(subscriber.stream_name, "SQL_app1_db1");
    }

    #[tokio::test]
    async fn test_publish() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();
        let client2 = client.clone();

        let publisher =
            NatsStream::<PublishTestHandler, TestHandlerError>::new(NatsStreamOptions {
                client,
                stream_name: "TEST_PUB".to_string(),
            });

        let subscriber =
            NatsStream::<PublishTestHandler, TestHandlerError>::new(NatsStreamOptions {
                client: client2,
                stream_name: "TEST_PUB".to_string(),
            });

        // Channel to communicate between publisher and subscriber
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Start handler
        tokio::spawn({
            let subscriber = subscriber.clone();

            async move {
                subscriber
                    .handle(PublishTestHandler::new(tx))
                    .await
                    .unwrap();
            }
        });

        // Give handler time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Test message
        let message = Bytes::from("test message");

        // Publish message
        publisher.publish(message.clone()).await.unwrap();

        // Wait for message to be received
        let received: Bytes = rx.recv().await.unwrap();
        assert_eq!(received, message);
    }

    #[tokio::test]
    async fn test_request() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();
        let client2 = client.clone();

        let requester =
            NatsStream::<RequestTestHandler, TestHandlerError>::new(NatsStreamOptions {
                client,
                stream_name: "TEST_REQ".to_string(),
            });

        let responder =
            NatsStream::<RequestTestHandler, TestHandlerError>::new(NatsStreamOptions {
                client: client2,
                stream_name: "TEST_REQ".to_string(),
            });

        // Start handler that echoes request with "reply: " prefix
        tokio::spawn({
            let responder = responder.clone();
            async move {
                responder.handle(RequestTestHandler).await.unwrap();
            }
        });

        // Give handler time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Test message
        let message = Bytes::from("test message");

        // Make request and verify response
        let response = requester.request(message).await.unwrap();
        assert_eq!(response, Bytes::from("reply: test message"));
    }
}
