//! Implementation of streams using NATS with HA replication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::Error;

use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use async_nats::jetstream::stream::Config as StreamConfig;
use async_nats::jetstream::stream::Stream as JetsteamSteam;
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_stream::{Stream, Stream1, Stream2, Stream3, StreamHandler};
use serde::Deserialize;

#[derive(Deserialize)]
struct StreamPublishReply {
    stream: String,
    seq: u64,
}

/// Options for configuring a `NatsStream`.
pub struct NatsStreamOptions {
    /// The NATS client to use.
    pub client: Client,

    /// The name of the stream (may be refined through scopes).
    pub stream_name: String,
}

/// Implementation of streams using NATS with HA replication.
#[derive(Clone, Debug)]
pub struct NatsStream<H>
where
    H: StreamHandler,
{
    client: Client,
    jetstream_context: JetStreamContext,
    stream_name: String,
    _handler: std::marker::PhantomData<H>,
}

impl<H> NatsStream<H>
where
    H: StreamHandler,
{
    /// Creates a new `NatsStream` with the specified options.
    #[must_use]
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
        }
    }

    fn with_scope(&self, scope: &str) -> Self {
        Self {
            client: self.client.clone(),
            jetstream_context: self.jetstream_context.clone(),
            stream_name: format!("{}_{}", self.stream_name, scope).to_ascii_uppercase(),
            _handler: std::marker::PhantomData,
        }
    }

    fn get_reply_stream_name(&self) -> String {
        format!("{}_reply", self.stream_name).to_ascii_uppercase()
    }

    async fn get_reply_stream(&self) -> Result<JetsteamSteam, Error<H>> {
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

    async fn get_request_stream(&self) -> Result<JetsteamSteam, Error<H>> {
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
impl<H> Stream<H> for NatsStream<H>
where
    H: StreamHandler,
{
    type Error = Error<H>;

    async fn handle(&self, handler: H) -> Result<(), Self::Error> {
        println!("Subscribing to {}", self.get_request_stream_name());

        let mut stream = self.get_request_stream().await?;
        let mut consumer = stream
            .create_consumer(ConsumerConfig {
                durable_name: None,
                ..Default::default()
            })
            .await
            .map_err(|e| Error::ConsumerCreate(e.kind()))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| Error::ConsumerStream(e.kind()))?;

        let mut caught_up = false;

        let stream_info = stream
            .info()
            .await
            .map_err(|e| Error::StreamInfo(e.kind()))?;

        if stream_info.state.messages == 0 {
            caught_up = true;
            handler.on_caught_up().await.map_err(Error::Handler)?;
        }

        // Process messages
        while let Some(message) = messages.next().await {
            let message = message.map_err(|e| Error::ConsumerMessages(e.kind()))?;
            let seq = message.info().map_err(|_| Error::NoInfo)?.stream_sequence;

            let response = handler
                .handle(message.payload.clone())
                .await
                .map_err(Error::Handler)?;

            // Ensure reply stream exists
            self.get_reply_stream().await?;

            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Expected-Last-Sequence", (seq - 1).to_string());

            // Copy headers from response to reply
            for (key, value) in response.headers {
                headers.insert(key, value);
            }

            self.client
                .publish_with_headers(self.get_reply_stream_name(), headers, response.data)
                .await
                .map_err(|e| Error::ReplyPublish(e.kind()))?;

            message.double_ack().await.map_err(|_| Error::ConsumerAck)?;

            if !caught_up {
                let stream_info = stream
                    .info()
                    .await
                    .map_err(|e| Error::StreamInfo(e.kind()))?;

                let consumer_info = consumer
                    .info()
                    .await
                    .map_err(|e| Error::ConsumerInfo(e.kind()))?;

                if consumer_info.num_pending == 0
                    && consumer_info.delivered.stream_sequence >= stream_info.state.last_sequence
                {
                    caught_up = true;
                    handler.on_caught_up().await.map_err(Error::Handler)?;
                }
            }
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

                    // Check and handle message persistence
                    if let Some(value) = message.headers.get("Request-Message-Should-Persist") {
                        if value.as_str() == "false" {
                            if let Ok(stream) = self.get_request_stream().await {
                                stream
                                    .delete_message(response.seq)
                                    .await
                                    .map_err(|e| Error::RequestDelete(e.kind()))?;
                            }
                        }
                    }

                    return Ok(message.payload);
                }
                Err(e) => {
                    if e.kind() == async_nats::jetstream::stream::DirectGetErrorKind::NotFound {
                        tokio::task::yield_now().await;
                    } else {
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
        impl<H> $name<H> for NatsStream<H>
        where
            H: StreamHandler,
        {
            type Error = Error<H>;
            type Scoped = NatsStream<H>;

            fn scope(&self, scope: String) -> Self::Scoped {
                self.with_scope(&scope)
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

    use proven_stream::{HandlerResponse, StreamHandlerError};

    #[derive(Clone, Debug)]
    struct TestHandlerError;

    impl std::fmt::Display for TestHandlerError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TestHandlerError")
        }
    }

    impl std::error::Error for TestHandlerError {}
    impl StreamHandlerError for TestHandlerError {}

    #[derive(Clone, Debug)]
    struct PublishTestHandler {
        tx: tokio::sync::mpsc::Sender<Bytes>,
    }

    impl PublishTestHandler {
        const fn new(tx: tokio::sync::mpsc::Sender<Bytes>) -> Self {
            Self { tx }
        }
    }

    #[async_trait]
    impl StreamHandler for PublishTestHandler {
        type Error = TestHandlerError;

        async fn handle(&self, data: Bytes) -> Result<HandlerResponse, Self::Error> {
            self.tx.send(data.clone()).await.unwrap();

            Ok(HandlerResponse {
                data,
                ..Default::default()
            })
        }
    }

    #[derive(Clone, Debug)]
    struct RequestTestHandler;

    #[async_trait]
    impl StreamHandler for RequestTestHandler {
        type Error = TestHandlerError;

        async fn handle(&self, data: Bytes) -> Result<HandlerResponse, Self::Error> {
            let mut response = b"reply: ".to_vec();
            response.extend_from_slice(&data);

            Ok(HandlerResponse {
                data: Bytes::from(response),
                ..Default::default()
            })
        }
    }

    #[derive(Clone, Debug)]
    struct CatchUpTestHandler {
        caught_up: std::sync::Arc<std::sync::atomic::AtomicBool>,
        message_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl CatchUpTestHandler {
        fn new() -> Self {
            Self {
                caught_up: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                message_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn is_caught_up(&self) -> bool {
            self.caught_up.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn message_count(&self) -> usize {
            self.message_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl StreamHandler for CatchUpTestHandler {
        type Error = TestHandlerError;

        async fn handle(&self, data: Bytes) -> Result<HandlerResponse, Self::Error> {
            self.message_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            Ok(HandlerResponse {
                data,
                ..Default::default()
            })
        }

        async fn on_caught_up(&self) -> Result<(), Self::Error> {
            self.caught_up
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    async fn cleanup_stream(client: &Client, stream_name: &str) {
        let js = jetstream::new(client.clone());
        // Ignore errors since the stream might not exist
        let _ = js.delete_stream(stream_name).await;
    }

    #[tokio::test]
    async fn test_caught_up() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();
        let client2 = client.clone();

        // Clean up the stream and its request/reply variants before starting
        cleanup_stream(&client, "TEST_CATCHUP_REQUEST").await;
        cleanup_stream(&client, "TEST_CATCHUP_REPLY").await;

        let publisher = NatsStream::<CatchUpTestHandler>::new(NatsStreamOptions {
            client,
            stream_name: "TEST_CATCHUP".to_string(),
        });

        // Publish three messages before starting the handler
        for i in 1..=3 {
            publisher
                .publish(Bytes::from(format!("test message {i}")))
                .await
                .unwrap();
        }

        let subscriber = NatsStream::<CatchUpTestHandler>::new(NatsStreamOptions {
            client: client2,
            stream_name: "TEST_CATCHUP".to_string(),
        });

        let handler = CatchUpTestHandler::new();
        let handler_clone = handler.clone();

        // Start handler
        let handle = tokio::spawn(async move {
            subscriber.handle(handler).await.unwrap();
        });

        // Wait for processing to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify that all messages were processed before on_caught_up was called
        assert!(handler_clone.is_caught_up());
        assert_eq!(handler_clone.message_count(), 3);

        // Clean up
        handle.abort();
    }

    #[tokio::test]
    async fn test_stream_name_scoping() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();

        let subscriber = NatsStream::<PublishTestHandler>::new(NatsStreamOptions {
            client,
            stream_name: "SQL".to_string(),
        });

        // Should force uppercase
        let subscriber = subscriber.with_scope("app1");
        assert_eq!(subscriber.stream_name, "SQL_APP1");

        let subscriber = subscriber.with_scope("DB1");
        assert_eq!(subscriber.stream_name, "SQL_APP1_DB1");
    }

    #[tokio::test]
    async fn test_publish() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();
        let client2 = client.clone();

        let publisher = NatsStream::<PublishTestHandler>::new(NatsStreamOptions {
            client,
            stream_name: "TEST_PUB".to_string(),
        });

        let subscriber = NatsStream::<PublishTestHandler>::new(NatsStreamOptions {
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

        let requester = NatsStream::<RequestTestHandler>::new(NatsStreamOptions {
            client,
            stream_name: "TEST_REQ".to_string(),
        });

        let responder = NatsStream::<RequestTestHandler>::new(NatsStreamOptions {
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

    #[derive(Clone, Debug)]
    struct NonPersistentRequestTestHandler;

    #[async_trait]
    impl StreamHandler for NonPersistentRequestTestHandler {
        type Error = TestHandlerError;

        async fn handle(&self, data: Bytes) -> Result<HandlerResponse, Self::Error> {
            let mut headers = std::collections::HashMap::new();
            headers.insert(
                "Request-Message-Should-Persist".to_string(),
                "false".to_string(),
            );

            Ok(HandlerResponse { data, headers })
        }
    }

    #[tokio::test]
    async fn test_request_message_deletion() {
        let client = async_nats::connect("nats://localhost:4222").await.unwrap();
        let client2 = client.clone();

        // Clean up the stream and its request/reply variants before starting
        cleanup_stream(&client, "TEST_REQ_DELETE_REQUEST").await;
        cleanup_stream(&client, "TEST_REQ_DELETE_REPLY").await;

        let requester = NatsStream::<NonPersistentRequestTestHandler>::new(NatsStreamOptions {
            client,
            stream_name: "TEST_REQ_DELETE".to_string(),
        });

        let responder = NatsStream::<NonPersistentRequestTestHandler>::new(NatsStreamOptions {
            client: client2,
            stream_name: "TEST_REQ_DELETE".to_string(),
        });

        // Start handler
        tokio::spawn({
            let responder = responder.clone();
            async move {
                responder
                    .handle(NonPersistentRequestTestHandler)
                    .await
                    .unwrap();
            }
        });

        // Give handler time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Make a request
        let message = Bytes::from("test message");
        let _ = requester.request(message).await.unwrap();

        // Verify the message was deleted from request stream
        let js = jetstream::new(requester.client.clone());
        let mut stream = js.get_stream("TEST_REQ_DELETE_REQUEST").await.unwrap();
        let info = stream.info().await.unwrap();

        assert_eq!(info.state.messages, 0, "Request message was not deleted");
    }
}
