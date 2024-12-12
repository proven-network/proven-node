mod error;
mod subscription_handler;

use crate::stream::InitializedNatsStream;
use crate::subject::NatsSubject;
pub use error::Error;
use subscription_handler::ClientSubscriptionHandler;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::{Client, ClientOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;
use proven_messaging::subject::Subject;
use proven_messaging::Message;
use tokio::sync::{mpsc, Mutex};

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct NatsClientOptions;
impl ClientOptions for NatsClientOptions {}

/// A client for an in-memory service.
#[derive(Debug)]
pub struct NatsClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
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
    reply_receiver: Arc<Mutex<mpsc::Receiver<Message<X::ResponseType>>>>,
    reply_subject: NatsSubject<X::ResponseType, D, S>,
    stream: <Self as Client<X, T, D, S>>::StreamType,
}

impl<X, T, D, S> Clone for NatsClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
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
            reply_receiver: self.reply_receiver.clone(),
            reply_subject: self.reply_subject.clone(),
            stream: self.stream.clone(),
        }
    }
}

#[async_trait]
impl<X, T, D, S> Client<X, T, D, S> for NatsClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
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

    type Options = NatsClientOptions;

    type StreamType = InitializedNatsStream<T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        let (sender, receiver) = mpsc::channel::<Message<X::ResponseType>>(100);

        let reply_handler = ClientSubscriptionHandler::new(sender);

        let client = async_nats::connect("localhost:4222").await.unwrap();
        let reply_subject: NatsSubject<X::ResponseType, D, S> =
            NatsSubject::new(client, format!("{name}_reply")).unwrap();

        reply_subject.subscribe(reply_handler).await.unwrap();

        Ok(Self {
            reply_receiver: Arc::new(Mutex::new(receiver)),
            reply_subject,
            stream,
        })
    }

    async fn request(&self, request: T) -> Result<X::ResponseType, Self::Error> {
        let mut reply_receiver = self.reply_receiver.lock().await;

        let mut headers = HeaderMap::new();
        headers.insert("service-client", String::from(self.reply_subject.clone()));

        let _ = self
            .stream
            .publish(Message {
                headers: Some(headers),
                payload: request,
            })
            .await;

        let reply = reply_receiver.recv().await.unwrap();
        drop(reply_receiver);

        Ok(reply.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::NatsServiceOptions;
    use crate::stream::{NatsStream, NatsStreamOptions};

    use std::time::Duration;

    use async_nats::ConnectOptions;
    use proven_messaging::stream::Stream;
    use proven_messaging::Message;
    use proven_messaging::ServiceResponse;
    use serde::{Deserialize, Serialize};
    use tokio::time::timeout;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        content: String,
    }

    impl TryFrom<Bytes> for TestMessage {
        type Error = serde_json::Error;

        fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
            serde_json::from_slice(&bytes)
        }
    }

    impl TryInto<Bytes> for TestMessage {
        type Error = serde_json::Error;

        fn try_into(self) -> Result<Bytes, Self::Error> {
            Ok(Bytes::from(serde_json::to_vec(&self)?))
        }
    }

    #[derive(Clone, Debug)]
    struct TestHandler;

    #[async_trait]
    impl ServiceHandler<TestMessage, serde_json::Error, serde_json::Error> for TestHandler {
        type Error = serde_json::Error;
        type ResponseType = TestMessage;

        async fn handle(
            &self,
            message: Message<TestMessage>,
        ) -> Result<ServiceResponse<TestMessage>, Self::Error> {
            // Echo back the message with "response: " prefixed
            Ok(TestMessage {
                content: format!("response: {}", message.payload.content),
            }
            .into())
        }
    }

    async fn cleanup_stream(client: &async_nats::Client, stream_name: &str) {
        let js = async_nats::jetstream::new(client.clone());
        let _ = js.delete_stream(stream_name).await;
    }

    #[tokio::test]
    async fn test_client_request_response() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_client_stream").await;

        // Create stream
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_client_stream",
            NatsStreamOptions {
                client: client.clone(),
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Start service
        let _service = initialized_stream
            .clone()
            .start_service(
                "test_service",
                NatsServiceOptions {
                    client: client.clone(),
                    durable_name: None,
                    jetstream_context: async_nats::jetstream::new(client.clone()),
                },
                TestHandler,
            )
            .await
            .expect("Failed to start service");

        // Create client
        let client = initialized_stream
            .client("test_service", NatsClientOptions, TestHandler)
            .await
            .expect("Failed to create client");

        // Send request and get response with timeout
        let request = TestMessage {
            content: "hello".to_string(),
        };

        let response = timeout(Duration::from_secs(5), client.request(request))
            .await
            .expect("Request timed out")
            .expect("Failed to send request");

        assert_eq!(
            response,
            TestMessage {
                content: "response: hello".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_client_request_timeout() {
        // Connect to NATS
        let client = ConnectOptions::default()
            .connection_timeout(Duration::from_secs(5))
            .connect("localhost:4222")
            .await
            .expect("Failed to connect to NATS");

        cleanup_stream(&client, "test_client_timeout").await;

        // Create stream without service
        let stream = NatsStream::<TestMessage, serde_json::Error, serde_json::Error>::new(
            "test_client_timeout",
            NatsStreamOptions {
                client: client.clone(),
            },
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Create client
        let client = initialized_stream
            .client("test_client", NatsClientOptions, TestHandler)
            .await
            .expect("Failed to create client");

        // Send request without service running
        let request = TestMessage {
            content: "hello".to_string(),
        };

        let result = timeout(Duration::from_secs(1), client.request(request)).await;
        assert!(result.is_err(), "Expected timeout error");
    }
}
