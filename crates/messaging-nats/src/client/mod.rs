mod error;

use crate::stream::InitializedNatsStream;
pub use error::Error;
use futures::StreamExt;

use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_nats::jetstream::consumer::pull::Config as NatsConsumerConfig;
use async_nats::jetstream::consumer::Consumer as NatsConsumerType;
use async_nats::jetstream::stream::Config as NatsStreamConfig;
use async_nats::jetstream::Context;
use async_nats::Client as AsyncNatsClient;
use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::channel::mpsc::{self, Sender};
use proven_messaging::client::{Client, ClientOptions, ClientResponseType};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;
use tokio::sync::{oneshot, Mutex};

use uuid::Uuid;

type ResponseMap<R> = HashMap<usize, oneshot::Sender<ClientResponseType<R>>>;
type StreamMap<R> = HashMap<usize, StreamState<R>>;

/// The state of a streaming response.
#[derive(Debug)]
struct StreamState<R> {
    sender: Sender<R>,
    received_messages: HashSet<usize>,
    total_expected: Option<usize>,
}

/// Options for the NATS service client.
#[derive(Clone, Debug)]
pub struct NatsClientOptions {
    /// The NATS client to use for the service client.
    pub client: AsyncNatsClient,
}

impl ClientOptions for NatsClientOptions {}

/// A client for sending requests to a NATS-based service.
/// TODO: Look into using machine identifiers and shortcut responses in-memory if service is on same machine.
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
    nats_client: AsyncNatsClient,
    nats_jetstream_context: Context,
    reply_stream_name: String,
    request_id_counter: Arc<AtomicUsize>,
    response_map: Arc<Mutex<ResponseMap<X::ResponseType>>>,
    stream_map: Arc<Mutex<StreamMap<X::ResponseType>>>,
    stream_name: String,
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
            nats_client: self.nats_client.clone(),
            nats_jetstream_context: self.nats_jetstream_context.clone(),
            reply_stream_name: self.reply_stream_name.clone(),
            request_id_counter: self.request_id_counter.clone(),
            response_map: self.response_map.clone(),
            stream_map: self.stream_map.clone(),
            stream_name: self.stream_name.clone(),
        }
    }
}

impl<X, T, D, S> Drop for NatsClient<X, T, D, S>
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
    fn drop(&mut self) {
        let reply_stream_name = self.reply_stream_name.clone();
        let jetstream_context = self.nats_jetstream_context.clone();
        tokio::spawn(async move {
            let _ = jetstream_context.delete_stream(&reply_stream_name).await;
        });
    }
}

impl<X, T, D, S> NatsClient<X, T, D, S>
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
    fn spawn_response_handler(
        consumer: NatsConsumerType<NatsConsumerConfig>,
        response_map: Arc<Mutex<ResponseMap<X::ResponseType>>>,
        stream_map: Arc<Mutex<StreamMap<X::ResponseType>>>,
    ) {
        tokio::spawn(async move {
            let mut messages = consumer.messages().await.unwrap();
            while let Some(msg) = messages.next().await {
                let msg = msg.unwrap();

                match msg
                    .headers
                    .as_ref()
                    .map(|h| (h.get("request-id"), h.get("stream-id"), h.get("stream-end")))
                {
                    // Single response
                    Some((Some(request_id), None, None)) => {
                        let request_id: usize = request_id.to_string().parse().unwrap();

                        let response: X::ResponseType = msg.payload.clone().try_into().unwrap();

                        let mut map = response_map.lock().await;
                        if let Some(sender) = map.remove(&request_id) {
                            let _ = sender.send(ClientResponseType::Response(response));
                        }
                    }
                    // Final streamed response has the total expected - if that many are received the stream can close
                    Some((Some(request_id), Some(stream_id), Some(total_expected))) => {
                        let request_id: usize = request_id.to_string().parse().unwrap();
                        let stream_id: usize = stream_id.to_string().parse().unwrap();
                        let total_expected: usize = total_expected.to_string().parse().unwrap();
                        let response: X::ResponseType = msg.payload.clone().try_into().unwrap();

                        // No need to manage stream state if only one message is expected
                        if total_expected == 1 {
                            let mut response_map = response_map.lock().await;
                            if let Some(sender) = response_map.remove(&request_id) {
                                let _ = sender.send(ClientResponseType::Stream(Box::new(
                                    futures::stream::iter(vec![response]),
                                )));
                            }
                            drop(response_map);
                            continue;
                        }

                        let mut stream_map = stream_map.lock().await;
                        if let Some(state) = stream_map.get_mut(&request_id) {
                            state.received_messages.insert(stream_id);
                            state.total_expected = Some(total_expected);
                            let _ = state.sender.try_send(response);

                            if state.received_messages.len() == total_expected {
                                state.sender.close_channel();
                                stream_map.remove(&request_id);
                            }
                        } else {
                            let (tx, rx) = mpsc::channel(32);
                            let mut state = StreamState {
                                sender: tx,
                                received_messages: HashSet::new(),
                                total_expected: Some(total_expected),
                            };
                            state.received_messages.insert(stream_id);
                            let _ = state.sender.try_send(response);
                            stream_map.insert(request_id, state);

                            let mut response_map = response_map.lock().await;
                            if let Some(sender) = response_map.remove(&request_id) {
                                let stream = Box::new(rx);
                                let _ = sender.send(ClientResponseType::Stream(stream));
                            }
                        }
                        drop(stream_map);
                    }
                    // A streamed response but not the final one
                    Some((Some(request_id), Some(stream_id), None)) => {
                        let request_id: usize = request_id.to_string().parse().unwrap();
                        let stream_id: usize = stream_id.to_string().parse().unwrap();
                        let response: X::ResponseType = msg.payload.clone().try_into().unwrap();

                        let mut stream_map = stream_map.lock().await;
                        if let Some(state) = stream_map.get_mut(&request_id) {
                            state.received_messages.insert(stream_id);
                            let _ = state.sender.try_send(response);
                        } else {
                            let (tx, rx) = mpsc::channel(32);
                            let mut state = StreamState {
                                sender: tx,
                                received_messages: HashSet::new(),
                                total_expected: None,
                            };
                            state.received_messages.insert(stream_id);
                            let _ = state.sender.try_send(response);
                            stream_map.insert(request_id, state);

                            let mut response_map = response_map.lock().await;
                            if let Some(sender) = response_map.remove(&request_id) {
                                let stream = Box::new(rx);
                                let _ = sender.send(ClientResponseType::Stream(stream));
                            }
                        }
                        drop(stream_map);
                    }
                    _ => unreachable!(),
                }
            }
        });
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
        options: Self::Options,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        let client_id = Uuid::new_v4().to_string();
        let response_map = Arc::new(Mutex::new(HashMap::new()));

        let jetstream_context = async_nats::jetstream::new(options.client.clone());
        let reply_stream_name = format!("{name}_client_{client_id}");

        let reply_stream = jetstream_context
            .create_stream(NatsStreamConfig {
                name: reply_stream_name.clone(),
                no_ack: true,
                storage: async_nats::jetstream::stream::StorageType::Memory,
                ..Default::default()
            })
            .await
            .unwrap();

        let reply_stream_consumer = reply_stream
            .create_consumer(NatsConsumerConfig {
                name: Some(format!("{reply_stream_name}_consumer")),
                durable_name: None,
                ack_policy: async_nats::jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
            .unwrap();

        let stream_map = Arc::new(Mutex::new(HashMap::new()));

        // Spawn response handler
        Self::spawn_response_handler(
            reply_stream_consumer,
            response_map.clone(),
            stream_map.clone(),
        );

        Ok(Self {
            nats_client: options.client,
            nats_jetstream_context: jetstream_context,
            reply_stream_name,
            request_id_counter: Arc::new(AtomicUsize::new(0)),
            response_map,
            stream_map,
            stream_name: stream.name(),
        })
    }

    async fn request(
        &self,
        request: T,
    ) -> Result<ClientResponseType<X::ResponseType>, Self::Error> {
        let request_id = self.request_id_counter.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();

        // Insert sender into response map
        {
            let mut map = self.response_map.lock().await;
            map.insert(request_id, sender);
        }

        let mut headers = HeaderMap::new();
        headers.insert("reply-stream-name", self.reply_stream_name.clone().as_str());
        headers.insert("request-id", request_id.to_string().as_str());

        let bytes: Bytes = request.try_into().unwrap();

        self.nats_client
            .publish_with_headers(self.stream_name.clone(), headers, bytes)
            .await
            .unwrap();

        // Wait for response with cleanup
        if let Ok(Ok(response)) =
            tokio::time::timeout(std::time::Duration::from_secs(5), receiver).await
        {
            Ok(response)
        } else {
            // Clean up map on timeout/error
            let mut map = self.response_map.lock().await;
            map.remove(&request_id);
            drop(map);
            Err(Error::NoResponse)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::NatsServiceOptions;
    use crate::stream::{NatsStream, NatsStreamOptions};

    use std::time::Duration;

    use async_nats::ConnectOptions;
    use proven_messaging::service_responder::ServiceResponder;
    use proven_messaging::stream::Stream;
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
        type ResponseDeserializationError = serde_json::Error;
        type ResponseSerializationError = serde_json::Error;

        async fn handle<R>(
            &self,
            msg: TestMessage,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                TestMessage,
                serde_json::Error,
                serde_json::Error,
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
        {
            Ok(responder
                .reply(TestMessage {
                    content: format!("response: {}", msg.content),
                })
                .await)
        }
    }

    #[derive(Clone, Debug)]
    struct StreamingTestHandler;

    #[async_trait]
    impl ServiceHandler<TestMessage, serde_json::Error, serde_json::Error> for StreamingTestHandler {
        type Error = serde_json::Error;
        type ResponseType = TestMessage;
        type ResponseDeserializationError = serde_json::Error;
        type ResponseSerializationError = serde_json::Error;

        async fn handle<R>(
            &self,
            msg: TestMessage,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                TestMessage,
                serde_json::Error,
                serde_json::Error,
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
        {
            use futures::stream;

            let messages = vec![
                TestMessage {
                    content: format!("stream 1: {}", msg.content),
                },
                TestMessage {
                    content: format!("stream 2: {}", msg.content),
                },
                TestMessage {
                    content: format!("stream 3: {}", msg.content),
                },
            ];

            Ok(responder.stream(stream::iter(messages)).await)
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
            .client(
                "test_service",
                NatsClientOptions {
                    client: client.clone(),
                },
                TestHandler,
            )
            .await
            .expect("Failed to create client");

        // Send request and get response with timeout
        let request = TestMessage {
            content: "hello".to_string(),
        };

        if let ClientResponseType::Response(response) =
            timeout(Duration::from_secs(5), client.request(request))
                .await
                .expect("Request timed out")
                .expect("Failed to send request")
        {
            assert_eq!(
                response,
                TestMessage {
                    content: "response: hello".to_string()
                }
            );
        } else {
            panic!("Expected single response");
        }
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
            .client(
                "test_client",
                NatsClientOptions {
                    client: client.clone(),
                },
                TestHandler,
            )
            .await
            .expect("Failed to create client");

        // Send request without service running
        let request = TestMessage {
            content: "hello".to_string(),
        };

        let result = timeout(Duration::from_secs(1), client.request(request)).await;
        assert!(result.is_err(), "Expected timeout error");
    }

    #[tokio::test]
    async fn test_client_stream_response() {
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

        // Start service with streaming handler
        let _service = initialized_stream
            .clone()
            .start_service(
                "test_service",
                NatsServiceOptions {
                    client: client.clone(),
                    durable_name: None,
                    jetstream_context: async_nats::jetstream::new(client.clone()),
                },
                StreamingTestHandler,
            )
            .await
            .expect("Failed to start service");

        // Create client
        let client = initialized_stream
            .client(
                "test_service",
                NatsClientOptions {
                    client: client.clone(),
                },
                StreamingTestHandler,
            )
            .await
            .expect("Failed to create client");

        // Send request and collect stream responses
        let request = TestMessage {
            content: "hello".to_string(),
        };

        if let ClientResponseType::Stream(mut stream) =
            timeout(Duration::from_secs(5), client.request(request))
                .await
                .expect("Request timed out")
                .expect("Failed to send request")
        {
            let mut responses = Vec::new();
            while let Some(response) = stream.next().await {
                responses.push(response);
            }

            assert_eq!(responses.len(), 3);
            assert_eq!(
                responses,
                vec![
                    TestMessage {
                        content: "stream 1: hello".to_string()
                    },
                    TestMessage {
                        content: "stream 2: hello".to_string()
                    },
                    TestMessage {
                        content: "stream 3: hello".to_string()
                    },
                ]
            );
        } else {
            panic!("Expected stream response");
        }
    }
}
