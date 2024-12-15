#![allow(clippy::type_complexity)]

mod error;
mod subscription_handler;

use crate::stream::InitializedMemoryStream;
use crate::subject::MemorySubject;
use crate::subscription::MemorySubscription;
pub use error::Error;
use subscription_handler::ClientSubscriptionHandler;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::{Client, ClientOptions, ClientResponseType};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;
use proven_messaging::subject::Subject;
use tokio::sync::{mpsc, Mutex};

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryClientOptions;
impl ClientOptions for MemoryClientOptions {}

/// A client for an in-memory service.
#[derive(Debug)]
pub struct MemoryClient<X, T, D, S>
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
    reply_receiver: Arc<Mutex<mpsc::Receiver<X::ResponseType>>>,
    reply_subject: MemorySubject<
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >,
    stream: <Self as Client<X, T, D, S>>::StreamType,
    subscription: MemorySubscription<
        ClientSubscriptionHandler<
            X::ResponseType,
            X::ResponseDeserializationError,
            X::ResponseSerializationError,
        >,
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >,
}

impl<X, T, D, S> Clone for MemoryClient<X, T, D, S>
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
            subscription: self.subscription.clone(),
        }
    }
}

#[async_trait]
impl<X, T, D, S> Client<X, T, D, S> for MemoryClient<X, T, D, S>
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

    type Options = MemoryClientOptions;

    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        let (sender, receiver) = mpsc::channel::<X::ResponseType>(100);

        let reply_handler = ClientSubscriptionHandler::new(sender);

        let reply_subject: MemorySubject<
            X::ResponseType,
            X::ResponseDeserializationError,
            X::ResponseSerializationError,
        > = MemorySubject::new(format!("{name}_reply")).unwrap();

        let subscription = reply_subject.subscribe(reply_handler).await.unwrap();

        Ok(Self {
            reply_receiver: Arc::new(Mutex::new(receiver)),
            reply_subject,
            stream,
            subscription,
        })
    }

    async fn request(
        &self,
        request: T,
    ) -> Result<ClientResponseType<X::ResponseType>, Self::Error> {
        let mut reply_receiver = self.reply_receiver.lock().await;

        self.stream.publish(request).await.unwrap();

        tokio::task::yield_now().await;

        let reply = reply_receiver.recv().await.unwrap();
        drop(reply_receiver);

        // TODO: Handle stream responses

        Ok(ClientResponseType::Response(reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::MemoryServiceOptions;
    use crate::stream::{MemoryStream, MemoryStreamOptions};

    use std::convert::Infallible;
    use std::time::Duration;

    use proven_messaging::service_responder::ServiceResponder;
    use proven_messaging::stream::Stream;
    use tokio::time::timeout;

    #[derive(Clone, Debug)]
    struct TestHandler;

    #[async_trait]
    impl ServiceHandler<Bytes, Infallible, Infallible> for TestHandler {
        type Error = Infallible;
        type ResponseType = Bytes;
        type ResponseDeserializationError = Infallible;
        type ResponseSerializationError = Infallible;

        async fn handle<R>(&self, msg: Bytes, responder: R) -> Result<R::UsedResponder, Self::Error>
        where
            R: ServiceResponder<
                Bytes,
                Infallible,
                Infallible,
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
        {
            Ok(responder
                .reply(Bytes::from(format!(
                    "response: {}",
                    String::from_utf8(msg.to_vec()).unwrap()
                )))
                .await)
        }
    }

    #[tokio::test]
    async fn test_client_request_response() {
        // Create stream
        let stream = MemoryStream::<Bytes, Infallible, Infallible>::new(
            "test_client_stream",
            MemoryStreamOptions {},
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Start service
        let _service = initialized_stream
            .clone()
            .start_service("test_service", MemoryServiceOptions {}, TestHandler)
            .await
            .expect("Failed to start service");

        // Create client
        let client = initialized_stream
            .client("test_service", MemoryClientOptions {}, TestHandler)
            .await
            .expect("Failed to create client");

        // Send request and get response with timeout
        let request = Bytes::from("hello");

        if let ClientResponseType::Response(response) =
            timeout(Duration::from_secs(5), client.request(request))
                .await
                .expect("Request timed out")
                .expect("Failed to send request")
        {
            assert_eq!(response, Bytes::from("response: hello"));
        } else {
            panic!("Expected single response");
        }
    }

    #[tokio::test]
    async fn test_client_request_timeout() {
        // Create stream without service
        let stream = MemoryStream::<Bytes, Infallible, Infallible>::new(
            "test_client_timeout",
            MemoryStreamOptions {},
        );

        let initialized_stream = stream.init().await.expect("Failed to initialize stream");

        // Create client
        let client = initialized_stream
            .client("test_client", MemoryClientOptions {}, TestHandler)
            .await
            .expect("Failed to create client");

        // Send request without service running
        let request = Bytes::from("hello");

        let result = timeout(Duration::from_secs(1), client.request(request)).await;
        assert!(result.is_err(), "Expected timeout error");
    }
}
