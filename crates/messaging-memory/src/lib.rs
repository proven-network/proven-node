//! In-memory implementation of the messaging crate.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod gotham_state;

/// Clients send requests to services.
pub mod client;

/// Consumers are stateful views of streams.
pub mod consumer;

/// Services are special consumers that respond to requests.
pub mod service;

/// Service responders are responders for services.
pub mod service_responder;

/// Streams are persistent, ordered, and append-only sequences of messages.
pub mod stream;

/// Subjects are named channels for messages.
pub mod subject;

/// Subscribers consume messages from subjects.
pub mod subscription;

/// Subscription responders are responders for subscriptions.
pub mod subscription_responder;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use gotham_state::GothamState;
use tokio::sync::{broadcast, Mutex};

static GLOBAL_STATE: LazyLock<Mutex<GothamState>> =
    LazyLock::new(|| Mutex::new(GothamState::default()));

#[derive(Clone, Debug)]
struct ClientRequest<T> {
    client_id: String,
    request_id: String,
    payload: T,
    sequence_number: u64,
}

#[derive(Clone, Debug)]
struct ServiceResponse<R> {
    request_id: String,
    stream_id: Option<usize>,
    stream_end: Option<usize>,
    payload: R,
}

#[derive(Clone, Debug)]
struct GlobalState<T> {
    client_requests: Arc<Mutex<HashMap<String, broadcast::Sender<ClientRequest<T>>>>>,
    service_responses: Arc<Mutex<HashMap<String, broadcast::Sender<ServiceResponse<T>>>>>,
    subjects: Arc<Mutex<HashMap<String, broadcast::Sender<T>>>>,
}

impl<T> Default for GlobalState<T> {
    fn default() -> Self {
        Self {
            client_requests: Arc::new(Mutex::new(HashMap::new())),
            service_responses: Arc::new(Mutex::new(HashMap::new())),
            subjects: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::subject::*;

    use std::convert::Infallible;
    use std::error::Error as StdError;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use bytes::Bytes;
    use proven_messaging::subject::*;
    use proven_messaging::subscription_handler::*;
    use proven_messaging::subscription_responder::SubscriptionResponder;
    use serial_test::serial;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::{timeout, Duration};

    #[derive(Clone, Debug)]
    struct TestSubscriptionHandler {
        sender: mpsc::Sender<Bytes>,
    }

    #[derive(Debug, Clone)]
    pub struct TestSubscriptionHandlerError;

    impl std::fmt::Display for TestSubscriptionHandlerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MemorySubscriberHandlerError")
        }
    }

    impl StdError for TestSubscriptionHandlerError {}

    #[async_trait]
    impl SubscriptionHandler<Bytes, Infallible, Infallible> for TestSubscriptionHandler {
        type Error = TestSubscriptionHandlerError;

        type ResponseType = Bytes;
        type ResponseDeserializationError = Infallible;
        type ResponseSerializationError = Infallible;

        async fn handle<R>(
            &self,
            message: Bytes,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: SubscriptionResponder<Self::ResponseType, Infallible, Infallible>,
        {
            let _ = self
                .sender
                .send(message)
                .await
                .map_err(|_| TestSubscriptionHandlerError);

            Ok(responder.no_reply().await)
        }
    }

    fn setup_test_handler() -> (TestSubscriptionHandler, Receiver<Bytes>) {
        let (sender, receiver) = mpsc::channel(10);
        (TestSubscriptionHandler { sender }, receiver)
    }

    #[tokio::test]
    #[serial]
    async fn test_no_scope() {
        let subject = MemorySubject::new("test").unwrap();
        let (handler, mut receiver) = setup_test_handler();

        let _subscription = subject.subscribe(handler).await.unwrap();

        subject.publish(Bytes::from("message1")).await.unwrap();
        subject.publish(Bytes::from("message2")).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1")
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2")
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_single_scope() {
        let subject2 = MemorySubject2::new("test").unwrap();
        let subject1 = subject2.scope("scope1").scope("scope2");

        let (handler, mut receiver) = setup_test_handler();

        let _subscription = subject1.subscribe(handler).await.unwrap();

        subject1.publish(Bytes::from("message1")).await.unwrap();
        subject1.publish(Bytes::from("message2")).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1")
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2")
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_double_scope() {
        let subject3 = MemorySubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let (handler, mut receiver) = setup_test_handler();

        let _subscription = subject1.subscribe(handler).await.unwrap();

        subject1.publish(Bytes::from("message1")).await.unwrap();
        subject1.publish(Bytes::from("message2")).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1")
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2")
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_triple_scope() {
        let subject3 = MemorySubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope1");
        let subject1 = subject2.scope("scope2");
        let subject = subject1.scope("scope3");

        let (handler, mut receiver) = setup_test_handler();

        let _subscription = subject.subscribe(handler).await.unwrap();

        subject.publish(Bytes::from("message1")).await.unwrap();
        subject.publish(Bytes::from("message2")).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1")
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2")
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_wildcard_scope() {
        let subject3 = MemorySubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let wildcard_subject = subject3.any().any().any();

        let (handler, mut receiver) = setup_test_handler();

        let _subscription = wildcard_subject.subscribe(handler).await.unwrap();

        subject1.publish(Bytes::from("message1")).await.unwrap();
        subject1.publish(Bytes::from("message2")).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1")
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2")
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_greedy_wildcard_scope() {
        let subject3 = MemorySubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let greedy_wildcard_subject = subject3.all();

        let (handler, mut receiver) = setup_test_handler();

        let _subscription = greedy_wildcard_subject.subscribe(handler).await.unwrap();

        subject1.publish(Bytes::from("message1")).await.unwrap();
        subject1.publish(Bytes::from("message2")).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1")
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2")
        );
    }

    #[derive(Clone, Debug, PartialEq)]
    struct CustomType(i32);

    impl TryFrom<Bytes> for CustomType {
        type Error = Infallible;

        fn try_from(value: Bytes) -> Result<Self, Self::Error> {
            Ok(Self(i32::from_be_bytes(value.as_ref().try_into().unwrap())))
        }
    }

    impl TryInto<Bytes> for CustomType {
        type Error = Infallible;

        fn try_into(self) -> Result<Bytes, Self::Error> {
            Ok(Bytes::from(self.0.to_be_bytes().to_vec()))
        }
    }

    #[derive(Clone, Debug)]
    struct CustomHandler(mpsc::Sender<CustomType>);

    #[async_trait]
    impl SubscriptionHandler<CustomType, Infallible, Infallible> for CustomHandler {
        type Error = TestSubscriptionHandlerError;

        type ResponseType = CustomType;
        type ResponseDeserializationError = Infallible;
        type ResponseSerializationError = Infallible;

        async fn handle<R>(
            &self,
            message: CustomType,
            responder: R,
        ) -> Result<R::UsedResponder, Self::Error>
        where
            R: SubscriptionResponder<Self::ResponseType, Infallible, Infallible>,
        {
            let _ = self
                .0
                .send(message)
                .await
                .map_err(|_| TestSubscriptionHandlerError);

            Ok(responder.no_reply().await)
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_non_bytes() {
        let subject = MemorySubject::new("test").unwrap();

        let (sender, mut receiver) = mpsc::channel(10);
        let handler = CustomHandler(sender);

        let _subscription = subject.subscribe(handler).await.unwrap();

        subject.publish(CustomType(42)).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            CustomType(42)
        );
    }
}
