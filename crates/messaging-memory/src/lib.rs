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

/// Streams are persistent, ordered, and append-only sequences of messages.
pub mod stream;

/// Subjects are named channels for messages.
pub mod subject;

/// Subscribers consume messages from subjects.
pub mod subscription;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use gotham_state::GothamState;
use proven_messaging::Message;
use tokio::sync::{broadcast, Mutex};

static GLOBAL_STATE: LazyLock<Mutex<GothamState>> =
    LazyLock::new(|| Mutex::new(GothamState::default()));

#[derive(Clone, Debug)]
struct SubjectState<T> {
    subjects: Arc<Mutex<HashMap<String, broadcast::Sender<Message<T>>>>>,
}

impl<T> Default for SubjectState<T> {
    fn default() -> Self {
        Self {
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
    use proven_messaging::subscription::*;
    use proven_messaging::subscription_handler::*;
    use proven_messaging::Message;
    use serial_test::serial;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::{timeout, Duration};

    #[derive(Clone, Debug)]
    struct TestSubscriptionHandler {
        sender: mpsc::Sender<Message<Bytes>>,
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

        async fn handle(
            &self,
            message: Message<Bytes>,
        ) -> Result<Option<Message<Self::ResponseType>>, Self::Error> {
            println!("Handling data: {:?}", message);
            let _ = self
                .sender
                .send(message)
                .await
                .map_err(|_| TestSubscriptionHandlerError);

            Ok(None)
        }
    }

    fn setup_test_handler() -> (TestSubscriptionHandler, Receiver<Message<Bytes>>) {
        let (sender, receiver) = mpsc::channel(10);
        (TestSubscriptionHandler { sender }, receiver)
    }

    #[tokio::test]
    #[serial]
    async fn test_no_scope() {
        let subject = MemorySubject::new("test").unwrap();
        let (handler, mut receiver) = setup_test_handler();

        let _ = subject.subscribe(handler).await.unwrap();

        subject
            .publish(Bytes::from("message1").into())
            .await
            .unwrap();
        subject
            .publish(Bytes::from("message2").into())
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1").into()
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2").into()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_single_scope() {
        let subject2 = MemorySubject2::new("test").unwrap();
        let subject1 = subject2.scope("scope1").scope("scope2");

        let (handler, mut receiver) = setup_test_handler();

        let _ = subject1.subscribe(handler).await.unwrap();

        subject1
            .publish(Bytes::from("message1").into())
            .await
            .unwrap();
        subject1
            .publish(Bytes::from("message2").into())
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1").into()
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2").into()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_double_scope() {
        let subject3 = MemorySubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let (handler, mut receiver) = setup_test_handler();

        let _ = subject1.subscribe(handler).await.unwrap();

        subject1
            .publish(Bytes::from("message1").into())
            .await
            .unwrap();
        subject1
            .publish(Bytes::from("message2").into())
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1").into()
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2").into()
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

        let _ = subject.subscribe(handler).await.unwrap();

        subject
            .publish(Bytes::from("message1").into())
            .await
            .unwrap();
        subject
            .publish(Bytes::from("message2").into())
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1").into()
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2").into()
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

        let _ = wildcard_subject.subscribe(handler).await.unwrap();

        subject1
            .publish(Bytes::from("message1").into())
            .await
            .unwrap();
        subject1
            .publish(Bytes::from("message2").into())
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1").into()
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2").into()
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

        let _ = greedy_wildcard_subject.subscribe(handler).await.unwrap();

        subject1
            .publish(Bytes::from("message1").into())
            .await
            .unwrap();
        subject1
            .publish(Bytes::from("message2").into())
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1").into()
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2").into()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_last_message() {
        let subject = MemorySubject::new("test").unwrap();
        let (handler, mut receiver) = setup_test_handler();

        let subscriber = subject.subscribe(handler).await.unwrap();

        subject
            .publish(Bytes::from("message1").into())
            .await
            .unwrap();
        subject
            .publish(Bytes::from("message2").into())
            .await
            .unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message1").into()
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from("message2").into()
        );

        assert_eq!(
            subscriber.last_message().await,
            Some(Bytes::from("message2").into())
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
    struct CustomHandler(mpsc::Sender<Message<CustomType>>);

    #[async_trait]
    impl SubscriptionHandler<CustomType, Infallible, Infallible> for CustomHandler {
        type Error = TestSubscriptionHandlerError;

        type ResponseType = CustomType;

        async fn handle(
            &self,
            message: Message<CustomType>,
        ) -> Result<Option<Message<CustomType>>, Self::Error> {
            let _ = self
                .0
                .send(message)
                .await
                .map_err(|_| TestSubscriptionHandlerError);

            Ok(None)
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_non_bytes() {
        let subject = MemorySubject::new("test").unwrap();

        let (sender, mut receiver) = mpsc::channel(10);
        let handler = CustomHandler(sender);

        let _ = subject.subscribe(handler).await.unwrap();

        subject.publish(CustomType(42).into()).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            CustomType(42).into()
        );
    }
}
