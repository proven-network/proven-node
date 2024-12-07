//! In-memory implementation of the messaging crate.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod gotham_state;
/// Subjects can be published to and subscribed to (by passing a handler).
pub mod subject;
/// Subscribers are created by subscribing to a subject.
pub mod subscriber;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use gotham_state::GothamState;
use tokio::sync::{broadcast, Mutex};

static GLOBAL_STATE: LazyLock<Mutex<GothamState>> =
    LazyLock::new(|| Mutex::new(GothamState::default()));

#[derive(Clone, Debug)]
struct SubjectState<T> {
    subjects: Arc<Mutex<HashMap<String, broadcast::Sender<T>>>>,
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
    use super::subscriber::*;

    use std::error::Error as StdError;

    use async_trait::async_trait;
    use bytes::Bytes;
    use proven_messaging::*;
    use serial_test::serial;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::{timeout, Duration};

    #[derive(Clone, Debug)]
    struct TestHandler {
        sender: mpsc::Sender<Bytes>,
    }

    #[derive(Debug, Clone)]
    pub struct InMemorySubscriberHandlerError;

    impl std::fmt::Display for InMemorySubscriberHandlerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "InMemorySubscriberHandlerError")
        }
    }

    impl StdError for InMemorySubscriberHandlerError {}

    impl HandlerError for InMemorySubscriberHandlerError {}

    #[async_trait]
    impl Handler<Bytes> for TestHandler {
        type Error = InMemorySubscriberHandlerError;

        async fn handle(&self, data: Bytes) -> Result<(), Self::Error> {
            println!("Handling data: {:?}", data);
            self.sender
                .send(data)
                .await
                .map_err(|_| InMemorySubscriberHandlerError)
        }
    }

    fn setup_test_handler() -> (TestHandler, Receiver<Bytes>) {
        let (sender, receiver) = mpsc::channel(10);
        (TestHandler { sender }, receiver)
    }

    #[tokio::test]
    #[serial]
    async fn test_no_scope() {
        let subject = InMemoryPublishableSubject::new("test");
        let (handler, mut receiver) = setup_test_handler();

        let _: InMemorySubscriber<Bytes, TestHandler> = subject.subscribe(handler).await.unwrap();

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
        let subject2 = InMemoryPublishableSubject2::new("test");
        let subject1 = subject2.scope("scope1").scope("scope2");

        let (handler, mut receiver) = setup_test_handler();

        let _: InMemorySubscriber<Bytes, TestHandler> = subject1.subscribe(handler).await.unwrap();

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
        let subject3 = InMemoryPublishableSubject3::new("test");
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let (handler, mut receiver) = setup_test_handler();

        let _: InMemorySubscriber<Bytes, TestHandler> = subject1.subscribe(handler).await.unwrap();

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
        let subject3 = InMemoryPublishableSubject3::new("test");
        let subject2 = subject3.scope("scope1");
        let subject1 = subject2.scope("scope2");
        let subject = subject1.scope("scope3");

        let (handler, mut receiver) = setup_test_handler();

        let _: InMemorySubscriber<Bytes, TestHandler> = subject.subscribe(handler).await.unwrap();

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
        let subject3 = InMemoryPublishableSubject3::new("test");
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let wildcard_subject = subject3.any().any().any();

        let (handler, mut receiver) = setup_test_handler();

        let _: InMemorySubscriber<Bytes, TestHandler> =
            wildcard_subject.subscribe(handler).await.unwrap();

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
        let subject3 = InMemoryPublishableSubject3::new("test");
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let greedy_wildcard_subject = subject3.all();

        let (handler, mut receiver) = setup_test_handler();

        let _: InMemorySubscriber<Bytes, TestHandler> =
            greedy_wildcard_subject.subscribe(handler).await.unwrap();

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
    async fn test_last_message() {
        let subject = InMemoryPublishableSubject::new("test");
        let (handler, mut receiver) = setup_test_handler();

        let subscriber: InMemorySubscriber<Bytes, TestHandler> =
            subject.subscribe(handler).await.unwrap();

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

        assert_eq!(
            subscriber.last_message().await,
            Some(Bytes::from("message2"))
        );
    }

    #[derive(Clone, Debug, PartialEq)]
    struct CustomType(i32);

    #[derive(Clone)]
    struct CustomHandler(mpsc::Sender<CustomType>);

    #[async_trait]
    impl Handler<CustomType> for CustomHandler {
        type Error = InMemorySubscriberHandlerError;
        async fn handle(&self, data: CustomType) -> Result<(), Self::Error> {
            self.0
                .send(data)
                .await
                .map_err(|_| InMemorySubscriberHandlerError)
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_non_bytes() {
        let subject = InMemoryPublishableSubject::new("test");

        let (sender, mut receiver) = mpsc::channel(10);
        let handler = CustomHandler(sender);

        let _: InMemorySubscriber<CustomType, _> = subject.subscribe(handler).await.unwrap();

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
