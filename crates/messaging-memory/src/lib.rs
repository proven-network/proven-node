//! In-memory implementation of the messaging crate.
#![feature(associated_type_defaults)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod gotham_state;

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
use tokio::sync::{broadcast, Mutex};

static GLOBAL_STATE: LazyLock<Mutex<GothamState>> =
    LazyLock::new(|| Mutex::new(GothamState::default()));

type Headers = HashMap<String, String>;
type OptionalHeaders = Option<Headers>;
type MessageWithOptionalHeaders<T> = (T, OptionalHeaders);

#[derive(Clone, Debug)]
struct SubjectState<T> {
    subjects: Arc<Mutex<HashMap<String, broadcast::Sender<MessageWithOptionalHeaders<T>>>>>,
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
    use super::subscription::*;

    use std::collections::HashMap;
    use std::error::Error as StdError;

    use async_trait::async_trait;
    use bytes::Bytes;
    use proven_messaging::subject::*;
    use proven_messaging::subscription::*;
    use proven_messaging::subscription_handler::*;
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

    impl SubscriptionHandlerError for TestSubscriptionHandlerError {}

    #[async_trait]
    impl SubscriptionHandler for TestSubscriptionHandler {
        type Error = TestSubscriptionHandlerError;

        async fn handle(
            &self,
            _subject: String,
            data: Bytes,
            _headers_opt: Option<HashMap<String, String>>,
        ) -> Result<(), Self::Error> {
            println!("Handling data: {:?}", data);
            self.sender
                .send(data)
                .await
                .map_err(|_| TestSubscriptionHandlerError)
        }
    }

    fn setup_test_handler() -> (TestSubscriptionHandler, Receiver<Bytes>) {
        let (sender, receiver) = mpsc::channel(10);
        (TestSubscriptionHandler { sender }, receiver)
    }

    #[tokio::test]
    #[serial]
    async fn test_no_scope() {
        let subject = MemoryPublishableSubject::new("test").unwrap();
        let (handler, mut receiver) = setup_test_handler();

        let _: MemorySubscriber<TestSubscriptionHandler> = subject
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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
        let subject2 = MemoryPublishableSubject2::new("test").unwrap();
        let subject1 = subject2.scope("scope1").scope("scope2");

        let (handler, mut receiver) = setup_test_handler();

        let _: MemorySubscriber<TestSubscriptionHandler> = subject1
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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
        let subject3 = MemoryPublishableSubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let (handler, mut receiver) = setup_test_handler();

        let _: MemorySubscriber<TestSubscriptionHandler> = subject1
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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
        let subject3 = MemoryPublishableSubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope1");
        let subject1 = subject2.scope("scope2");
        let subject = subject1.scope("scope3");

        let (handler, mut receiver) = setup_test_handler();

        let _: MemorySubscriber<TestSubscriptionHandler> = subject
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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
        let subject3 = MemoryPublishableSubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let wildcard_subject = subject3.any().any().any();

        let (handler, mut receiver) = setup_test_handler();

        let _: MemorySubscriber<TestSubscriptionHandler> = wildcard_subject
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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
        let subject3 = MemoryPublishableSubject3::new("test").unwrap();
        let subject2 = subject3.scope("scope2");
        let subject1 = subject2.scope("scope1").scope("scope3");

        let greedy_wildcard_subject = subject3.all();

        let (handler, mut receiver) = setup_test_handler();

        let _: MemorySubscriber<TestSubscriptionHandler> = greedy_wildcard_subject
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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
        let subject = MemoryPublishableSubject::new("test").unwrap();
        let (handler, mut receiver) = setup_test_handler();

        let subscriber: MemorySubscriber<TestSubscriptionHandler> = subject
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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

    #[derive(Clone, Debug)]
    struct CustomHandler(mpsc::Sender<CustomType>);

    #[async_trait]
    impl SubscriptionHandler<CustomType> for CustomHandler {
        type Error = TestSubscriptionHandlerError;

        async fn handle(
            &self,
            _subject: String,
            data: CustomType,
            _headers_opt: Option<HashMap<String, String>>,
        ) -> Result<(), Self::Error> {
            self.0
                .send(data)
                .await
                .map_err(|_| TestSubscriptionHandlerError)
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_non_bytes() {
        let subject = MemoryPublishableSubject::new("test").unwrap();

        let (sender, mut receiver) = mpsc::channel(10);
        let handler = CustomHandler(sender);

        let _: MemorySubscriber<_, CustomType> = subject
            .subscribe(MemorySubscriberOptions, handler)
            .await
            .unwrap();

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
