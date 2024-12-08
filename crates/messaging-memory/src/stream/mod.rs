#![allow(dead_code)]

mod error;
mod subscription_handler;

use crate::subscription::{InMemorySubscriber, InMemorySubscriberOptions};
pub use error::Error;
use subscription_handler::StreamSubscriptionHandler;

use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::Stream;
use proven_messaging::subject::Subject;
use tokio::sync::{mpsc, Mutex};

/// An in-memory stream.
#[derive(Clone, Debug)]
pub struct MemoryStream<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    messages: Arc<Mutex<Vec<T>>>,
    name: String,
    subscriptions: Vec<InMemorySubscriber<StreamSubscriptionHandler<T>, T>>,
}

#[async_trait]
impl<T> Stream<T, Infallible, Infallible> for MemoryStream<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<T>;

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new<J, N>(stream_name: N, subjects: Vec<J>) -> Result<Self, Self::Error>
    where
        J: Subject<T, Infallible, Infallible>,
        N: Into<String> + Send,
    {
        let (sender, mut receiver) = mpsc::channel::<T>(100);

        let mut subscriptions = Vec::new();
        for subject in subjects {
            let handler = StreamSubscriptionHandler::new(sender.clone());
            let subscription = subject
                .subscribe(InMemorySubscriberOptions, handler)
                .await
                .map_err(|e| Error::Subscription(e))?;

            subscriptions.push(subscription);
        }

        let messages = Arc::new(Mutex::new(Vec::new()));
        let messages_clone = messages.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                messages_clone.lock().await.push(message);
            }
        });

        Ok(Self {
            messages,
            name: stream_name.into(),
            subscriptions,
        })
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: usize) -> Result<Option<T>, Self::Error> {
        Ok(self.messages.lock().await.get(seq).cloned())
    }

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<T>, Self::Error> {
        Ok(self.messages.lock().await.last().cloned())
    }

    /// Returns the name of the stream.
    async fn name(&self) -> String {
        self.name.clone()
    }

    /// Consumes the stream with the given consumer.
    async fn start_consumer<C>(&self, _consumer: C) -> Result<(), Self::Error>
    where
        C: Consumer<Self, T, Infallible, Infallible>,
    {
        unimplemented!()
    }

    /// Consumes the stream with the given service.
    async fn start_service<S>(&self, _service: S) -> Result<(), Self::Error>
    where
        S: Service<Self, T, Infallible, Infallible>,
    {
        unimplemented!()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use crate::subject::{InMemoryPublishableSubject, InMemorySubject};
    use proven_messaging::subject::PublishableSubject;

    #[tokio::test]
    async fn test_get_message() {
        let publishable_subject = InMemoryPublishableSubject::new("test_get_message").unwrap();
        let subject = InMemorySubject::new("test_get_message").unwrap();
        let stream: MemoryStream<String> = MemoryStream::new("test_stream", vec![subject])
            .await
            .unwrap();
        let message = "test_message".to_string();
        publishable_subject.publish(message.clone()).await.unwrap();

        // Wait for the message to be processed
        tokio::task::yield_now().await;

        assert_eq!(stream.get(0).await.unwrap(), Some(message));
    }

    #[tokio::test]
    async fn test_last_message() {
        let publishable_subject = InMemoryPublishableSubject::new("test_last_message").unwrap();
        let subject = InMemorySubject::new("test_last_message").unwrap();
        let stream: MemoryStream<String> = MemoryStream::new("test_last_message", vec![subject])
            .await
            .unwrap();
        let message1 = "test_message1".to_string();
        let message2 = "test_message2".to_string();
        publishable_subject.publish(message1).await.unwrap();
        publishable_subject.publish(message2.clone()).await.unwrap();

        // Wait for the messages to be processed
        tokio::task::yield_now().await;

        assert_eq!(stream.last_message().await.unwrap(), Some(message2));
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let subject = InMemorySubject::new("test_empty_stream").unwrap();
        let stream: MemoryStream<String> = MemoryStream::new("test_stream", vec![subject])
            .await
            .unwrap();
        assert_eq!(stream.get(0).await.unwrap(), None);
        assert_eq!(stream.last_message().await.unwrap(), None);
    }
}
