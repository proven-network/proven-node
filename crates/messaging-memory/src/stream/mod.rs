#![allow(dead_code)]

mod error;
mod subscription_handler;

use crate::consumer::{MemoryConsumer, MemoryConsumerOptions};
use crate::subject::MemorySubject;
pub use error::Error;
use proven_messaging::Message;
use subscription_handler::StreamSubscriptionHandler;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::consumer::Consumer;
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::service::Service;
use proven_messaging::stream::{ScopedStream, ScopedStream1, ScopedStream2, ScopedStream3, Stream};
use proven_messaging::subject::Subject;
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

/// An in-memory stream.
#[derive(Clone, Debug)]
pub struct MemoryStream<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    messages: Arc<Mutex<Vec<Message<T>>>>,
    name: String,
}

#[async_trait]
impl<T> Stream<T> for MemoryStream<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<T>;

    type SubjectType = MemorySubject<T>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N) -> Result<Self, Self::Error>
    where
        N: Into<String> + Send,
    {
        Ok(Self {
            messages: Arc::new(Mutex::new(Vec::new())),
            name: stream_name.into(),
        })
    }

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N>(
        stream_name: N,
        subjects: Vec<MemorySubject<T>>,
    ) -> Result<Self, Self::Error>
    where
        N: Into<String> + Send,
    {
        let (sender, mut receiver) = mpsc::channel::<Message<T>>(100);

        for subject in subjects {
            let handler = StreamSubscriptionHandler::new(sender.clone());
            subject
                .subscribe(handler)
                .await
                .map_err(|e| Error::Subject(e))?;
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
        })
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: usize) -> Result<Option<Message<T>>, Self::Error> {
        Ok(self.messages.lock().await.get(seq).cloned())
    }

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<Message<T>>, Self::Error> {
        Ok(self.messages.lock().await.last().cloned())
    }

    /// Returns the name of the stream.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: Message<T>) -> Result<usize, Self::Error> {
        debug!("Publishing message to {}: {:?}", self.name(), message);

        let seq = {
            let mut messages = self.messages.lock().await;
            let seq = messages.len();
            messages.push(message);
            seq
        };

        Ok(seq)
    }

    /// Consumes the stream with the given consumer.
    async fn start_consumer<X>(&self, _handler: X) -> Result<MemoryConsumer<X, T>, Self::Error>
    where
        X: ConsumerHandler<T>,
    {
        let consumer = MemoryConsumer::new(self.clone(), MemoryConsumerOptions, _handler).await?;

        Ok(consumer)
    }

    /// Consumes the stream with the given service.
    async fn start_service<S>(&self, _service: S) -> Result<(), Self::Error>
    where
        S: Service<Self, T>,
    {
        unimplemented!()
    }
}

/// All scopes applied and can initialize.
#[derive(Clone, Debug, Default)]
pub struct ScopedMemoryStream<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    prefix: Option<String>,
    _marker: PhantomData<T>,
}

impl<T> ScopedMemoryStream<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    const fn with_scope(prefix: String) -> Self {
        Self {
            prefix: Some(prefix),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T> ScopedStream<T> for ScopedMemoryStream<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<T>;

    type StreamType = MemoryStream<T>;

    type SubjectType = MemorySubject<T>;

    async fn init(&self) -> Result<Self::StreamType, Self::Error> {
        let stream = MemoryStream::new(self.prefix.clone().unwrap()).await?;
        Ok(stream)
    }

    async fn init_with_subjects(
        &self,
        subjects: Vec<Self::SubjectType>,
    ) -> Result<Self::StreamType, Self::Error> {
        let stream =
            MemoryStream::new_with_subjects(self.prefix.clone().unwrap(), subjects).await?;
        Ok(stream)
    }
}

macro_rules! impl_scoped_stream {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Clone, Debug, Default)]
            pub struct [< ScopedMemoryStream $index >]<T>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                prefix: Option<String>,
                _marker: PhantomData<T>,
            }

            impl<T> [< ScopedMemoryStream $index >]<T>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                /// Creates a new `[< MemoryStream $index >]`.
                #[must_use]
                pub const fn new() -> Self {
                    Self {
                        prefix: None,
                        _marker: PhantomData,
                    }
                }

                const fn with_scope(prefix: String) -> Self {
                    Self {
                        prefix: Some(prefix),
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T> [< ScopedStream $index >]<T> for [< ScopedMemoryStream $index >]<T>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                type Error = Error<T>;

                type Scoped = $parent<T>;

                fn scope<S: Into<String> + Send>(&self, scope: S) -> $parent<T> {
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.into()),
                        None => scope.into(),
                    };
                    $parent::<T>::with_scope(new_scope)
                }
            }
        }
    };
}
impl_scoped_stream!(
    1,
    ScopedMemoryStream,
    Stream1,
    "A double-scoped in-memory stream."
);
impl_scoped_stream!(
    2,
    ScopedMemoryStream1,
    Stream1,
    "A double-scoped in-memory stream."
);
impl_scoped_stream!(
    3,
    ScopedMemoryStream2,
    Stream2,
    "A triple-scoped in-memory stream."
);

#[cfg(test)]
mod tests {
    use super::*;

    use crate::subject::{MemoryPublishableSubject, MemorySubject};
    use proven_messaging::subject::PublishableSubject;

    #[tokio::test]
    async fn test_get_message() {
        let publishable_subject = MemoryPublishableSubject::new("test_get_message").unwrap();
        let subject = MemorySubject::new("test_get_message").unwrap();
        let stream: MemoryStream<String> =
            MemoryStream::new_with_subjects("test_stream", vec![subject])
                .await
                .unwrap();
        let message = "test_message".to_string();
        publishable_subject
            .publish(message.clone().into())
            .await
            .unwrap();

        // Wait for the message to be processed
        tokio::task::yield_now().await;

        assert_eq!(stream.get(0).await.unwrap(), Some(message.into()));
    }

    #[tokio::test]
    async fn test_last_message() {
        let publishable_subject = MemoryPublishableSubject::new("test_last_message").unwrap();
        let subject = MemorySubject::new("test_last_message").unwrap();
        let stream: MemoryStream<String> =
            MemoryStream::new_with_subjects("test_last_message", vec![subject])
                .await
                .unwrap();
        let message1 = "test_message1".to_string();
        let message2 = "test_message2".to_string();
        publishable_subject.publish(message1.into()).await.unwrap();
        publishable_subject
            .publish(message2.clone().into())
            .await
            .unwrap();

        // Wait for the messages to be processed
        tokio::task::yield_now().await;

        assert_eq!(stream.last_message().await.unwrap(), Some(message2.into()));
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let subject = MemorySubject::new("test_empty_stream").unwrap();
        let stream: MemoryStream<String> =
            MemoryStream::new_with_subjects("test_stream", vec![subject])
                .await
                .unwrap();
        assert_eq!(stream.get(0).await.unwrap(), None);
        assert_eq!(stream.last_message().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_scoped_stream() {
        let stream1: ScopedMemoryStream1<String> = ScopedMemoryStream1::new();
        let scoped_stream = stream1.scope("scope1");

        let subject = MemorySubject::new("test_scoped").unwrap();
        let stream = scoped_stream
            .init_with_subjects(vec![subject])
            .await
            .unwrap();

        assert_eq!(stream.name(), "scope1");
    }

    #[tokio::test]
    async fn test_nested_scoped_stream() {
        let stream2: ScopedMemoryStream2<String> = ScopedMemoryStream2::new();
        let scoped_stream = stream2.scope("scope1").scope("scope2");

        let subject = MemorySubject::new("test_nested_scoped").unwrap();
        let stream = scoped_stream
            .init_with_subjects(vec![subject])
            .await
            .unwrap();

        assert_eq!(stream.name(), "scope1:scope2");
    }

    #[tokio::test]
    async fn test_direct_publish() {
        let subject = MemorySubject::new("test_direct_publish").unwrap();
        let stream: MemoryStream<String> =
            MemoryStream::new_with_subjects("test_stream", vec![subject])
                .await
                .unwrap();

        let message1 = "test_message1".to_string();
        let message2 = "test_message2".to_string();

        let seq1 = stream.publish(message1.clone().into()).await.unwrap();
        let seq2 = stream.publish(message2.clone().into()).await.unwrap();

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(stream.get(0).await.unwrap(), Some(message1.into()));
        assert_eq!(stream.get(1).await.unwrap(), Some(message2.into()));
    }
}
