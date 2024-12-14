mod error;
mod subscription_handler;

use crate::client::MemoryClient;
use crate::consumer::{MemoryConsumer, MemoryConsumerOptions};
use crate::service::{MemoryService, MemoryServiceOptions};
use crate::subject::MemoryUnpublishableSubject;
use crate::subscription::MemorySubscription;
pub use error::Error;
use proven_messaging::service::Service;
use subscription_handler::StreamSubscriptionHandler;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::Client;
use proven_messaging::consumer::Consumer;
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::{
    InitializedStream, Stream, Stream1, Stream2, Stream3, StreamOptions,
};
use proven_messaging::subject::Subject;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

type InternalSubscription<T, D, S> =
    MemorySubscription<StreamSubscriptionHandler<T, D, S>, T, D, S>;

/// Options for the in-memory stream (there are none).
#[derive(Clone, Debug, Default)]
pub struct MemoryStreamOptions;
impl StreamOptions for MemoryStreamOptions {}

/// An in-memory stream.
#[derive(Debug)]
pub struct InitializedMemoryStream<T, D, S>
where
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
    consumer_channels: Arc<Mutex<Vec<mpsc::Sender<T>>>>,
    messages: Arc<Mutex<Vec<Option<T>>>>,
    name: String,
    subscriptions: Vec<InternalSubscription<T, D, S>>,
    _marker: PhantomData<(D, S)>,
}

impl<T, D, S> Clone for InitializedMemoryStream<T, D, S>
where
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
            consumer_channels: self.consumer_channels.clone(),
            messages: self.messages.clone(),
            name: self.name.clone(),
            subscriptions: self.subscriptions.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> InitializedMemoryStream<T, D, S>
where
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
    /// Returns a stream of messages from the beginning.
    pub async fn messages(&self) -> ReceiverStream<T> {
        let messages = self.messages.lock().await.clone();
        let (sender, receiver) = mpsc::channel::<T>(100);

        // First send all existing messages, filtering out None values
        tokio::spawn({
            let sender = sender.clone();
            async move {
                for message in messages.into_iter().flatten() {
                    if sender.send(message).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Add sender to active consumers
        self.consumer_channels.lock().await.push(sender);

        ReceiverStream::new(receiver)
    }
}

#[async_trait]
impl<T, D, S> InitializedStream<T, D, S> for InitializedMemoryStream<T, D, S>
where
    Self: Clone + Send + Sync + 'static,
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

    type Options = MemoryStreamOptions;

    type Client<X>
        = MemoryClient<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    type Consumer<X>
        = MemoryConsumer<X, T, D, S>
    where
        X: ConsumerHandler<T, D, S>;

    type Service<X>
        = MemoryService<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    type Subject = MemoryUnpublishableSubject<T, D, S>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N, _options: MemoryStreamOptions) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
    {
        Ok(Self {
            consumer_channels: Arc::new(Mutex::new(Vec::new())),
            messages: Arc::new(Mutex::new(Vec::new())),
            name: stream_name.into(),
            subscriptions: Vec::new(),
            _marker: PhantomData,
        })
    }

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N, J>(
        stream_name: N,
        _options: Self::Options,
        subjects: Vec<J>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::Subject> + Clone + Send,
    {
        let (sender, mut receiver) = mpsc::channel::<T>(100);

        let mut subscriptions = Vec::new();
        for subject in subjects {
            let handler = StreamSubscriptionHandler::new(sender.clone());
            subscriptions.push(
                subject
                    .into()
                    .subscribe(handler)
                    .await
                    .map_err(Error::Subject)?,
            );
        }

        let messages = Arc::new(Mutex::new(Vec::new()));
        let messages_clone = messages.clone();
        let consumer_channels = Arc::new(Mutex::new(Vec::<mpsc::Sender<T>>::new()));
        let consumer_channels_clone = consumer_channels.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                messages_clone.lock().await.push(Some(message.clone()));

                // Broadcast the message to all consumers
                let mut channels = consumer_channels_clone.lock().await;
                channels.retain_mut(|sender| {
                    let message = message.clone();
                    sender.try_send(message).is_ok()
                });
            }
        });

        Ok(Self {
            consumer_channels,
            messages,
            name: stream_name.into(),
            subscriptions,
            _marker: PhantomData,
        })
    }

    async fn client<N, X>(
        &self,
        service_name: N,
        options: <Self::Client<X> as Client<X, T, D, S>>::Options,
        handler: X,
    ) -> Result<Self::Client<X>, <Self::Client<X> as Client<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        let client = MemoryClient::new(
            format!("{}_{}", self.name(), service_name.into()),
            self.clone(),
            options,
            handler,
        )
        .await?;

        Ok(client)
    }

    /// Deletes the message with the given sequence number.
    async fn del(&self, seq: u64) -> Result<(), Self::Error> {
        // TODO: Add error handling for sequence number conversion
        let seq_usize: usize = seq.try_into().unwrap();

        let mut messages = self.messages.lock().await;
        if seq_usize >= messages.len() {
            return Err(Error::InvalidSeq(seq_usize, messages.len()));
        }

        messages[seq_usize] = None;
        drop(messages);

        Ok(())
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<T>, Self::Error> {
        // TODO: Add error handling for sequence number conversion
        let seq_usize: usize = seq.try_into().unwrap();

        let messages = self.messages.lock().await;
        if seq_usize >= messages.len() {
            return Err(Error::InvalidSeq(seq_usize, messages.len()));
        }

        Ok(messages.get(seq_usize).and_then(Clone::clone))
    }

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<T>, Self::Error> {
        Ok(self
            .messages
            .lock()
            .await
            .iter()
            .rev()
            .find_map(Clone::clone))
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        Ok(self.messages.lock().await.len().try_into().unwrap())
    }

    /// Returns the name of the stream.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: T) -> Result<u64, Self::Error> {
        debug!("Publishing message to {}: {:?}", self.name(), message);

        let seq_usize = {
            let mut messages = self.messages.lock().await;
            let seq = messages.len();
            messages.push(Some(message.clone()));
            seq
        };

        // Broadcast the message to all consumers
        let mut channels = self.consumer_channels.lock().await;
        channels.retain_mut(|sender| {
            let message = message.clone();
            sender.try_send(message).is_ok()
        });
        drop(channels);

        // TODO: Add error handling for sequence number conversion
        let seq: u64 = seq_usize.try_into().unwrap();

        Ok(seq)
    }

    /// Consumes the stream with the given consumer.
    async fn start_consumer<N, X>(
        &self,
        consumer_name: N,
        options: MemoryConsumerOptions,
        handler: X,
    ) -> Result<Self::Consumer<X>, <Self::Consumer<X> as Consumer<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<T, D, S>,
    {
        let consumer = MemoryConsumer::new(
            format!("{}_{}", self.name(), consumer_name.into()),
            self.clone(),
            options,
            handler,
        )
        .await?;

        Ok(consumer)
    }

    /// Consumes the stream with the given service.
    async fn start_service<N, X>(
        &self,
        service_name: N,
        options: MemoryServiceOptions,
        handler: X,
    ) -> Result<Self::Service<X>, <Self::Service<X> as Service<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        let service = MemoryService::new(
            format!("{}_{}", self.name(), service_name.into()),
            self.clone(),
            options,
            handler,
        )
        .await?;

        Ok(service)
    }
}

/// All scopes applied and can initialize.
#[derive(Debug)]
pub struct MemoryStream<T, D, S>
where
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
    full_name: String,
    options: MemoryStreamOptions,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for MemoryStream<T, D, S>
where
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
            full_name: self.full_name.clone(),
            options: self.options.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> Stream<T, D, S> for MemoryStream<T, D, S>
where
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
    type Options = MemoryStreamOptions;

    /// The initialized stream type.
    type Initialized = InitializedMemoryStream<T, D, S>;

    type Subject = MemoryUnpublishableSubject<T, D, S>;

    fn new<N>(stream_name: N, options: MemoryStreamOptions) -> Self
    where
        N: Clone + Into<String> + Send,
    {
        Self {
            full_name: stream_name.into(),
            options,
            _marker: PhantomData,
        }
    }

    async fn init(
        &self,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error> {
        let stream =
            InitializedMemoryStream::<T, D, S>::new(self.full_name.clone(), self.options.clone())
                .await?;
        Ok(stream)
    }

    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error>
    where
        J: Into<Self::Subject> + Clone + Send,
    {
        let stream = InitializedMemoryStream::<T, D, S>::new_with_subjects(
            self.full_name.clone(),
            self.options.clone(),
            subjects,
        )
        .await?;
        Ok(stream)
    }
}

macro_rules! impl_scoped_stream {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Debug)]
            pub struct [< ScopedMemoryStream $index >]<T, D, S>
            where
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
                full_name: String,
                options: MemoryStreamOptions,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< ScopedMemoryStream $index >]<T, D, S>
            where
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
                        full_name: self.full_name.clone(),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> [< ScopedMemoryStream $index >]<T, D, S>
            where
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
                /// Creates a new `[< MemoryStream $index >]`.
                #[must_use]
                pub fn new<K>(stream_name: K, options: MemoryStreamOptions) -> Self
                where
                    K: Clone + Into<String> + Send,
                {
                    Self {
                        full_name: stream_name.into(),
                        options,
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> [< Stream $index >]<T, D, S> for [< ScopedMemoryStream $index >]<T, D, S>
            where
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
                type Options = MemoryStreamOptions;

                type Scoped = $parent<T, D, S>;

                fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> $parent<T, D, S> {
                    $parent::<T, D, S> {
                        full_name: format!("{}_{}", self.full_name, scope.into()),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}

impl_scoped_stream!(
    1,
    MemoryStream,
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
    use crate::subject::MemorySubject;

    use std::convert::Infallible;
    use std::error::Error as StdError;

    use bytes::Bytes;
    use proven_messaging::subject::PublishableSubject;

    #[tokio::test]
    async fn test_get_message() {
        let publishable_subject = MemorySubject::new("test_get_message").unwrap();
        let subject = MemorySubject::new("test_get_message").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_stream",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();
        let message = Bytes::from("test_message");
        publishable_subject.publish(message.clone()).await.unwrap();

        // Wait for the message to be processed
        tokio::task::yield_now().await;

        assert_eq!(stream.get(0).await.unwrap(), Some(message));
    }

    #[tokio::test]
    async fn test_last_message() {
        let publishable_subject = MemorySubject::new("test_last_message").unwrap();
        let subject = MemorySubject::new("test_last_message").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_last_message",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();
        let message1 = Bytes::from("test_message1");
        let message2 = Bytes::from("test_message2");
        publishable_subject.publish(message1).await.unwrap();
        publishable_subject.publish(message2.clone()).await.unwrap();

        // Wait for the messages to be processed
        tokio::task::yield_now().await;

        assert_eq!(stream.last_message().await.unwrap(), Some(message2));
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let subject: MemorySubject<Bytes, Infallible, Infallible> =
            MemorySubject::new("test_empty_stream").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_stream",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();

        assert!(stream.get(0).await.is_err());
        assert_eq!(stream.last_message().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_scoped_stream() {
        let stream1: ScopedMemoryStream1<Bytes, Infallible, Infallible> =
            ScopedMemoryStream1::new("test_scoped_stream", MemoryStreamOptions);
        let scoped_stream = stream1.scope("scope1");

        let subject = MemorySubject::new("test_scoped").unwrap();
        let stream = scoped_stream
            .init_with_subjects(vec![subject])
            .await
            .unwrap();

        assert_eq!(stream.name(), "test_scoped_stream_scope1");
    }

    #[tokio::test]
    async fn test_nested_scoped_stream() {
        let stream2: ScopedMemoryStream2<Bytes, Infallible, Infallible> =
            ScopedMemoryStream2::new("test_nested_scoped_stream", MemoryStreamOptions);
        let scoped_stream = stream2.scope("scope1").scope("scope2");

        let subject = MemorySubject::new("test_nested_scoped").unwrap();
        let stream = scoped_stream
            .init_with_subjects(vec![subject])
            .await
            .unwrap();

        assert_eq!(stream.name(), "test_nested_scoped_stream_scope1_scope2");
    }

    #[tokio::test]
    async fn test_direct_publish() {
        let subject = MemorySubject::new("test_direct_publish").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_stream",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();

        let message1 = Bytes::from("test_message1");
        let message2 = Bytes::from("test_message2");

        let seq1 = stream.publish(message1.clone()).await.unwrap();
        let seq2 = stream.publish(message2.clone()).await.unwrap();

        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(stream.get(0).await.unwrap(), Some(message1));
        assert_eq!(stream.get(1).await.unwrap(), Some(message2));
    }

    #[tokio::test]
    async fn test_start_consumer() {
        use async_trait::async_trait;
        use proven_messaging::consumer_handler::ConsumerHandler;

        #[derive(Clone, Debug)]
        struct TestHandler {
            received_messages: Arc<Mutex<Vec<Bytes>>>,
        }

        #[derive(Clone, Debug)]
        struct TestHandlerError;

        impl std::fmt::Display for TestHandlerError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "MemorySubscriberHandlerError")
            }
        }

        impl StdError for TestHandlerError {}

        #[async_trait]
        impl ConsumerHandler<Bytes, Infallible, Infallible> for TestHandler {
            type Error = TestHandlerError;

            async fn handle(&self, message: Bytes) -> Result<(), Self::Error> {
                self.received_messages.lock().await.push(message);
                Ok(())
            }
        }

        let publishable_subject = MemorySubject::new("test_start_consumer").unwrap();
        let subject = MemorySubject::new("test_start_consumer").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_stream",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();

        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let handler = TestHandler {
            received_messages: received_messages.clone(),
        };

        let prestart_subject_message = Bytes::from("prestart_subject_message");
        publishable_subject
            .publish(prestart_subject_message.clone())
            .await
            .unwrap();

        tokio::task::yield_now().await;

        let prestart_direct_message = Bytes::from("prestart_direct_message");
        stream
            .publish(prestart_direct_message.clone())
            .await
            .unwrap();

        // Wait for the message to be processed
        tokio::task::yield_now().await;

        let _consumer = stream
            .start_consumer("test", MemoryConsumerOptions, handler)
            .await
            .unwrap();

        // Wait for the consumer to start
        tokio::task::yield_now().await;

        let poststart_subject_message = Bytes::from("poststart_subject_message");
        publishable_subject
            .publish(poststart_subject_message.clone())
            .await
            .unwrap();

        tokio::task::yield_now().await;

        let poststart_direct_message = Bytes::from("poststart_direct_message");
        stream
            .publish(poststart_direct_message.clone())
            .await
            .unwrap();

        // Wait for the message to be processed
        tokio::task::yield_now().await;

        let received_messages = received_messages.lock().await;
        assert_eq!(received_messages.len(), 4);
        assert_eq!(received_messages[0], prestart_subject_message);
        assert_eq!(received_messages[1], prestart_direct_message);
        assert_eq!(received_messages[2], poststart_subject_message);
        assert_eq!(received_messages[3], poststart_direct_message);
        drop(received_messages);
    }

    #[tokio::test]
    async fn test_delete_message() {
        let subject = MemorySubject::new("test_delete").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_stream",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();

        let message1 = Bytes::from("test_message1");
        let message2 = Bytes::from("test_message2");
        let message3 = Bytes::from("test_message3");

        stream.publish(message1.clone()).await.unwrap();
        stream.publish(message2.clone()).await.unwrap();
        stream.publish(message3.clone()).await.unwrap();

        // Delete middle message
        stream.del(1).await.unwrap();

        // First and last messages should still be accessible
        assert_eq!(stream.get(0).await.unwrap(), Some(message1));
        assert_eq!(stream.get(1).await.unwrap(), None);
        assert_eq!(stream.get(2).await.unwrap(), Some(message3.clone()));

        // Attempting to delete an invalid sequence should fail
        assert!(stream.del(99).await.is_err());

        // Last message should still be message3
        assert_eq!(stream.last_message().await.unwrap(), Some(message3));
    }
}
