mod error;
mod subscription_handler;

use crate::GLOBAL_STATE;
use crate::client::MemoryClient;
use crate::consumer::{MemoryConsumer, MemoryConsumerOptions};
use crate::service::{MemoryService, MemoryServiceOptions};
use crate::subject::MemoryUnpublishableSubject;
use crate::subscription::MemorySubscription;
pub use error::Error;
use proven_messaging::service::Service;
use subscription_handler::StreamSubscriptionHandler;

use std::collections::HashMap;
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
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;

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
    pub async fn message_stream(&self) -> ReceiverStream<T> {
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
    ) -> Result<Self::Client<X>, <Self::Client<X> as Client<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        let client = MemoryClient::new(
            format!("{}_{}", self.name(), service_name.into()),
            self.clone(),
            options,
        )
        .await?;

        Ok(client)
    }

    /// Consumes the stream with the given consumer.
    async fn consumer<N, X>(
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

    /// Deletes the message with the given sequence number.
    async fn delete(&self, seq: u64) -> Result<(), Self::Error> {
        // TODO: Add error handling for sequence number conversion
        let seq_usize: usize = seq.try_into().unwrap();

        let mut messages = self.messages.lock().await;

        if seq_usize > messages.len() {
            return Err(Error::InvalidSeq(seq_usize, messages.len()));
        }

        messages[seq_usize - 1] = None;
        drop(messages);

        Ok(())
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<T>, Self::Error> {
        // TODO: Add error handling for sequence number conversion
        let seq_usize: usize = seq.try_into().unwrap();

        let messages = self.messages.lock().await;
        if seq_usize > messages.len() || seq_usize == 0 {
            return Err(Error::InvalidSeq(seq_usize, messages.len()));
        }

        Ok(messages.get(seq_usize - 1).and_then(Clone::clone))
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

    /// The last sequence number in the stream.
    async fn last_seq(&self) -> Result<u64, Self::Error> {
        Ok(self.messages.lock().await.len().try_into().unwrap())
    }

    /// The number of messages in the stream.
    async fn messages(&self) -> Result<u64, Self::Error> {
        Ok(self
            .messages
            .lock()
            .await
            .iter()
            .filter(|message| message.is_some())
            .count()
            .try_into()
            .unwrap())
    }

    /// Returns the name of the stream.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: T) -> Result<u64, Self::Error> {
        let seq: u64 = {
            let mut messages = self.messages.lock().await;

            messages.push(Some(message.clone()));
            let seq_usize = messages.len();

            // Broadcast the message to all consumers while holding the lock
            let mut channels = self.consumer_channels.lock().await;
            channels.retain_mut(|sender| {
                let message = message.clone();
                sender.try_send(message).is_ok()
            });
            drop(channels);
            drop(messages);

            // TODO: Add error handling for sequence number conversion
            seq_usize.try_into().unwrap()
        };

        Ok(seq)
    }

    /// Publishes a batch of messages atomically to the stream.
    /// Returns the sequence number of the last published message.
    async fn publish_batch(&self, messages: Vec<T>) -> Result<u64, Self::Error> {
        if messages.is_empty() {
            return Err(Error::EmptyBatch);
        }

        let last_seq: u64 = {
            let mut stream_messages = self.messages.lock().await;

            // Add all messages atomically
            for message in &messages {
                stream_messages.push(Some(message.clone()));
            }

            let last_seq_usize = stream_messages.len();

            // Broadcast all messages to consumers while holding the lock
            let mut channels = self.consumer_channels.lock().await;
            for message in &messages {
                channels.retain_mut(|sender| {
                    let message = message.clone();
                    sender.try_send(message).is_ok()
                });
            }
            drop(channels);
            drop(stream_messages);

            // TODO: Add error handling for sequence number conversion
            last_seq_usize.try_into().unwrap()
        };

        Ok(last_seq)
    }

    /// Publishes a rollup message directly to the stream - purges all prior messages.
    /// Must provide expected sequence number for optimistic concurrency control.
    async fn rollup(&self, message: T, expected_seq: u64) -> Result<u64, Self::Error> {
        let expected_seq_usize: usize = expected_seq.try_into().unwrap();

        let seq_usize = {
            let mut messages = self.messages.lock().await;
            let seq = messages.len();

            if seq != expected_seq_usize {
                return Err(Error::OutdatedSeq(seq, expected_seq_usize));
            }

            // Tombstone all prior messages by setting them to None
            for message in messages.iter_mut() {
                *message = None;
            }

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

    /// Consumes the stream with the given service.
    async fn service<N, X>(
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
        let stream_name = self.full_name.clone();
        let options = self.options.clone();

        // Always create the stream first
        let new_stream =
            InitializedMemoryStream::<T, D, S>::new(stream_name.clone(), options).await?;

        // Atomically check-and-insert into global cache
        let mut global_state = GLOBAL_STATE.lock().await;

        // Check if stream already exists (another thread may have created it while we were creating ours)
        if let Some(stream_map) =
            global_state.try_borrow::<HashMap<String, InitializedMemoryStream<T, D, S>>>()
            && let Some(existing_stream) = stream_map.get(&stream_name)
        {
            // Use the existing stream and discard the one we just created
            return Ok(existing_stream.clone());
        }

        // Initialize the map if it doesn't exist
        if !global_state.has::<HashMap<String, InitializedMemoryStream<T, D, S>>>() {
            global_state.put(HashMap::<String, InitializedMemoryStream<T, D, S>>::new());
        }

        // Store the stream we created
        if let Some(stream_map) =
            global_state.try_borrow_mut::<HashMap<String, InitializedMemoryStream<T, D, S>>>()
        {
            stream_map.insert(stream_name, new_stream.clone());
        }

        drop(global_state);

        Ok(new_stream)
    }

    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error>
    where
        J: Into<Self::Subject> + Clone + Send,
    {
        let stream_name = self.full_name.clone();
        let options = self.options.clone();

        // Always create the stream first
        let new_stream = InitializedMemoryStream::<T, D, S>::new_with_subjects(
            stream_name.clone(),
            options,
            subjects,
        )
        .await?;

        // Atomically check-and-insert into global cache
        let mut global_state = GLOBAL_STATE.lock().await;

        // Check if stream already exists (another thread may have created it while we were creating ours)
        if let Some(stream_map) =
            global_state.try_borrow::<HashMap<String, InitializedMemoryStream<T, D, S>>>()
            && let Some(existing_stream) = stream_map.get(&stream_name)
        {
            // Use the existing stream and discard the one we just created
            return Ok(existing_stream.clone());
        }

        // Initialize the map if it doesn't exist
        if !global_state.has::<HashMap<String, InitializedMemoryStream<T, D, S>>>() {
            global_state.put(HashMap::<String, InitializedMemoryStream<T, D, S>>::new());
        }

        // Store the stream we created
        if let Some(stream_map) =
            global_state.try_borrow_mut::<HashMap<String, InitializedMemoryStream<T, D, S>>>()
        {
            stream_map.insert(stream_name, new_stream.clone());
        }

        drop(global_state);

        Ok(new_stream)
    }
}

macro_rules! impl_scoped_stream {
    ($index:expr_2021, $parent:ident, $parent_trait:ident, $doc:expr_2021) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Debug)]
            pub struct [< MemoryStream $index >]<T, D, S>
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

            impl<T, D, S> Clone for [< MemoryStream $index >]<T, D, S>
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

            impl<T, D, S> [< MemoryStream $index >]<T, D, S>
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

            #[async_trait]
            impl<T, D, S> [< Stream $index >]<T, D, S> for [< MemoryStream $index >]<T, D, S>
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

                fn scope<K>(&self, scope: K) -> $parent<T, D, S>
                where
                    K: AsRef<str> + Send,
                {
                    $parent::<T, D, S> {
                        full_name: format!("{}_{}", self.full_name, scope.as_ref()),
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
    "A single-scoped in-memory stream."
);
impl_scoped_stream!(
    2,
    MemoryStream1,
    Stream1,
    "A double-scoped in-memory stream."
);
impl_scoped_stream!(
    3,
    MemoryStream2,
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
    use proven_bootable::Bootable;
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

        assert_eq!(stream.get(1).await.unwrap(), Some(message));
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
        let stream1: MemoryStream1<Bytes, Infallible, Infallible> =
            MemoryStream1::new("test_scoped_stream", MemoryStreamOptions);
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
        let stream2: MemoryStream2<Bytes, Infallible, Infallible> =
            MemoryStream2::new("test_nested_scoped_stream", MemoryStreamOptions);
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

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(stream.get(1).await.unwrap(), Some(message1));
        assert_eq!(stream.get(2).await.unwrap(), Some(message2));
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

            async fn handle(
                &self,
                message: Bytes,
                _stream_sequence: u64,
            ) -> Result<(), Self::Error> {
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

        let consumer = stream
            .consumer("test", MemoryConsumerOptions, handler)
            .await
            .unwrap();

        consumer.start().await.unwrap();

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
        stream.delete(2).await.unwrap();

        // First and last messages should still be accessible
        assert_eq!(stream.get(1).await.unwrap(), Some(message1));
        assert_eq!(stream.get(2).await.unwrap(), None);
        assert_eq!(stream.get(3).await.unwrap(), Some(message3.clone()));

        // Attempting to delete an invalid sequence should fail
        assert!(stream.delete(99).await.is_err());

        // Last message should still be message3
        assert_eq!(stream.last_message().await.unwrap(), Some(message3));
    }

    #[tokio::test]
    async fn test_rollup() {
        let subject = MemorySubject::new("test_rollup").unwrap();
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

        // Perform rollup with message3, expecting the sequence number to be 2
        let seq = stream.rollup(message3.clone(), 2).await.unwrap();

        // Verify the sequence number returned by rollup
        assert_eq!(seq, 2);

        // Verify that only message3 is present in the stream
        assert_eq!(stream.get(1).await.unwrap(), None);
        assert_eq!(stream.get(2).await.unwrap(), None);
        assert_eq!(stream.get(3).await.unwrap(), Some(message3.clone()));

        // Verify that the last message is message3
        assert_eq!(stream.last_message().await.unwrap(), Some(message3));
    }

    #[tokio::test]
    async fn test_rollup_with_outdated_seq() {
        let subject = MemorySubject::new("test_rollup_with_outdated_seq").unwrap();
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

        // Attempt rollup with message3, providing an outdated expected sequence number
        let result = stream.rollup(message3.clone(), 1).await;

        // Verify that an error is returned
        assert!(result.is_err());
        if let Err(Error::OutdatedSeq(current_seq, expected_seq)) = result {
            assert_eq!(current_seq, 2);
            assert_eq!(expected_seq, 1);
        } else {
            panic!("Expected Error::OutdatedSeq");
        }
    }

    #[tokio::test]
    async fn test_publish_batch() {
        let subject: MemorySubject<Bytes, std::convert::Infallible, std::convert::Infallible> =
            MemorySubject::new("test_publish_batch").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_stream",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();

        let message1 = Bytes::from("batch_message1");
        let message2 = Bytes::from("batch_message2");
        let message3 = Bytes::from("batch_message3");
        let all_messages = vec![message1.clone(), message2.clone(), message3.clone()];

        // Publish batch
        let last_seq = stream.publish_batch(all_messages).await.unwrap();
        assert_eq!(last_seq, 3);

        // Verify all messages were stored
        assert_eq!(stream.get(1).await.unwrap(), Some(message1));
        assert_eq!(stream.get(2).await.unwrap(), Some(message2));
        assert_eq!(stream.get(3).await.unwrap(), Some(message3));
        assert_eq!(stream.last_seq().await.unwrap(), 3);
        assert_eq!(stream.messages().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_publish_batch_empty() {
        let subject: MemorySubject<Bytes, std::convert::Infallible, std::convert::Infallible> =
            MemorySubject::new("test_publish_batch_empty").unwrap();
        let stream = InitializedMemoryStream::new_with_subjects(
            "test_stream",
            MemoryStreamOptions,
            vec![subject],
        )
        .await
        .unwrap();

        // Attempt to publish empty batch
        let result: Result<u64, Error> = stream.publish_batch(vec![]).await;
        assert!(result.is_err());
        if matches!(result, Err(Error::EmptyBatch)) {
            // Expected error
        } else {
            panic!("Expected Error::EmptyBatch");
        }
    }
}
