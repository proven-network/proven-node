use crate::client::Client;
use crate::consumer::Consumer;
use crate::consumer_handler::ConsumerHandler;
use crate::service::Service;
use crate::service_handler::ServiceHandler;
use crate::subject::Subject;
use crate::Message;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for stream errors
pub trait StreamError: Debug + Error + Send + Sync + 'static {}

/// Marker trait for stream options
pub trait StreamOptions: Clone + Debug + Send + Sync + 'static {}

/// A trait representing a stream.
#[async_trait]
pub trait InitializedStream<T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Error + Send + Sync + 'static,
    S: Debug + Error + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// The options for the stream.
    type Options: StreamOptions;

    /// The subject type for the stream.
    type Subject: Subject<T, D, S>;

    /// The client type for the stream.
    type Client<X>: Client<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    /// The consumer type for the stream.
    type Consumer<X>: Consumer<X, T, D, S>
    where
        X: ConsumerHandler<T, D, S>;

    /// The service type for the stream.
    type Service<X>: Service<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N, options: Self::Options) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send;

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N, J>(
        stream_name: N,
        options: Self::Options,
        subjects: Vec<J>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::Subject> + Clone + Send;

    /// Gets a client for a service.
    async fn client<N, X>(
        &self,
        service_name: N,
        handler: X,
    ) -> Result<Self::Client<X>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>;

    /// Deletes the message with the given sequence number.
    async fn del(&self, seq: u64) -> Result<(), Self::Error>;

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<Message<T>>, Self::Error>;

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<Message<T>>, Self::Error>;

    /// Returns the name of the stream.
    fn name(&self) -> String;

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: Message<T>) -> Result<u64, Self::Error>;

    /// Consumes the stream with the given consumer.
    async fn start_consumer<N, X>(
        &self,
        consumer_name: N,
        options: <Self::Consumer<X> as Consumer<X, T, D, S>>::Options,
        handler: X,
    ) -> Result<Self::Consumer<X>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<T, D, S>;

    /// Consumes the stream with the given service.
    async fn start_service<N, X>(
        &self,
        service_name: N,
        options: <Self::Service<X> as Service<X, T, D, S>>::Options,
        handler: X,
    ) -> Result<Self::Service<X>, <Self::Service<X> as Service<X, T, D, S>>::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>;
}

/// A trait representing a scoped-stream.
#[async_trait]
pub trait Stream<T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Error + Send + Sync + 'static,
    S: Debug + Error + Send + Sync + 'static,
{
    /// The options for the consumer.
    type Options: StreamOptions;

    /// The stream type.
    type Initialized: InitializedStream<T, D, S, Options = Self::Options> + Clone;

    /// The subject type for the stream.
    type Subject: Subject<T, D, S>;

    /// Creates a new stream.
    fn new<K>(stream_name: K, options: Self::Options) -> Self
    where
        K: Clone + Into<String> + Send;

    /// Initializes the stream.
    async fn init(
        &self,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error>;

    /// Initializes the stream with the given subjects - must all be the same type.
    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error>
    where
        J: Into<Self::Subject> + Clone + Send;
}

macro_rules! define_scoped_stream {
    ($index:expr, $parent:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            pub trait [< Stream $index >]<T, D, S>
            where
                Self: Clone + Send + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Error + Send + Sync + 'static,
                S: Debug + Error + Send + Sync + 'static,
            {
                /// The options for the stream.
                type Options: StreamOptions;

                /// The scoped stream type.
                type Scoped: $parent<T, D, S> + Clone + Send + Sync + 'static;

                /// Creates a scoped stream.
                fn scope<K>(&self, scope: K) -> <Self as [< Stream $index >]<T, D, S>>::Scoped
                where
                    K: Clone + Into<String> + Send;
            }
        }
    };
}

define_scoped_stream!(1, Stream, "A trait representing a single-scoped stream.");
define_scoped_stream!(2, Stream1, "A trait representing a double-scoped stream.");
define_scoped_stream!(3, Stream2, "A trait representing a triple-scoped stream.");
