use crate::consumer::Consumer;
use crate::consumer_handler::ConsumerHandler;
use crate::service::Service;
use crate::service_handler::ServiceHandler;
use crate::subject::Subject;
use crate::Message;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for stream errors
pub trait StreamError: Error + Send + Sync + 'static {}

/// Marker trait for stream options
pub trait StreamOptions: Clone + Send + Sync + 'static {}

/// A trait representing a stream.
#[async_trait]
pub trait Stream<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// The options for the stream.
    type Options: StreamOptions;

    /// The subject type for the stream.
    type SubjectType: Subject<T>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N, options: Self::Options) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send;

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N>(
        stream_name: N,
        options: Self::Options,
        subjects: Vec<Self::SubjectType>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send;

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
        handler: X,
    ) -> Result<impl Consumer<X, T>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<T>;

    /// Consumes the stream with the given service.
    async fn start_service<N, X>(
        &self,
        service_name: N,
        handler: X,
    ) -> Result<impl Service<X, T>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T>;
}

/// A trait representing a scoped-stream.
#[async_trait]
pub trait ScopedStream<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// The options for the consumer.
    type Options: StreamOptions;

    /// The stream type.
    type StreamType: Stream<T>;

    /// The subject type for the stream.
    type SubjectType: Subject<T>;

    /// Initializes the stream.
    async fn init(&self) -> Result<Self::StreamType, Self::Error>;

    /// Initializes the stream with the given subjects - must all be the same type.
    async fn init_with_subjects(
        &self,
        subjects: Vec<Self::SubjectType>,
    ) -> Result<Self::StreamType, Self::Error>;
}

macro_rules! define_scoped_stream {
    ($index:expr, $parent:ident, $doc:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc]
            pub trait [< ScopedStream $index >]<T>
            where
                Self: Clone + Send + Sync + 'static,
                T: Clone + Debug + Send + Sync + 'static,
            {
                /// The error type for the stream.
                type Error: StreamError;

                /// The options for the stream.
                type Options: StreamOptions;

                /// The scoped stream type.
                type Scoped: $parent<T> + Clone + Send + Sync + 'static;

                /// Creates a scoped stream.
                fn scope<S>(&self, scope: S) -> <Self as [< ScopedStream $index >]<T>>::Scoped
                where
                    S: Clone + Into<String> + Send;
            }
        }
    };
}

define_scoped_stream!(
    1,
    ScopedStream,
    "A trait representing a single-scoped stream."
);
define_scoped_stream!(
    2,
    ScopedStream1,
    "A trait representing a double-scoped stream."
);
define_scoped_stream!(
    3,
    ScopedStream2,
    "A trait representing a triple-scoped stream."
);
