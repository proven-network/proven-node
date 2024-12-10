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

/// Marker trait for stream errors
pub trait StreamError: Error + Send + Sync + 'static {}

/// Marker trait for stream options
pub trait StreamOptions: Clone + Send + Sync + 'static {}

/// A trait representing a stream.
#[async_trait]
pub trait Stream
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// The options for the stream.
    type Options: StreamOptions;

    /// The type of data in the stream.
    type Type: Clone + Debug + Send + Sync;

    /// The subject type for the stream.
    type SubjectType: Subject;

    /// The client type for the stream.
    type ClientType<X>: Client<Type = Self::Type, ResponseType = X::ResponseType>
    where
        X: ServiceHandler<Type = Self::Type>;

    /// The consumer type for the stream.
    type ConsumerType: Consumer;

    /// The service type for the stream.
    type ServiceType<X>: Service
    where
        X: ServiceHandler<Type = Self::Type>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N, options: Self::Options) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send;

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N, S>(
        stream_name: N,
        options: Self::Options,
        subjects: Vec<S>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        S: Into<Self::SubjectType> + Clone + Send;

    /// Gets a client for a service.
    async fn client<N, X>(
        &self,
        service_name: N,
        handler: X,
    ) -> Result<Self::ClientType<X>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<Type = Self::Type>;

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<Message<Self::Type>>, Self::Error>;

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<Message<Self::Type>>, Self::Error>;

    /// Returns the name of the stream.
    fn name(&self) -> String;

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: Message<Self::Type>) -> Result<u64, Self::Error>;

    /// Consumes the stream with the given consumer.
    async fn start_consumer<N, X>(
        &self,
        consumer_name: N,
        handler: X,
    ) -> Result<Self::ConsumerType, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<Type = Self::Type>;

    /// Consumes the stream with the given service.
    async fn start_service<N, X>(
        &self,
        service_name: N,
        handler: X,
    ) -> Result<Self::ServiceType<X>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<Type = Self::Type>;
}

/// A trait representing a scoped-stream.
#[async_trait]
pub trait ScopedStream
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// The options for the consumer.
    type Options: StreamOptions;

    /// The type of data in the stream.
    type Type: Clone + Debug + Send + Sync;

    /// The stream type.
    type StreamType: Stream<Type = Self::Type>;

    /// The subject type for the stream.
    type SubjectType: Subject<Type = Self::Type>;

    /// Initializes the stream.
    async fn init(&self) -> Result<Self::StreamType, Self::Error>;

    /// Initializes the stream with the given subjects - must all be the same type.
    async fn init_with_subjects<S>(
        &self,
        subjects: Vec<S>,
    ) -> Result<Self::StreamType, Self::Error>
    where
        S: Into<Self::SubjectType> + Clone + Send;
}

macro_rules! define_scoped_stream {
    ($index:expr, $parent:ident, $doc:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc]
            pub trait [< ScopedStream $index >]
            where
                Self: Clone + Send + Sync + 'static,
            {
                /// The error type for the stream.
                type Error: StreamError;

                /// The options for the stream.
                type Options: StreamOptions;

                /// The type of data in the stream.
                type Type: Clone + Debug + Send + Sync;

                /// The scoped stream type.
                type Scoped: $parent + Clone + Send + Sync + 'static;

                /// Creates a scoped stream.
                fn scope<S>(&self, scope: S) -> <Self as [< ScopedStream $index >]>::Scoped
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
