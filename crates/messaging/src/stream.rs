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

    /// The response type for the stream (streams don't actually respond but it's used for services and consumers).
    type ResponseType: Clone + Debug + Send + Sync;

    /// The consumer type for the stream.
    type ConsumerType: Consumer<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// The client type for the stream.
    type ClientType: Client<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// The service type for the stream.
    type ServiceType: Service<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// The subject type for the stream.
    type SubjectType: Subject;

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
    async fn get(&self, seq: u64) -> Result<Option<Message<Self::Type>>, Self::Error>;

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<Message<Self::Type>>, Self::Error>;

    /// Returns the name of the stream.
    fn name(&self) -> String;

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: Message<Self::Type>) -> Result<u64, Self::Error>;

    /// Gets a client for a service.
    async fn client<N, X>(&self, service_name: N) -> Result<Self::ClientType, Self::Error>
    where
        N: Clone + Into<String> + Send;

    /// Consumes the stream with the given consumer.
    async fn start_consumer<N, X>(
        &self,
        consumer_name: N,
        handler: X,
    ) -> Result<Self::ConsumerType, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// Consumes the stream with the given service.
    async fn start_service<N, X>(
        &self,
        service_name: N,
        handler: X,
    ) -> Result<Self::ServiceType, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<Type = Self::Type, ResponseType = Self::ResponseType>;
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

    /// The response type for the stream.
    type ResponseType: Clone + Debug + Send + Sync;

    /// The stream type.
    type StreamType: Stream<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// The subject type for the stream.
    type SubjectType: Subject<Type = Self::Type, ResponseType = Self::ResponseType>;

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

                /// The response type for the stream (streams don't actually respond but it's used for services and consumers).
                type ResponseType: Clone + Debug + Send + Sync;

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
