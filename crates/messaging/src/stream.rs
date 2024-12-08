use crate::consumer::Consumer;
use crate::service::Service;
use crate::subject::Subject;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for stream errors
pub trait StreamError: Error + Send + Sync + 'static {}

/// A trait representing a stream.
#[async_trait]
pub trait Stream<T, DE, SE>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    DE: Error + Send + Sync + 'static,
    SE: Error + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new<J, N>(stream_name: N, subjects: Vec<J>) -> Result<Self, Self::Error>
    where
        N: Into<String> + Send,
        J: Subject<T, DE, SE>;

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: usize) -> Result<Option<T>, Self::Error>;

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<T>, Self::Error>;

    /// Returns the name of the stream.
    async fn name(&self) -> String;

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: T) -> Result<usize, Self::Error>;

    /// Consumes the stream with the given consumer.
    async fn start_consumer<C>(&self, consumer: C) -> Result<(), Self::Error>
    where
        C: Consumer<Self, T, DE, SE>;

    /// Consumes the stream with the given service.
    async fn start_service<S>(&self, service: S) -> Result<(), Self::Error>
    where
        S: Service<Self, T, DE, SE>;
}

/// A trait representing a scoped-stream.
#[async_trait]
pub trait ScopedStream<T, DE, SE>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    DE: Error + Send + Sync + 'static,
    SE: Error + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// Initializes the stream.
    async fn init<J>(&self, subjects: Vec<J>) -> Result<impl Stream<T, DE, SE>, Self::Error>
    where
        J: Subject<T, DE, SE>;
}

macro_rules! define_scoped_stream {
    ($index:expr, $parent:ident, $doc:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc]
            pub trait [< ScopedStream $index >]<T, DE, SE>
            where
                Self: Clone + Send + Sync + 'static,
                T: Clone + Debug + Send + Sync + 'static,
                DE: Error + Send + Sync + 'static,
                SE: Error + Send + Sync + 'static,
            {
                /// The error type for the stream.
                type Error: StreamError;

                /// The scoped stream type.
                type Scoped: $parent<T, DE, SE, Error = Self::Error> + Clone + Send + Sync + 'static;

                /// Creates a scoped stream.
                fn scope<S>(&self, scope: S) -> <Self as [< ScopedStream $index >]<T, DE, SE>>::Scoped
                where
                    S: Into<String> + Send;
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
