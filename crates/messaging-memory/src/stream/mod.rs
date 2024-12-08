mod error;

pub use error::Error;
use proven_messaging::subject::Subject;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::Stream;

/// An in-memory stream.
#[derive(Clone, Debug)]
pub struct MemoryStream<T, DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    subjects: Vec<String>,
    _marker: PhantomData<(T, DE, SE)>,
}

#[async_trait]
impl<T, DE, SE> Stream<T, DE, SE> for MemoryStream<T, DE, SE>
where
    Self: Clone + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new<J, N>(_stream_name: N, subjects: Vec<J>) -> Self
    where
        J: Subject<T, DE, SE>,
        N: Into<String> + Send,
    {
        Self {
            subjects: subjects.into_iter().map(|s| s.name()).collect(),
            _marker: PhantomData,
        }
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, _seq: u64) -> Result<Option<T>, Self::Error> {
        unimplemented!()
    }

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<T>, Self::Error> {
        unimplemented!()
    }

    /// Returns the name of the stream.
    async fn name(&self) -> String {
        unimplemented!()
    }

    /// Consumes the stream with the given consumer.
    async fn start_consumer<C>(&self, _consumer: C) -> Result<(), Self::Error>
    where
        C: Consumer<Self, T, DE, SE>,
    {
        unimplemented!()
    }

    /// Consumes the stream with the given service.
    async fn start_service<S>(&self, _service: S) -> Result<(), Self::Error>
    where
        S: Service<Self, T, DE, SE>,
    {
        unimplemented!()
    }
}
