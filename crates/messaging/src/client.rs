use crate::service_handler::ServiceHandler;
use crate::stream::Stream;
use crate::Message;

use std::fmt::Debug;

pub struct Client<X, S, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<Type = T>,
{
    stream: S,
    _marker: std::marker::PhantomData<(X, S, T)>,
}

impl<X, S, T> Client<X, S, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    S: Stream<Type = T>,
    X: ServiceHandler<Type = T>,
{
    /// Creates a new client.
    #[must_use]
    pub const fn new(stream: S) -> Self {
        Self {
            stream,
            _marker: std::marker::PhantomData,
        }
    }

    /// Requests a response from the service.
    pub async fn request(
        &self,
        _request: Message<T>,
    ) -> Result<Message<X::ResponseType>, X::Error> {
        // Ok(self.stream.publish(_request).await.unwrap())
        unimplemented!()
    }
}
