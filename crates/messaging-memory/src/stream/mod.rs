#![allow(dead_code)]

mod error;
mod subscription_handler;

use crate::subscription::{InMemorySubscriber, InMemorySubscriberOptions};
pub use error::Error;
use subscription_handler::StreamSubscriptionHandler;

use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::Stream;
use proven_messaging::subject::Subject;
use tokio::sync::mpsc;

/// An in-memory stream.
#[derive(Clone, Debug)]
pub struct MemoryStream<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    subscriptions: Vec<InMemorySubscriber<StreamSubscriptionHandler<T>, T>>,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T> Stream<T, Infallible, Infallible> for MemoryStream<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new<J, N>(_stream_name: N, subjects: Vec<J>) -> Self
    where
        J: Subject<T, Infallible, Infallible>,
        N: Into<String> + Send,
    {
        let (sender, _receiver) = mpsc::channel::<T>(100);

        let mut subscriptions = Vec::new();
        for subject in subjects {
            let handler = StreamSubscriptionHandler::new(sender.clone());
            subscriptions.push(
                subject
                    .subscribe(InMemorySubscriberOptions, handler)
                    .await
                    .unwrap(),
            );
        }

        // TODO: do something with the receiver

        Self {
            subscriptions,
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
        C: Consumer<Self, T, Infallible, Infallible>,
    {
        unimplemented!()
    }

    /// Consumes the stream with the given service.
    async fn start_service<S>(&self, _service: S) -> Result<(), Self::Error>
    where
        S: Service<Self, T, Infallible, Infallible>,
    {
        unimplemented!()
    }
}
