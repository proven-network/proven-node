mod error;

use crate::client::NatsClient;
use crate::consumer::NatsConsumer;
use crate::service::NatsService;
use crate::subject::NatsUnpublishableSubject;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::jetstream::stream::{Config as NatsStreamConfig, Stream as NatsStreamType};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client as AsyncNatsClient;
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::{
    ScopedStream, ScopedStream1, ScopedStream2, ScopedStream3, Stream, StreamOptions,
};
use proven_messaging::Message;

/// Options for the NATS stream.
#[derive(Clone, Debug)]
pub struct NatsStreamOptions {
    /// The NATS client.
    pub client: AsyncNatsClient,
}
impl StreamOptions for NatsStreamOptions {}

/// An in-memory stream.
#[derive(Debug)]
pub struct NatsStream<T, D, S>
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
    jetstream_context: JetStreamContext,
    name: String,
    nats_stream: NatsStreamType,
    _marker: PhantomData<T>,
}

impl<T, D, S> Clone for NatsStream<T, D, S>
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
            jetstream_context: self.jetstream_context.clone(),
            name: self.name.clone(),
            nats_stream: self.nats_stream.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> Stream<T, D, S> for NatsStream<T, D, S>
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
    type Error<DE, SE>
        = Error<DE, SE>
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;
    type Options = NatsStreamOptions;
    type SubjectType = NatsUnpublishableSubject<T, D, S>;

    type ClientType<X>
        = NatsClient<Self, X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    type ConsumerType<X>
        = NatsConsumer<Self, X, T, D, S>
    where
        X: ConsumerHandler<T, D, S>;

    type ServiceType<X>
        = NatsService<Self, X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N, options: NatsStreamOptions) -> Result<Self, Self::Error<D, S>>
    where
        N: Clone + Into<String> + Send,
    {
        let jetstream_context = async_nats::jetstream::new(options.client.clone());

        let nats_stream = jetstream_context
            .create_stream(NatsStreamConfig {
                name: stream_name.clone().into(),
                allow_direct: true,
                allow_rollup: true,
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(Self {
            jetstream_context,
            name: stream_name.into(),
            nats_stream,
            _marker: PhantomData,
        })
    }

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N, J>(
        stream_name: N,
        options: NatsStreamOptions,
        subjects: Vec<J>,
    ) -> Result<Self, Self::Error<D, S>>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::SubjectType> + Clone + Send,
    {
        let jetstream_context = async_nats::jetstream::new(options.client.clone());

        let nats_stream = jetstream_context
            .create_stream(NatsStreamConfig {
                name: stream_name.clone().into(),
                allow_direct: true,
                allow_rollup: true,
                subjects: subjects
                    .iter()
                    .map(|s| {
                        let subject: Self::SubjectType = s.clone().into();
                        let string: String = subject.into();
                        string
                    })
                    .map(Into::into)
                    .collect(),
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(Self {
            jetstream_context,
            name: stream_name.into(),
            nats_stream,
            _marker: PhantomData,
        })
    }

    async fn client<N, X>(
        &self,
        _service_name: N,
        _handler: X,
    ) -> Result<NatsClient<Self, X, T, D, S>, Self::Error<D, S>>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        // Implementation here
        unimplemented!()
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<Message<T>>, Self::Error<D, S>> {
        match self.nats_stream.direct_get(seq).await {
            Ok(message) => {
                let payload: T = message
                    .payload
                    .try_into()
                    .map_err(|e| Error::Deserialize(e))?;

                Ok(Some(Message {
                    headers: Some(message.headers),
                    payload,
                }))
            }
            Err(e) => match e.kind() {
                async_nats::jetstream::stream::DirectGetErrorKind::NotFound => Ok(None),
                _ => Err(Error::DirectGet(e.kind())),
            },
        }
    }

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<Message<T>>, Self::Error<D, S>> {
        let last_seq = self
            .nats_stream
            .clone()
            .info()
            .await
            .map_err(|e| Error::Info(e.kind()))?
            .state
            .last_sequence;

        self.get(last_seq).await
    }

    /// Returns the name of the stream.
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Publishes a message directly to the stream.
    async fn publish(&self, message: Message<T>) -> Result<u64, Self::Error<D, S>> {
        let payload: Bytes = message
            .payload
            .try_into()
            .map_err(|e| Error::Serialize(e))?;

        let seq = if let Some(headers) = message.headers {
            self.jetstream_context
                .publish_with_headers(self.name(), headers, payload)
                .await
                .map_err(|e| Error::Publish(e.kind()))?
                .await
                .map_err(|e| Error::Publish(e.kind()))?
                .sequence
        } else {
            self.jetstream_context
                .publish(self.name(), payload)
                .await
                .map_err(|e| Error::Publish(e.kind()))?
                .await
                .map_err(|e| Error::Publish(e.kind()))?
                .sequence
        };

        Ok(seq)
    }

    /// Consumes the stream with the given consumer.
    async fn start_consumer<N, X>(
        &self,
        _consumer_name: N,
        _handler: X,
    ) -> Result<Self::ConsumerType<X>, Self::Error<D, S>>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<T, D, S>,
    {
        // Implementation here
        unimplemented!()
    }

    /// Consumes the stream with the given service.
    async fn start_service<N, X>(
        &self,
        _service_name: N,
        _handler: X,
    ) -> Result<Self::ServiceType<X>, Self::Error<D, S>>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        // Implementation here
        unimplemented!()
    }
}

/// All scopes applied and can initialize.
#[derive(Debug)]
pub struct ScopedNatsStream<T, D, S>
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
    options: NatsStreamOptions,
    prefix: Option<String>,
    _marker: PhantomData<T>,
}

impl<T, D, S> Clone for ScopedNatsStream<T, D, S>
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
            options: self.options.clone(),
            prefix: self.prefix.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> ScopedStream<T, D, S> for ScopedNatsStream<T, D, S>
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
    type Error<DE, SE>
        = Error<DE, SE>
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;
    type Options = NatsStreamOptions;
    type StreamType = NatsStream<T, D, S>;
    type SubjectType = NatsUnpublishableSubject<T, D, S>;

    async fn init(&self) -> Result<Self::StreamType, Self::Error<D, S>> {
        let stream = NatsStream::new(self.prefix.clone().unwrap(), self.options.clone()).await?;

        Ok(stream)
    }

    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::StreamType, Self::Error<D, S>>
    where
        J: Into<Self::SubjectType> + Clone + Send,
    {
        let stream = NatsStream::new_with_subjects(
            self.prefix.clone().unwrap(),
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
            pub struct [< ScopedNatsStream $index >]<T, D, S>
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
                options: NatsStreamOptions,
                prefix: Option<String>,
                _marker: PhantomData<T>,
            }

            impl<T, D, S> Clone for [< ScopedNatsStream $index >]<T, D, S>
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
                        options: self.options.clone(),
                        prefix: self.prefix.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> [< ScopedNatsStream $index >]<T, D, S>
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
                /// Creates a new `[< NatsStream $index >]`.
                #[must_use]
                pub const fn new(options: NatsStreamOptions) -> Self {
                    Self {
                        options,
                        prefix: None,
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< ScopedStream $index >]<T, D, S> for [< ScopedNatsStream $index >]<T, D, S>
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
                type Error = Error<D, S>;
                type Options = NatsStreamOptions;
                type Scoped = $parent<T, D, S>;

                fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> $parent<T, D, S> {
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.into()),
                        None => scope.into(),
                    };
                    $parent::<T, D, S> {
                        options: self.options.clone(),
                        prefix: Some(new_scope),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}
impl_scoped_stream!(
    1,
    ScopedNatsStream,
    Stream1,
    "A double-scoped in-memory stream."
);
impl_scoped_stream!(
    2,
    ScopedNatsStream1,
    Stream1,
    "A double-scoped in-memory stream."
);
impl_scoped_stream!(
    3,
    ScopedNatsStream2,
    Stream2,
    "A triple-scoped in-memory stream."
);
