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
    InitializedStream, Stream, Stream1, Stream2, Stream3, StreamOptions,
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
pub struct InitializedNatsStream<T, D, S>
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

impl<T, D, S> Clone for InitializedNatsStream<T, D, S>
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
impl<T, D, S> InitializedStream<T, D, S> for InitializedNatsStream<T, D, S>
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

    type Subject = NatsUnpublishableSubject<T, D, S>;

    type Client<X>
        = NatsClient<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    type Consumer<X>
        = NatsConsumer<X, T, D, S>
    where
        X: ConsumerHandler<T, D, S>;

    type Service<X>
        = NatsService<X, T, D, S>
    where
        X: ServiceHandler<T, D, S>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N, options: NatsStreamOptions) -> Result<Self, Self::Error>
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
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
        J: Into<Self::Subject> + Clone + Send,
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
                        let subject: Self::Subject = s.clone().into();
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
    ) -> Result<NatsClient<X, T, D, S>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T, D, S>,
    {
        // Implementation here
        unimplemented!()
    }

    /// Deletes the message with the given sequence number.
    async fn del(&self, seq: u64) -> Result<(), Self::Error> {
        self.nats_stream
            .delete_message(seq)
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<Message<T>>, Self::Error> {
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
    async fn last_message(&self) -> Result<Option<Message<T>>, Self::Error> {
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
    async fn publish(&self, message: Message<T>) -> Result<u64, Self::Error> {
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
    ) -> Result<Self::Consumer<X>, Self::Error>
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
    ) -> Result<Self::Service<X>, Self::Error>
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
    full_name: String,
    options: NatsStreamOptions,
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
            full_name: self.full_name.clone(),
            options: self.options.clone(),
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
    type Options = NatsStreamOptions;

    /// The initialized stream type.
    type Initialized = InitializedNatsStream<T, D, S>;

    type Subject = NatsUnpublishableSubject<T, D, S>;

    /// Creates a new `NatsStream`.
    fn new<K>(stream_name: K, options: NatsStreamOptions) -> Self
    where
        K: Clone + Into<String> + Send,
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
        let stream =
            InitializedNatsStream::new(self.full_name.clone(), self.options.clone()).await?;

        Ok(stream)
    }

    async fn init_with_subjects<J>(
        &self,
        subjects: Vec<J>,
    ) -> Result<Self::Initialized, <Self::Initialized as InitializedStream<T, D, S>>::Error>
    where
        J: Into<Self::Subject> + Clone + Send,
    {
        let stream = InitializedNatsStream::new_with_subjects(
            self.full_name.clone(),
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
            pub struct [< NatsStream $index >]<T, D, S>
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
                options: NatsStreamOptions,
                _marker: PhantomData<T>,
            }

            impl<T, D, S> Clone for [< NatsStream $index >]<T, D, S>
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

            impl<T, D, S> [< NatsStream $index >]<T, D, S>
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
                pub fn new<K>(stream_name: K, options: NatsStreamOptions) -> Self
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
            impl<T, D, S> [< Stream $index >]<T, D, S> for [< NatsStream $index >]<T, D, S>
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
                type Options = NatsStreamOptions;

                type Scoped = $parent<T, D, S>;

                fn scope<K: Clone + Into<String> + Send>(&self, scope: K) -> $parent<T, D, S> {
                    $parent::<T, D, S> {
                        full_name: format!("{}_{}", self.full_name, scope.into()),
                        options: self.options.clone(),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}
impl_scoped_stream!(1, NatsStream, Stream1, "A single-scoped NATs stream.");
impl_scoped_stream!(2, NatsStream1, Stream1, "A double-scoped NATs stream.");
impl_scoped_stream!(3, NatsStream2, Stream2, "A triple-scoped NATs stream.");
