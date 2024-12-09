mod error;

use crate::consumer::NatsConsumer;
use crate::service::NatsService;
use crate::subject::NatsSubject;
pub use error::Error;
use proven_messaging::service_handler::ServiceHandler;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::jetstream::stream::{Config as NatsStreamConfig, Stream as NatsStreamType};
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::stream::{
    ScopedStream, ScopedStream1, ScopedStream2, ScopedStream3, Stream, StreamOptions,
};
use proven_messaging::Message;

/// Options for the NATS stream.
#[derive(Clone, Debug)]
pub struct NatsStreamOptions {
    /// The NATS client.
    pub client: async_nats::Client,
}
impl StreamOptions for NatsStreamOptions {}

/// An in-memory stream.
#[derive(Clone, Debug)]
pub struct NatsStream<T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    name: String,
    nats_stream: NatsStreamType,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T> Stream<T> for NatsStream<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    type Error = Error;

    type Options = NatsStreamOptions;

    type SubjectType = NatsSubject<T>;

    /// Creates a new stream.
    async fn new<N>(stream_name: N, options: NatsStreamOptions) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
    {
        let jetstram_context = async_nats::jetstream::new(options.client);

        let nats_stream = jetstram_context
            .create_stream(NatsStreamConfig {
                name: stream_name.clone().into(),
                allow_direct: true,
                allow_rollup: true,
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(Self {
            name: stream_name.into(),
            nats_stream,
            _marker: PhantomData,
        })
    }

    /// Creates a new stream with the given subjects - must all be the same type.
    async fn new_with_subjects<N>(
        stream_name: N,
        options: NatsStreamOptions,
        subjects: Vec<NatsSubject<T>>,
    ) -> Result<Self, Self::Error>
    where
        N: Clone + Into<String> + Send,
    {
        let jetstram_context = async_nats::jetstream::new(options.client);

        let nats_stream = jetstram_context
            .create_stream(NatsStreamConfig {
                name: stream_name.clone().into(),
                allow_direct: true,
                allow_rollup: true,
                subjects: subjects.into_iter().map(Into::into).collect(),
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(Self {
            name: stream_name.into(),
            nats_stream,
            _marker: PhantomData,
        })
    }

    /// Gets the message with the given sequence number.
    async fn get(&self, seq: u64) -> Result<Option<Message<T>>, Self::Error> {
        match self.nats_stream.direct_get(seq).await {
            Ok(message) => {
                let payload: T = message.payload.try_into()?;
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
        let jetstram_context =
            async_nats::jetstream::new(async_nats::connect("localhost").await.unwrap());

        let payload: Bytes = message.payload.try_into()?;

        let seq = if let Some(headers) = message.headers {
            jetstram_context
                .publish_with_headers(self.name(), headers, payload)
                .await
                .map_err(|e| Error::Publish(e.kind()))?
                .await
                .map_err(|e| Error::Publish(e.kind()))?
                .sequence
        } else {
            jetstram_context
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
        _name: N,
        _handler: X,
    ) -> Result<NatsConsumer<X, T>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ConsumerHandler<T>,
    {
        unimplemented!()
    }

    /// Consumes the stream with the given service.
    async fn start_service<N, X>(
        &self,
        _service_name: N,
        _handler: X,
    ) -> Result<NatsService<X, T>, Self::Error>
    where
        N: Clone + Into<String> + Send,
        X: ServiceHandler<T>,
    {
        unimplemented!()
    }
}

/// All scopes applied and can initialize.
#[derive(Clone, Debug)]
pub struct ScopedNatsStream<T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    options: NatsStreamOptions,
    prefix: Option<String>,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T> ScopedStream<T> for ScopedNatsStream<T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    type Error = Error;

    type Options = NatsStreamOptions;

    type StreamType = NatsStream<T>;

    type SubjectType = NatsSubject<T>;

    async fn init(&self) -> Result<Self::StreamType, Self::Error> {
        let stream = NatsStream::new(self.prefix.clone().unwrap(), self.options.clone()).await?;

        Ok(stream)
    }

    async fn init_with_subjects(
        &self,
        subjects: Vec<Self::SubjectType>,
    ) -> Result<Self::StreamType, Self::Error> {
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
            #[derive(Clone, Debug)]
            pub struct [< ScopedNatsStream $index >]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,
            {
                options: NatsStreamOptions,
                prefix: Option<String>,
                _marker: PhantomData<T>,
            }

            impl<T> [< ScopedNatsStream $index >]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,
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
            impl<T> [< ScopedStream $index >]<T> for [< ScopedNatsStream $index >]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,
            {
                type Error = Error;

                type Options = NatsStreamOptions;

                type Scoped = $parent<T>;

                fn scope<S: Clone + Into<String> + Send>(&self, scope: S) -> $parent<T> {
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.into()),
                        None => scope.into(),
                    };
                    $parent::<T> {
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
