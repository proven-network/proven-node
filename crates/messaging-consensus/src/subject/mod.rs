//! Subjects in the consensus messaging system.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;

use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_messaging::subject::Subject;

/// A consensus subject.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ConsensusSubject<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    name: String,
    governance: G,
    attestor: A,
    _marker: PhantomData<(T, D, S)>,
}

impl<G, A, T, D, S> ConsensusSubject<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    /// Creates a new consensus subject.
    pub const fn new(name: String, governance: G, attestor: A) -> Self {
        Self {
            name,
            governance,
            attestor,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<G, A, T, D, S> Subject<T, D, S> for ConsensusSubject<G, A, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    type Error = crate::error::ConsensusError;
    type StreamType = crate::stream::InitializedConsensusStream<G, A, T, D, S>;
    type SubscriptionType<X>
        = crate::subscription::ConsensusSubscription<G, A, X, T, D, S>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>;

    async fn subscribe<X>(&self, _handler: X) -> Result<Self::SubscriptionType<X>, Self::Error>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>,
    {
        // TODO: Implement subscription logic
        todo!("Consensus subscriptions not yet implemented")
    }

    async fn to_stream<K>(
        &self,
        _stream_name: K,
        _options: <<Self as Subject<T, D, S>>::StreamType as proven_messaging::stream::InitializedStream<T, D, S>>::Options,
    ) -> Result<
        Self::StreamType,
        <<Self as Subject<T, D, S>>::StreamType as proven_messaging::stream::InitializedStream<
            T,
            D,
            S,
        >>::Error,
    >
    where
        K: Into<String> + Send,
    {
        // TODO: Implement stream creation from subject
        todo!("Subject to stream conversion not yet implemented")
    }
}

// Note: From<String> trait removed since we now require governance and attestor parameters

impl<G, A, T, D, S> From<ConsensusSubject<G, A, T, D, S>> for String
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
{
    fn from(val: ConsensusSubject<G, A, T, D, S>) -> Self {
        val.name
    }
}
