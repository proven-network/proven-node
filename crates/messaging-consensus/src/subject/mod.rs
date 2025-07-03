//! Subjects in the consensus messaging system.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::debug;

use proven_attestation::Attestor;
use proven_consensus::transport::ConsensusTransport;
use proven_consensus::Consensus;
use proven_governance::Governance;
use proven_messaging::subject::Subject;

use crate::stream::InitializedConsensusStream;
use crate::subscription::{ConsensusSubscription, ConsensusSubscriptionOptions};

/// A consensus subject.
#[derive(Debug)]
pub struct ConsensusSubject<G, A, C, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    C: ConsensusTransport + Send + Sync + 'static,
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
    name: String,
    /// Governance system for consensus
    governance: G,
    /// Attestor for verification
    attestor: A,
    /// Consensus manager for actual messaging operations
    consensus_manager: std::sync::Arc<Consensus<G, A, C>>,
    _marker: PhantomData<(T, D, S)>,
}

impl<G, A, C, T, D, S> Clone for ConsensusSubject<G, A, C, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    C: ConsensusTransport + Send + Sync + 'static,
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
            name: self.name.clone(),
            governance: self.governance.clone(),
            attestor: self.attestor.clone(),
            consensus_manager: self.consensus_manager.clone(),
            _marker: PhantomData,
        }
    }
}

impl<G, A, C, T, D, S> ConsensusSubject<G, A, C, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    C: ConsensusTransport + Send + Sync + 'static,
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
    /// Creates a new consensus subject.
    #[must_use]
    pub const fn new(
        name: String,
        governance: G,
        attestor: A,
        consensus_manager: std::sync::Arc<Consensus<G, A, C>>,
    ) -> Self {
        Self {
            name,
            governance,
            attestor,
            consensus_manager,
            _marker: PhantomData,
        }
    }

    /// Get access to the consensus manager for this subject
    #[must_use]
    pub const fn consensus_manager(&self) -> &std::sync::Arc<Consensus<G, A, C>> {
        &self.consensus_manager
    }
}

#[async_trait]
impl<G, A, C, T, D, S> Subject<T, D, S> for ConsensusSubject<G, A, C, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    C: ConsensusTransport + Send + Sync + 'static,
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
    type Error = crate::error::MessagingConsensusError;
    type StreamType = crate::stream::InitializedConsensusStream<G, A, C, T, D, S>;
    type SubscriptionType<X>
        = crate::subscription::ConsensusSubscription<G, A, C, X, T, D, S>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>;

    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Self::Error>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>,
    {
        debug!("Creating subscription for subject '{}'", self.name);

        let options = ConsensusSubscriptionOptions::default();

        <ConsensusSubscription<G, A, C, X, T, D, S> as proven_messaging::subscription::Subscription<
            X,
            T,
            D,
            S,
        >>::new(self.clone(), options, handler)
        .await
    }

    async fn to_stream<K>(
        &self,
        stream_name: K,
        options: <<Self as Subject<T, D, S>>::StreamType as proven_messaging::stream::InitializedStream<T, D, S>>::Options,
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
        let stream_name = stream_name.into();
        debug!(
            "Creating stream '{}' from subject '{}'",
            stream_name, self.name
        );

        <InitializedConsensusStream<G, A, C, T, D, S> as proven_messaging::stream::InitializedStream<
            T,
            D,
            S,
        >>::new(stream_name, options)
        .await
    }
}

// Note: From<String> trait removed since we now require governance and attestor parameters

impl<G, A, C, T, D, S> From<ConsensusSubject<G, A, C, T, D, S>> for String
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
    C: ConsensusTransport + Send + Sync + 'static,
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
    fn from(val: ConsensusSubject<G, A, C, T, D, S>) -> Self {
        val.name
    }
}
