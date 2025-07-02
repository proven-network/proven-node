//! Consumers are stateful views of consensus streams.

use std::error::Error as StdError;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;

use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_messaging::consumer::{Consumer, ConsumerError, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;

use crate::error::ConsensusError;
use crate::stream::InitializedConsensusStream;

/// Options for consensus consumers.
#[derive(Clone, Debug, Copy)]
pub struct ConsensusConsumerOptions {
    /// Starting sequence number.
    pub start_sequence: Option<u64>,
}

impl ConsumerOptions for ConsensusConsumerOptions {}

/// Error type for consensus consumers.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusConsumerError {
    /// Consensus error.
    #[error("Consensus error: {0}")]
    Consensus(#[from] ConsensusError),
}

impl ConsumerError for ConsensusConsumerError {}

/// A consensus consumer.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ConsensusConsumer<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S>,
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
    stream: InitializedConsensusStream<G, A, T, D, S>,
    options: ConsensusConsumerOptions,
    handler: X,
    last_processed_seq: u64,
}

#[async_trait]
impl<G, A, X, T, D, S> Consumer<X, T, D, S> for ConsensusConsumer<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ConsumerHandler<T, D, S>,
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
    type Error = ConsensusConsumerError;
    type Options = ConsensusConsumerOptions;
    type StreamType = InitializedConsensusStream<G, A, T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            name,
            stream,
            options,
            handler,
            last_processed_seq: options.start_sequence.unwrap_or(0),
        })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        Ok(self.last_processed_seq)
    }
}

#[async_trait]
impl<G, A, X, T, D, S> Bootable for ConsensusConsumer<G, A, X, T, D, S>
where
    X: ConsumerHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static + Clone,
    S: Debug + Send + StdError + Sync + 'static + Clone,
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
{
    fn bootable_name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Start consumer message processing loop
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Stop consumer message processing loop
        Ok(())
    }

    async fn wait(&self) -> () {
        // TODO: Wait for consumer to finish processing
    }
}
