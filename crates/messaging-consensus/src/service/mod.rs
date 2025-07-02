//! Services are special consumers that respond to requests in the consensus network.

use std::error::Error as StdError;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;

use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_messaging::service::{Service, ServiceError, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;

use crate::error::ConsensusError;
use crate::service_responder::ConsensusServiceResponder;
use crate::stream::InitializedConsensusStream;
use crate::subscription_responder::ConsensusUsedServiceResponder;

/// Options for consensus services.
#[derive(Clone, Debug, Copy)]
pub struct ConsensusServiceOptions {
    /// Starting sequence number.
    pub start_sequence: Option<u64>,
}

impl ServiceOptions for ConsensusServiceOptions {}

/// Error type for consensus services.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusServiceError {
    /// Consensus error.
    #[error("Consensus error: {0}")]
    Consensus(#[from] ConsensusError),
}

impl ServiceError for ConsensusServiceError {}

/// A consensus service.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
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
    options: ConsensusServiceOptions,
    handler: X,
    last_processed_seq: u64,
}

#[async_trait]
impl<G, A, X, T, D, S> Service<X, T, D, S> for ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
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
    type Error = ConsensusServiceError;
    type Options = ConsensusServiceOptions;
    type Responder = ConsensusServiceResponder<
        T,
        D,
        S,
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >;
    type UsedResponder = ConsensusUsedServiceResponder;
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

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}

#[async_trait]
impl<G, A, X, T, D, S> Bootable for ConsensusService<G, A, X, T, D, S>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
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
    fn bootable_name(&self) -> &str {
        &self.name
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Start service message processing loop
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Stop service message processing loop
        Ok(())
    }

    async fn wait(&self) -> () {
        // TODO: Wait for service to finish processing
    }
}
