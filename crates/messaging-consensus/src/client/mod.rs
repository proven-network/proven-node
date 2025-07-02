//! Clients send requests to services in the consensus network.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;

use proven_attestation::Attestor;
use proven_governance::Governance;
use proven_messaging::client::{Client, ClientError, ClientOptions, ClientResponseType};
use proven_messaging::service_handler::ServiceHandler;

use crate::error::ConsensusError;
use crate::stream::InitializedConsensusStream;

/// Options for consensus clients.
#[derive(Clone, Debug)]
pub struct ConsensusClientOptions {
    /// Timeout for requests.
    pub timeout: std::time::Duration,
}

impl ClientOptions for ConsensusClientOptions {}

/// Error type for consensus clients.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusClientError {
    /// Consensus error.
    #[error("Consensus error: {0}")]
    Consensus(#[from] ConsensusError),
}

impl ClientError for ConsensusClientError {}

/// A consensus client.
#[derive(Clone, Debug)]
pub struct ConsensusClient<G, A, X, T, D, S>
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
    _name: String,
    _stream: InitializedConsensusStream<G, A, T, D, S>,
    _options: ConsensusClientOptions,
    _marker: PhantomData<X>,
}

#[async_trait]
impl<G, A, X, T, D, S> Client<X, T, D, S> for ConsensusClient<G, A, X, T, D, S>
where
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
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static,
{
    type Error = ConsensusClientError;
    type Options = ConsensusClientOptions;
    type ResponseType = X::ResponseType;
    type StreamType = InitializedConsensusStream<G, A, T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            _name: name,
            _stream: stream,
            _options: options,
            _marker: PhantomData,
        })
    }

    async fn request(
        &self,
        _request: T,
    ) -> Result<ClientResponseType<X::ResponseType>, Self::Error> {
        // TODO: Implement actual request/response through consensus
        todo!("Client request implementation not yet complete")
    }
}
