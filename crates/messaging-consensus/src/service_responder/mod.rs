//! Service responders handle responses in the consensus network.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;

use futures::stream::Stream;
use proven_messaging::service_responder::{
    ServiceResponder, ServiceResponderError, UsedServiceResponder,
};

/// A consensus service responder.
#[derive(Clone, Debug)]
pub struct ConsensusServiceResponder<T, D, S, R, RD, RS>
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
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    _marker: PhantomData<(T, D, S, R, RD, RS)>,
}

#[async_trait]
impl<T, D, S, R, RD, RS> ServiceResponder<T, D, S, R, RD, RS>
    for ConsensusServiceResponder<T, D, S, R, RD, RS>
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
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    type Error = crate::error::ConsensusError;
    type UsedResponder = ConsensusUsedResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        ConsensusUsedResponder {}
    }

    async fn reply(self, _response: R) -> Self::UsedResponder {
        // TODO: Implement response sending
        ConsensusUsedResponder {}
    }

    async fn reply_and_delete_request(self, _response: R) -> Self::UsedResponder {
        // TODO: Implement response sending with deletion
        ConsensusUsedResponder {}
    }

    async fn stream<W>(self, _stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        // TODO: Implement stream response
        ConsensusUsedResponder {}
    }

    async fn stream_and_delete_request<W>(self, _stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        // TODO: Implement stream response with deletion
        ConsensusUsedResponder {}
    }

    fn stream_sequence(&self) -> u64 {
        // TODO: Return actual sequence number
        0
    }
}

/// Used responder for consensus services.
#[derive(Debug)]
pub struct ConsensusUsedResponder;

impl UsedServiceResponder for ConsensusUsedResponder {}

impl ServiceResponderError for crate::error::ConsensusError {}
