//! Network service handler for Stream service

use async_trait::async_trait;
use proven_attestation::Attestor;
use proven_network::{NetworkResult, Service, ServiceContext};
use proven_storage::{LogStorage, StorageAdaptor};
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;
use std::sync::Arc;
use tokio_stream::StreamExt;

use super::messages::{StreamServiceMessage, StreamServiceResponse};
use super::service::StreamService;

/// Stream service handler
pub struct StreamHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: LogStorage + StorageAdaptor,
{
    service: Arc<StreamService<T, G, A, S>>,
}

impl<T, G, A, S> StreamHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: LogStorage + StorageAdaptor,
{
    /// Create a new handler
    pub fn new(service: Arc<StreamService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> Service for StreamHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: LogStorage + StorageAdaptor + Send + Sync + 'static,
{
    type Request = StreamServiceMessage;

    async fn handle(
        &self,
        message: Self::Request,
        _ctx: ServiceContext,
    ) -> NetworkResult<<Self::Request as proven_network::ServiceMessage>::Response> {
        match message {
            StreamServiceMessage::Read {
                stream_name,
                sequence_range,
            } => {
                // Check if we can serve this stream locally
                if let Ok(can_serve) = self.service.can_serve_stream(&stream_name).await
                    && !can_serve
                {
                    return Ok(StreamServiceResponse::Error(
                        "Cannot serve stream from this node".to_string(),
                    ));
                }

                // Read messages based on range
                let result =
                    if let (Some(start), Some(end)) = (sequence_range.start, sequence_range.end) {
                        self.service.read_range(&stream_name, start, end).await
                    } else if let Some(start) = sequence_range.start {
                        // Read from start to latest
                        match self.service.read_from(&stream_name, start).await {
                            Ok(stream) => {
                                let messages: Vec<_> =
                                    futures::StreamExt::map(stream, |(msg, _, _)| msg)
                                        .collect()
                                        .await;
                                Ok(messages)
                            }
                            Err(e) => Err(e),
                        }
                    } else {
                        // No range specified - return empty
                        Ok(vec![])
                    };

                match result {
                    Ok(messages) => Ok(StreamServiceResponse::Messages(messages)),
                    Err(e) => Ok(StreamServiceResponse::Error(e.to_string())),
                }
            }
            StreamServiceMessage::Create { .. } => Ok(StreamServiceResponse::Error(
                "Stream creation should go through consensus".to_string(),
            )),
            StreamServiceMessage::Delete { .. } => Ok(StreamServiceResponse::Error(
                "Stream deletion should go through consensus".to_string(),
            )),
            StreamServiceMessage::Query { .. } => Ok(StreamServiceResponse::Error(
                "Streaming queries not implemented".to_string(),
            )),
        }
    }
}
