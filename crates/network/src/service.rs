//! Service trait definitions for the network layer

use crate::error::NetworkResult;
use crate::message::{NetworkMessage, ServiceMessage};
use crate::stream::Stream;
use async_trait::async_trait;
use proven_topology::NodeId;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Context provided to service handlers
#[derive(Clone)]
pub struct ServiceContext {
    /// The node that sent the request
    pub sender: NodeId,
    /// Correlation ID if this is part of a request-response
    pub correlation_id: Option<uuid::Uuid>,
}

/// Trait for request-response services
#[async_trait]
pub trait Service: Send + Sync + 'static {
    /// The request type this service handles
    type Request: ServiceMessage;

    /// Handle a request and return a response
    async fn handle(
        &self,
        request: Self::Request,
        ctx: ServiceContext,
    ) -> NetworkResult<<Self::Request as ServiceMessage>::Response>;
}

// /// Trait for streaming services (complex version with request/response types)
// #[async_trait]
// pub trait StreamingService: Send + Sync + 'static {
//     /// The request type that initiates the stream
//     type Request: StreamingServiceMessage;
//
//     /// Handle a streaming request
//     async fn handle_stream(
//         &self,
//         request: Self::Request,
//         stream: Stream,
//         ctx: ServiceContext,
//     ) -> NetworkResult<()>;
// }

/// Type-erased service handler for internal use
pub(crate) trait ServiceHandler: Send + Sync {
    fn service_id(&self) -> &'static str;

    fn handle(
        &self,
        payload: bytes::Bytes,
        ctx: ServiceContext,
    ) -> Pin<Box<dyn Future<Output = NetworkResult<bytes::Bytes>> + Send>>;
}

/// Type-erased streaming service handler
pub(crate) trait StreamingServiceHandler: Send + Sync {
    fn stream_type(&self) -> &'static str;

    fn handle_stream(
        &self,
        peer: NodeId,
        stream: Stream,
        metadata: HashMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = NetworkResult<()>> + Send>>;
}

/// Concrete implementation of ServiceHandler
struct TypedServiceHandler<S: Service> {
    service: Arc<S>,
}

impl<S: Service> ServiceHandler for TypedServiceHandler<S> {
    fn service_id(&self) -> &'static str {
        S::Request::service_id()
    }

    fn handle(
        &self,
        payload: bytes::Bytes,
        ctx: ServiceContext,
    ) -> Pin<Box<dyn Future<Output = NetworkResult<bytes::Bytes>> + Send>> {
        let service = self.service.clone();

        Box::pin(async move {
            // Deserialize request
            let request = S::Request::deserialize(&payload)?;

            // Handle request
            let response = service.handle(request, ctx).await?;

            // Serialize response
            response.serialize()
        })
    }
}

// /// Concrete implementation of StreamingServiceHandler (for complex version)
// struct TypedStreamingServiceHandler<S: StreamingService> {
//     service: Arc<S>,
// }
//
// impl<S: StreamingService> StreamingServiceHandler for TypedStreamingServiceHandler<S> {
//     fn service_id(&self) -> &'static str {
//         S::Request::service_id()
//     }
//
//     fn stream_type(&self) -> &'static str {
//         S::Request::stream_type()
//     }
//
//     fn handle_stream(
//         &self,
//         payload: bytes::Bytes,
//         stream: Stream,
//         ctx: ServiceContext,
//     ) -> Pin<Box<dyn Future<Output = NetworkResult<()>> + Send>> {
//         let service = self.service.clone();
//
//         Box::pin(async move {
//             // Deserialize request
//             let request = S::Request::deserialize(&payload)?;
//
//             // Handle stream
//             service.handle_stream(request, stream, ctx).await
//         })
//     }
// }

/// Simple streaming service trait for direct stream handling
#[async_trait]
pub trait StreamingService: Send + Sync + 'static {
    /// The stream type identifier
    fn stream_type(&self) -> &'static str;

    /// Handle an incoming stream
    async fn handle_stream(
        &self,
        peer: NodeId,
        stream: Stream,
        metadata: HashMap<String, String>,
    ) -> NetworkResult<()>;
}

/// Concrete implementation of StreamingServiceHandler for simple services
struct SimpleStreamingServiceHandler<S> {
    service: Arc<S>,
}

impl<S: StreamingService> StreamingServiceHandler for SimpleStreamingServiceHandler<S> {
    fn stream_type(&self) -> &'static str {
        self.service.stream_type()
    }

    fn handle_stream(
        &self,
        peer: NodeId,
        stream: Stream,
        metadata: HashMap<String, String>,
    ) -> Pin<Box<dyn Future<Output = NetworkResult<()>> + Send>> {
        let service = self.service.clone();
        Box::pin(async move { service.handle_stream(peer, stream, metadata).await })
    }
}

// Helper functions to create typed handlers
pub(crate) fn create_service_handler<S: Service>(service: S) -> Box<dyn ServiceHandler> {
    Box::new(TypedServiceHandler {
        service: Arc::new(service),
    })
}

pub(crate) fn create_streaming_handler<S: StreamingService>(
    service: S,
) -> Box<dyn StreamingServiceHandler> {
    Box::new(SimpleStreamingServiceHandler {
        service: Arc::new(service),
    })
}
