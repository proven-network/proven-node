use crate::error::Error;
use crate::request::Request;
use crate::response::Response;
use crate::service_handler::HttpServiceHandler;
use crate::{DeserializeError, SerializeError};

use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};
use proven_messaging::{
    client::{Client, ClientResponseType},
    stream::InitializedStream,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, warn};

type NatsClient<S> =
    <S as InitializedStream<Request, DeserializeError, SerializeError>>::Client<HttpServiceHandler>;

/// Options for the NATS proxy client.
#[derive(Clone)]
pub struct NatsProxyClientOptions<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
    <<S as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
        HttpServiceHandler,
    > as Client<HttpServiceHandler, Request, DeserializeError, SerializeError>>::Options: Clone,
{
    /// The options for the messaging client.
    pub client_options: <NatsClient<S> as Client<
        HttpServiceHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// The port to listen on for incoming requests.
    pub http_port: u16,

    /// The stream to route requests to.
    pub stream: S,
}

/// The NATS proxy client.
#[derive(Clone)]
pub struct NatsProxyClient<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError> + Clone + Send + Sync + 'static,
    NatsClient<S>: Client<HttpServiceHandler, Request, DeserializeError, SerializeError>
        + Clone
        + Send
        + Sync
        + 'static,
    <<S as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
        HttpServiceHandler,
    > as Client<HttpServiceHandler, Request, DeserializeError, SerializeError>>::Options:
        Clone + Send,
{
    client_options: <NatsClient<S> as Client<
            HttpServiceHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,
    http_port: u16,
    stream: S,
}

impl<S> NatsProxyClient<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError> + Clone + Send + Sync + 'static,
    NatsClient<S>: Client<HttpServiceHandler, Request, DeserializeError, SerializeError>
        + Clone
        + Send
        + Sync
        + 'static,
    <<S as InitializedStream<Request, DeserializeError, SerializeError>>::Client<
        HttpServiceHandler,
    > as Client<HttpServiceHandler, Request, DeserializeError, SerializeError>>::Options:
        Clone + Send,
{
    /// Create a new NATS proxy client.
    pub fn new(options: NatsProxyClientOptions<S>) -> Self {
        Self {
            client_options: options.client_options,
            http_port: options.http_port,
            stream: options.stream,
        }
    }

    /// Start the NATS proxy client's HTTP listener.
    /// This function will run indefinitely until the server encounters an error or is shut down.
    pub async fn start(&self) -> Result<(), Error> {
        info!("Initializing NATS client for HTTP proxy...");
        let nats_client = self
            .stream
            .client::<_, HttpServiceHandler>("HTTP_PROXY", self.client_options.clone())
            .await
            .map_err(|e| {
                error!("NATS client setup failed: {}", e);
                Error::ClientSetup(e.to_string())
            })?;
        info!("NATS client initialized.");

        let shared_client = Arc::new(nats_client);

        async fn handle_request<Stream>(
            State(client): State<Arc<NatsClient<Stream>>>,
            method: Method,
            uri: Uri,
            headers: HeaderMap,
            body: Bytes,
        ) -> impl IntoResponse
        where
            Stream: InitializedStream<Request, DeserializeError, SerializeError>,
            NatsClient<Stream>:
                Client<HttpServiceHandler, Request, DeserializeError, SerializeError>,
        {
            info!(%method, %uri, "Received HTTP request");

            let proxy_request = Request {
                body: if body.is_empty() { None } else { Some(body) },
                headers,
                method,
                path: uri
                    .path_and_query()
                    .map_or_else(|| uri.path().to_string(), |pq| pq.to_string()),
            };

            match client.request(proxy_request).await {
                Ok(response_type) => match response_type {
                    ClientResponseType::Response(payload) => match Response::try_from(payload) {
                        Ok(proxy_response) => proxy_response.into_response(),
                        Err(e) => {
                            error!("Failed to deserialize NATS response: {}", e);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("Failed to process backend response: {}", e),
                            )
                                .into_response()
                        }
                    },
                    ClientResponseType::Stream(_) => {
                        unimplemented!("Stream response type not supported");
                    }
                },
                Err(e) => {
                    error!("Failed to send request via NATS: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to communicate with backend: {}", e),
                    )
                        .into_response()
                }
            }
        }

        let app = Router::new()
            .fallback(handle_request::<S>)
            .with_state(shared_client);

        let addr = SocketAddr::from(([127, 0, 0, 1], self.http_port));
        info!("HTTP proxy listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            error!("Failed to bind HTTP listener: {}", e);
            Error::Io("Failed to bind HTTP listener", e)
        })?;

        axum::serve(listener, app.into_make_service())
            .await
            .map_err(|e| {
                error!("HTTP server error: {}", e);
                Error::Io("HTTP server error", e)
            })?;

        warn!("HTTP server unexpectedly stopped.");
        
        Ok(())
    }

    /// Shutdown the NATS proxy client.
    pub async fn shutdown(&self) -> Result<(), Error> {
        info!("NatsProxyClient shutdown requested.");

        Ok(())
    }
}
