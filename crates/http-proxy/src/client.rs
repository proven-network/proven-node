use crate::error::Error;
use crate::request::Request;
use crate::response::Response;
use crate::service_handler::HttpServiceHandler;
use crate::{DeserializeError, SerializeError};

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};
use proven_messaging::{
    client::{Client, ClientResponseType},
    stream::InitializedStream,
};
use tracing::{error, info, warn};

type MessagingClient<S> =
    <S as InitializedStream<Request, DeserializeError, SerializeError>>::Client<HttpServiceHandler>;

/// Options for the HTTP proxy client.
#[derive(Clone)]
pub struct HttpProxyClientOptions<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// The options for the messaging client.
    pub client_options: <MessagingClient<S> as Client<
        HttpServiceHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// The port to listen on for incoming requests.
    pub http_port: u16,

    /// The name of the service.
    pub service_name: String,

    /// The stream to route requests to.
    pub stream: S,
}

/// The HTTP proxy client.
#[derive(Clone)]
pub struct HttpProxyClient<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// The options for the messaging client.
    client_options: <MessagingClient<S> as Client<
        HttpServiceHandler,
        Request,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// The port to listen on for incoming requests.
    http_port: u16,

    /// The name of the service.
    service_name: String,

    /// The stream to route requests to.
    stream: S,
}

impl<S> HttpProxyClient<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// Create a new HTTP proxy client.
    pub fn new(options: HttpProxyClientOptions<S>) -> Self {
        Self {
            client_options: options.client_options,
            http_port: options.http_port,
            service_name: options.service_name,
            stream: options.stream,
        }
    }

    /// Start the HTTP proxy client's listener.
    pub async fn start(&self) -> Result<(), Error> {
        info!("Initializing HTTP proxy client...");
        let messaging_client = self
            .stream
            .client::<_, HttpServiceHandler>(self.service_name.clone(), self.client_options.clone())
            .await
            .map_err(|e| {
                error!("HTTP proxy client setup failed: {}", e);
                Error::Client(e.to_string())
            })?;

        let shared_client = Arc::new(messaging_client);

        async fn handle_request<S>(
            State(client): State<Arc<MessagingClient<S>>>,
            method: Method,
            uri: Uri,
            headers: HeaderMap,
            body: Bytes,
        ) -> impl IntoResponse
        where
            S: InitializedStream<Request, DeserializeError, SerializeError>,
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

        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, self.http_port));
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

    /// Shutdown the HTTP proxy client.
    pub async fn shutdown(&self) -> Result<(), Error> {
        info!("HttpProxyClient shutdown requested.");

        Ok(())
    }
}
