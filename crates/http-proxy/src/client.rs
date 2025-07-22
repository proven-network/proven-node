use crate::error::Error;
use crate::request::Request;
use crate::service_handler::HttpServiceHandler;
use crate::{DeserializeError, SERVICE_NAME, SerializeError};

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};
use proven_bootable::Bootable;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::stream::InitializedStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

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

    /// The shutdown token.
    shutdown_token: CancellationToken,

    /// The stream to route requests to.
    stream: S,

    /// The task tracker.
    task_tracker: TaskTracker,
}

impl<S> HttpProxyClient<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    /// Create a new HTTP proxy client.
    pub fn new(
        HttpProxyClientOptions {
            client_options,
            http_port,
            stream,
        }: HttpProxyClientOptions<S>,
    ) -> Self {
        Self {
            client_options,
            http_port,
            shutdown_token: CancellationToken::new(),
            stream,
            task_tracker: TaskTracker::new(),
        }
    }
}

#[async_trait]
impl<S> Bootable for HttpProxyClient<S>
where
    S: InitializedStream<Request, DeserializeError, SerializeError>,
{
    fn bootable_name(&self) -> &'static str {
        "http-proxy (client)"
    }

    /// Start the HTTP proxy client's listener.
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        info!("Initializing HTTP proxy client...");
        let messaging_client = self
            .stream
            .client::<_, HttpServiceHandler>(SERVICE_NAME.to_string(), self.client_options.clone())
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
                    .map_or_else(|| uri.path().to_string(), std::string::ToString::to_string),
            };

            match client.request(proxy_request).await {
                Ok(response_type) => match response_type {
                    ClientResponseType::Response(payload) => payload.into_response(),
                    ClientResponseType::Stream(_) => {
                        unimplemented!("Stream response type not supported");
                    }
                },
                Err(e) => {
                    error!("Failed to send request: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to communicate with backend: {e}"),
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

        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            tokio::select! {
                e = axum::serve(listener, app.into_make_service()).into_future() => {
                    info!("http proxy client exited {:?}", e);
                }
                () = shutdown_token.cancelled() => {}
            };
        });

        self.task_tracker.close();

        Ok(())
    }

    /// Shutdown the HTTP proxy client.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("http proxy client shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("http proxy client shutdown");

        Ok(())
    }

    /// Wait for the HTTP proxy client to exit.
    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}
