//! HTTP proxy client implementation using engine streams.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{Router, response::IntoResponse};
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};
use proven_bootable::Bootable;
use proven_engine::{Client as EngineClient, Message, StreamConfig};
use proven_storage::LogIndex;
use tokio::sync::{RwLock, mpsc};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{Error, Request, Response};

/// Metadata keys for request/response correlation
const REQUEST_ID_KEY: &str = "request_id";

/// Default timeout for waiting for responses
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Pending request information
struct PendingRequest {
    tx: mpsc::Sender<Response>,
}

/// HTTP proxy client that publishes requests to a stream.
#[derive(Clone)]
pub struct ProxyClient {
    client: Arc<EngineClient>,
    stream_prefix: String,
    http_port: u16,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
    pending_requests: Arc<RwLock<HashMap<Uuid, PendingRequest>>>,
}

impl ProxyClient {
    /// Create a new proxy client.
    #[must_use]
    pub fn new(client: Arc<EngineClient>, stream_prefix: &str, http_port: u16) -> Self {
        Self {
            client,
            stream_prefix: stream_prefix.to_string(),
            http_port,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Handle an incoming HTTP request.
    #[allow(clippy::cognitive_complexity)]
    async fn handle_request(
        client: Arc<EngineClient>,
        stream_prefix: String,
        pending: Arc<RwLock<HashMap<Uuid, PendingRequest>>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        info!(%method, %uri, "Received HTTP request");

        let request_id = Uuid::new_v4();
        let proxy_request = Request {
            id: request_id,
            body: if body.is_empty() { None } else { Some(body) },
            headers,
            method,
            path: uri
                .path_and_query()
                .map_or_else(|| uri.path().to_string(), std::string::ToString::to_string),
        };

        // Create response channel
        let (tx, mut rx) = mpsc::channel(1);

        // Register pending request
        {
            let mut pending_map = pending.write().await;
            pending_map.insert(request_id, PendingRequest { tx });
        }

        // Publish request to stream
        let stream_name = format!("{stream_prefix}.requests");
        let result = publish_request(&client, &stream_name, proxy_request).await;

        if let Err(e) = result {
            // Remove from pending on error
            pending.write().await.remove(&request_id);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to send request: {e}"),
            )
                .into_response();
        }

        // Wait for response with timeout
        match timeout(DEFAULT_RESPONSE_TIMEOUT, rx.recv()).await {
            Ok(Some(response)) => response.into_response(),
            Ok(None) => {
                error!("Response channel closed for request {}", request_id);
                pending.write().await.remove(&request_id);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Response channel closed unexpectedly",
                )
                    .into_response()
            }
            Err(_) => {
                error!("Request {} timed out", request_id);
                pending.write().await.remove(&request_id);
                (
                    StatusCode::GATEWAY_TIMEOUT,
                    "Request timed out waiting for backend response",
                )
                    .into_response()
            }
        }
    }

    /// Start the response streaming task.
    fn start_response_streamer(&self) {
        let client = self.client.clone();
        let stream_name = format!("{}.responses", self.stream_prefix);
        let pending = self.pending_requests.clone();
        let shutdown = self.shutdown_token.clone();

        self.task_tracker
            .spawn(stream_responses(client, stream_name, pending, shutdown));
    }
}

/// Publish a request to the stream.
async fn publish_request(
    client: &EngineClient,
    stream_name: &str,
    request: Request,
) -> Result<LogIndex, Error> {
    // Serialize request
    let mut payload = Vec::new();
    ciborium::ser::into_writer(&request, &mut payload)?;

    // Create message with headers
    let message = Message::new(payload).with_header(REQUEST_ID_KEY, request.id.to_string());

    // Publish to stream
    let last_index = client
        .publish_to_stream(stream_name.to_string(), vec![message])
        .await?;

    Ok(last_index)
}

/// Start streaming responses from the stream.
#[allow(clippy::cognitive_complexity)]
async fn stream_responses(
    client: Arc<EngineClient>,
    stream_name: String,
    pending: Arc<RwLock<HashMap<Uuid, PendingRequest>>>,
    shutdown: CancellationToken,
) {
    use tokio::pin;
    use tokio_stream::StreamExt;

    let start_sequence = LogIndex::new(1).unwrap();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            result = client.stream_messages(stream_name.clone(), start_sequence, None) => {
                match result {
                    Ok(stream) => {
                        pin!(stream);

                        loop {
                            tokio::select! {
                                () = shutdown.cancelled() => return,
                                msg = stream.next() => {
                                    if let Some((message, _timestamp, _sequence)) = msg {
                                        // Extract request ID from headers
                                        let request_id = message
                                            .headers
                                            .iter()
                                            .find(|(k, _)| k == REQUEST_ID_KEY)
                                            .and_then(|(_, v)| Uuid::parse_str(v).ok());

                                        if let Some(id) = request_id {
                                            // Deserialize response
                                            match ciborium::de::from_reader::<Response, _>(message.payload.as_ref()) {
                                                Ok(response) => {
                                                    // Find and notify pending request
                                                    let pending_request = pending.write().await.remove(&id);
                                                    if let Some(pending) = pending_request
                                                        && let Err(e) = pending.tx.send(response).await
                                                    {
                                                        warn!("Failed to send response to waiting request: {}", e);
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to deserialize response: {}", e);
                                                }
                                            }
                                        }
                                    } else {
                                        warn!("Response stream ended, will retry");
                                        break;
                                    }
                                }
                            }
                        }

                        // Small delay before retrying
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Err(e) => {
                        error!("Failed to start streaming responses: {}", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    info!("Response streamer stopped");
}

#[async_trait]
impl Bootable for ProxyClient {
    fn bootable_name(&self) -> &'static str {
        "http-proxy-engine (client)"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.task_tracker.is_closed() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        info!("Starting HTTP proxy client...");

        // Ensure streams exist
        let request_stream = format!("{}.requests", self.stream_prefix);
        let response_stream = format!("{}.responses", self.stream_prefix);

        let stream_config = StreamConfig::default();

        let request_stream_exists = match self.client.get_stream_info(&request_stream).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !request_stream_exists {
            self.client
                .create_stream(request_stream.clone(), stream_config.clone())
                .await?;
        }

        let response_stream_exists = match self.client.get_stream_info(&response_stream).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !response_stream_exists {
            self.client
                .create_stream(response_stream.clone(), stream_config)
                .await?;
        }

        // Start response streamer
        self.start_response_streamer();

        // Build HTTP server
        let client = self.client.clone();
        let stream_prefix = self.stream_prefix.clone();
        let pending = self.pending_requests.clone();

        let app = Router::new().fallback(move |method, uri, headers, body| {
            let client = client.clone();
            let stream_prefix = stream_prefix.clone();
            let pending = pending.clone();
            async move {
                Self::handle_request(client, stream_prefix, pending, method, uri, headers, body)
                    .await
            }
        });

        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, self.http_port));
        info!("HTTP proxy listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;

        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            tokio::select! {
                e = axum::serve(listener, app.into_make_service()).into_future() => {
                    info!("HTTP proxy client exited: {:?}", e);
                }
                () = shutdown_token.cancelled() => {
                    info!("HTTP proxy client shutdown requested");
                }
            };
        });

        self.task_tracker.close();

        info!("HTTP proxy client started successfully");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down HTTP proxy client...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("HTTP proxy client shut down");
        Ok(())
    }

    async fn wait(&self) {
        self.task_tracker.wait().await;
    }
}
