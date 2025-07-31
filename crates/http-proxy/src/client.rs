//! HTTP proxy client implementation using engine request-reply.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{Router, response::IntoResponse};
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};
use proven_bootable::Bootable;
use proven_engine::{Client as EngineClient, Message};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};
use uuid::Uuid;

use crate::{Error, Request, Response};

/// Default timeout for HTTP proxy requests
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP proxy client that uses request-reply pattern.
#[derive(Clone)]
pub struct ProxyClient {
    client: Arc<EngineClient>,
    service_subject: String,
    http_port: u16,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl ProxyClient {
    /// Create a new proxy client.
    #[must_use]
    pub fn new(client: Arc<EngineClient>, stream_prefix: &str, http_port: u16) -> Self {
        Self {
            client,
            service_subject: format!("{stream_prefix}.service"),
            http_port,
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Handle an incoming HTTP request.
    #[allow(clippy::cognitive_complexity)]
    async fn handle_request(
        client: Arc<EngineClient>,
        service_subject: String,
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

        // Serialize request
        let mut payload = Vec::new();
        if let Err(e) = ciborium::ser::into_writer(&proxy_request, &mut payload) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to serialize request: {e}"),
            )
                .into_response();
        }

        let message = Message::new(payload);

        // Use request-reply pattern
        match client
            .request(&service_subject, message, DEFAULT_REQUEST_TIMEOUT)
            .await
        {
            Ok(response_msg) => {
                // Deserialize response
                match ciborium::de::from_reader::<Response, _>(response_msg.payload.as_ref()) {
                    Ok(response) => response.into_response(),
                    Err(e) => {
                        error!("Failed to deserialize response: {}", e);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Invalid response from backend",
                        )
                            .into_response()
                    }
                }
            }
            Err(e) => {
                let error_string = e.to_string();
                if error_string.contains("No responders") {
                    error!("No proxy service available");
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "No proxy service available",
                    )
                        .into_response()
                } else if error_string.contains("timeout") {
                    error!("Request timed out");
                    (
                        StatusCode::GATEWAY_TIMEOUT,
                        "Request timed out waiting for backend response",
                    )
                        .into_response()
                } else {
                    error!("Request failed: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Proxy error: {e}"),
                    )
                        .into_response()
                }
            }
        }
    }
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

        // Build HTTP server
        let client = self.client.clone();
        let service_subject = self.service_subject.clone();

        let app = Router::new().fallback(move |method, uri, headers, body| {
            let client = client.clone();
            let service_subject = service_subject.clone();
            async move {
                Self::handle_request(client, service_subject, method, uri, headers, body).await
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
